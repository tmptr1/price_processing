from PySide6.QtCore import QThread, Signal
import random
import multiprocessing as mp
import time
import re
import traceback
import datetime
import math
import os
import holidays
import holidays_ru
from sqlalchemy import text, select, delete, insert, update, Sequence, and_, not_, func, distinct, or_, String, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
import numpy as np
import pandas as pd
from python_calamine.pandas import pandas_monkeypatch
pd.set_option('future.no_silent_downcasting', True)
import openpyxl
import xml.etree.ElementTree as ET
import warnings
warnings.filterwarnings('ignore')
# from decimal import Decimal, ROUND_FLOOR
# mp.freeze_support()

import colors
from models import (Base1, Base1_1, PriceReport, Price_1, Price_1_1, SupplierPriceSettings, FileSettings, ColsFix, Brands, SupplierGoodsFix,
                    ExchangeRate, SumTable, SumTable2, TotalPrice_1, AppSettings)
# from Logs import add_log_cf
import setting
engine = setting.get_engine()
# engine.echo = True
session = sessionmaker(engine)
settings_data = setting.get_vars()

CHUNKSIZE = int(settings_data["chunk_size"])
LOG_ID = 1
REPORT_FILE = r"price_report.csv"
TABLES = [Price_1, Price_1_1]
SUM_TABLES = [SumTable, SumTable2]
BASE = [Base1, Base1_1]


class MainWorker(QThread):
    # StartTotalTimeSignal = Signal(bool)
    UpdatePriceStatusTableSignal = Signal(int, str, str, bool)
    ResetPriceStatusTableSignal = Signal(int)
    SetProgressBarValue = Signal(int, int)
    SetTotalTome = Signal(bool)

    SetButtonEnabledSignal = Signal(bool)
    UpdateReportSignal = Signal(bool)
    isPause = None
    total_file_count = 0
    cur_file_count = 0

    def __init__(self, file_size_limit, log=None, parent=None): # threads_count=None, sender=None
        self.log = log
        # self.sender = sender
        self.file_size_limit = file_size_limit
        if file_size_limit[0] == '>':
            self.file_size_type = 1  # 0 - light, 1 - heavy
        else:
            self.file_size_type = 0
        self.TmpPrice_1 = TABLES[self.file_size_type]
        self.TmpSum = SUM_TABLES[self.file_size_type]
        QThread.__init__(self, parent)


    def run(self):
        global session, engine
        wait_sec = 10
        self.SetButtonEnabledSignal.emit(False)
        while not self.isPause:
            start_cycle_time = datetime.datetime.now()

            # self.threads_count = 1

            # time.sleep(1)
            # print('C')
            try:
                # Base.metadata.drop_all(engine)
                # Base.metadata.create_all(engine)
                # print('sql ok')
                # return


                # files = ['1FAL', '0MI1', '1IMP', '1PRD', '1VAL', '1АТХ', '1ГУД', '2AVX', '4MI0']


                files = os.listdir(settings_data["mail_files_dir"])
                new_files = []
                # engine.echo=True
                with session() as sess:
                    self.mb_limit = int(sess.execute(select(AppSettings.var).where(AppSettings.param == 'mb_limit_1')).scalar())
                    # print(f"{self.mb_limit}")
                    for file in files:
                        if file.startswith('~$'):
                            continue
                        file_name = '.'.join(file.split('.')[:-1])
                        if len(file_name) < 4:
                            continue
                        price_code = file_name[:4]
                        new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(fr"{settings_data['mail_files_dir']}/{file}")
                                                                          ).strftime("%Y-%m-%d %H:%M:%S")

                        req = select(PriceReport.updated_at).where(PriceReport.price_code == price_code)
                        last_update_tile = sess.execute(req).scalar()
                        if last_update_tile and str(last_update_tile) == new_update_time:
                            continue

                        req = select(SupplierPriceSettings.standard).where(SupplierPriceSettings.price_code == price_code)
                        standard = sess.execute(req).scalar()
                        if not standard:
                            sess.query(PriceReport).where(PriceReport.price_code == price_code).delete()
                            sess.add(PriceReport(file_name=file, price_code=price_code, info_message="Нет в условиях",
                                                 updated_at=new_update_time))
                            self.log.add(LOG_ID, f"{price_code} Нет в условиях", f"<span style='color:{colors.orange_log_color};"
                                                                                 f"font-weight:bold;'>{price_code}</span> Нет в условиях")
                            continue
                        elif str(standard).upper() != 'ДА':
                            # req = update(PriceReport).where(PriceReport.price_code == price_code).values(info_message="Не указана стандартизация",
                            #                                                                       updated_at=new_update_time)
                            sess.query(PriceReport).where(PriceReport.price_code == price_code).delete()
                            sess.add(PriceReport(file_name=file, price_code=price_code, info_message="Не указана стандартизация",
                                                 updated_at=new_update_time))
                            sess.execute(req)
                            self.log.add(LOG_ID, f"{price_code} Не указана стандартизация",
                                         f"<span style='color:{colors.orange_log_color};"
                                         f"font-weight:bold;'>{price_code}</span> Не указана стандартизация")
                            continue

                        save = sess.execute(select(FileSettings.save).where(FileSettings.price_code == price_code)).scalar()
                        if not save or str(save).upper() != 'ДА':
                            sess.query(PriceReport).where(PriceReport.price_code == price_code).delete()
                            sess.add(PriceReport(file_name=file, price_code=price_code,
                                                 info_message="Не указано сохранение",
                                                 updated_at=new_update_time))
                            sess.execute(req)
                            self.log.add(LOG_ID, f"{price_code} Не указано сохранение",
                                         f"<span style='color:{colors.orange_log_color};"
                                         f"font-weight:bold;'>{price_code}</span> Не указано сохранение")
                            continue

                        if not last_update_tile:
                            new_files.append(file)
                            continue
                        if str(last_update_tile) < new_update_time:
                            new_files.append(file)

                    # Удаление неактуальных прайсов (сохранение != ДА)
                    loaded_prices = set(sess.execute(select(distinct(TotalPrice_1._07supplier_code))).scalars().all())
                    actual_prices = set(sess.execute(select(FileSettings.price_code).where(func.upper(FileSettings.save) == 'ДА')).scalars().all())
                    useless_prices = (loaded_prices-actual_prices)
                    if useless_prices:
                        self.log.add(LOG_ID, f"Удаление неактуальных прайсов")
                        cur_time = datetime.datetime.now()
                        sess.query(TotalPrice_1).where(TotalPrice_1._07supplier_code.in_(useless_prices)).delete()
                        self.log.add(LOG_ID, f"Удаление неактуальных прайсов завершено [{str(datetime.datetime.now() - cur_time)[:7]}]")
                    sess.commit()

                # print(f"{new_files=}")
                # return

                # new_files = ['1FRA Прайс ФорвардАвто Краснодар.xlsx']#'1MTK Остатки оригинал Bobcat Doosan.xlsx'] #'1AVX AVEX.xlsx']
                # new_files = ['1MTK Остатки оригинал Bobcat Doosan.xlsx']
                # new_files = ['4FAL FORUM_AUTO_PRICE_CENTER.xlsx']
                # new_files = ["1VAL Шевелько 'Аккумуляторы' (XLSX).xlsx"]
                # new_files = ['1ROS 155889.xlsx', '1FAV FAVORIT.xlsx']
                # new_files = ["1ROS 155889.xlsx"]
                # new_files = ["1APR Прайс Армтек Краснодар.xlsx"]
                # new_files = ["1VAL Шевелько 'Аккумуляторы' (XLSX).xlsx"]
                # new_files = ['MI21 mikado_price_vlgd.csv', "1VAL Шевелько 'Аккумуляторы' (XLSX).xlsx", "VTTE электроинструмент.xlsx",
                #              "1IMP IMPEKS_KRD.xlsx"]
                # new_files = ["АСИ1 price.xls"]
                # new_files = ["MKOZ остатки_ИМ.xls"]
                # new_files = ["1ГУД Прайс ШСН.xlsx"]
                # new_files = ["TKTZ Печать.xls"]
                # new_files = ["8ГУД дочерний парйс 1ГУД.csv", "9ГУД дочерний парйс 1ГУД.csv"]
                # new_files = ["2ССС web-parts SIMF.xlsx"]
                # new_files = ["1АСК Прайс.xlsx"]
                # new_files = ["1РЕФ Прайс.xlsx", "1VAL Шевелько 'Аккумуляторы' (XLSX).xlsx"]
                # new_files = ["3МСК rostov_.xlsx"]
                # new_files = ["1IMP IMPEKS_KRD.xlsx"]
                # new_files = ["1MKO остатки_ОС.xls", "1ROS 155889.xlsx", "MI09 mikado_price_vdkz.csv"]
                # new_files = ['1SHI Прайс_Эникс.xlsx', '1TEM prais LUBRIMEX Krasnodar.xls', '1IMP IMPEKS_KRD.xlsx',
                #              '1AVX AVEX.xlsx', '1MTK Остатки оригинал Bobcat Doosan.xlsx']
                # new_files = ['1EPR Европарт.xlsx', '1MKO остатки_ОС.xls']
                # new_files = ['1LAM Прайс-лист.xls', '1IMP IMPEKS_KRD.xlsx']
                # new_files = ['1KPG Прайс-лист.xlsx']
                files = []
                for f in new_files:
                    if self.check_file_condition(f):
                        files.append(f)

                if files:
                    self.log.add(LOG_ID, f"Начало обработки [{self.file_size_type+1}]") # (потоков: {self.threads_count})
                    cur_time = datetime.datetime.now()
                    # with session() as sess:
                    #     sess.execute(text(f"ALTER SEQUENCE sum_table_id_seq restart 1"))
                    #     sess.execute(text(f"ALTER SEQUENCE price_1_id_seq restart 1"))
                    #     sess.commit()

                    # if len(new_files) < self.threads_count:
                    #     self.threads_count = len(new_files)


                    self.SetTotalTome.emit(True)
                    # self.StartTotalTimeSignal.emit(True)
                    # self.sender.send(["new", len(files)])

                    # with mp.Pool(processes=self.threads_count) as pool:
                    #     args = [[file, self.sender, False] for file in new_files]
                    #     pool.map(multi_calculate, args)
                    self.total_file_count = len(files)
                    self.cur_file_count = 0
                    for file in files:
                        self.multi_calculate([file, False])
                        # print(self.file_size_type, ']', file)
                    # multi_calculate(args=['1ROS 155889.xlsx', self.sender])

                    self.log.add(LOG_ID, f"Обработка закончена [{str(datetime.datetime.now() - cur_time)[:7]}] [{self.file_size_type+1}]")

                    self.SetTotalTome.emit(False)
                    # self.StartTotalTimeSignal.emit(False)
                    self.UpdateReportSignal.emit(1)
                # self.log.add(1, "Начало обработки calc")
            except (OperationalError, UnboundExecutionError) as db_ex:
                self.log.add(LOG_ID, f"Повторное подключение к БД ...", f"<span style='color:{colors.orange_log_color};"
                                                                         f"font-weight:bold;'>Повторное подключение к БД ...</span>  ")
                try:
                    engine = setting.get_engine()
                    session = sessionmaker(engine)
                except:
                    pass
            except Exception as ex:
                ex_text = traceback.format_exc()
                self.log.error(LOG_ID, "ERROR", ex_text)
                # self.sender.send(["error", LOG_ID, ex, "ERROR", ex_text]) # только для mp
            finally:
                self.ResetPriceStatusTableSignal.emit(self.file_size_type)
                self.SetTotalTome.emit(False)

            # проверка на паузу
            finish_cycle_time = datetime.datetime.now()
            if wait_sec > (finish_cycle_time - start_cycle_time).seconds:
                for _ in range(wait_sec - (finish_cycle_time - start_cycle_time).seconds):
                    if self.isPause:
                        break
                    time.sleep(1)
        else:
            self.log.add(LOG_ID, f"Пауза [{self.file_size_type+1}]", f"<span style='color:{colors.orange_log_color};'>Пауза [{self.file_size_type+1}]</span>  ")
            self.SetButtonEnabledSignal.emit(True)

    def check_file_condition(self, file_name):
        MB = os.path.getsize(f"{settings_data['mail_files_dir']}/{file_name}") / 1024 / 1024
        if self.file_size_type == 1:
            if MB > self.mb_limit:
                return True
            return False
        # type = 0
        if MB <= self.mb_limit:
            return True
        return False

    def multi_calculate(self, args):
        file_name, custom_price_code = args
        self.color = [random.randrange(0, 360), random.randrange(55, 100), 90]
        children_prices = None
        try:
            if not custom_price_code:
                price_code = file_name[:4]
            else:
                price_code = custom_price_code
            # sender.send(["add", mp.current_process().name, price_code, 1, f"Загрузка сырых дынных...", True])
            self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, "Загрузка сырых дынных...", True)

            path_to_price = fr"{settings_data['mail_files_dir']}/{file_name}"
            frmt = file_name.split('.')[-1]
            new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_price)).strftime("%Y-%m-%d %H:%M:%S")

            start_calc_price_time = datetime.datetime.now()
            cur_time = datetime.datetime.now()

            # inspct = inspect(engine)
            # if inspct.has_table(self.TmpPrice_1.__tablename__):
            #     self.TmpPrice_1.__table__.drop(engine)
            # if inspct.has_table(self.TmpSum.__tablename__):
            #     self.TmpSum.__table__.drop(engine)

            # try:
            #     Base.metadata.create_all(engine)
            # except:
            #     pass

            # print(f"[{self.file_size_type}] {}")

            inspct = inspect(engine)
            if inspct.has_table(self.TmpPrice_1.__tablename__):
                self.TmpPrice_1.__table__.drop(engine)
            if inspct.has_table(self.TmpSum.__tablename__):
                self.TmpSum.__table__.drop(engine)

            BASE[self.file_size_type].metadata.create_all(engine)

            with session() as sess:
                # sess.query(Price_1).where(Price_1._07supplier_code == price_code).delete()
                # sess.execute(text(f"REINDEX TABLE {Price_1.__tablename__}"))
                # sess.execute(text(f"REINDEX TABLE {SumTable.__tablename__}"))
                # sess.execute(text(f"ALTER SEQUENCE {Price_1.__tablename__}_id_seq restart 1"))
                # sess.execute(text(f"ALTER SEQUENCE {SumTable.__tablename__}_id_seq restart 1"))
                # sess.query(Price_1).delete() # FOR 1 THREAD
                # sess.query(SumTable).delete() # FOR 1 THREAD
                sess.execute(text(f"ALTER TABLE {self.TmpPrice_1.__tablename__} SET (autovacuum_enabled = false);"))
                sess.execute(text(f"ALTER TABLE {self.TmpSum.__tablename__} SET (autovacuum_enabled = false);"))

                sess.commit()

                req = select(PriceReport).where(PriceReport.price_code == price_code)
                is_report_exists = sess.execute(req).scalar()
                if not is_report_exists:
                    sess.add(PriceReport(file_name=file_name, price_code=price_code))

                if not self.check_price_time(price_code, file_name, sess):
                    self.cur_file_count += 1
                    sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).values(
                        info_message="Не подходит по сроку обновления", updated_at=new_update_time)) # sess.execute(req)
                    sess.commit()
                    # add_log_cf(LOG_ID, f"Не подходит по сроку обновления ({self.cur_file_count}/{self.total_file_count})", sender, price_code, color)
                    self.add_log(self.file_size_type, price_code,f"Не подходит по сроку обновления ({self.cur_file_count}/{self.total_file_count})", cur_time)
                    return

                req = select(FileSettings).where(FileSettings.price_code == price_code)
                price_table_settings = sess.execute(req).scalars().all()
                if not price_table_settings:
                    self.cur_file_count += 1
                    sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).values(
                        info_message=f"Нет настроек", updated_at=new_update_time))
                    sess.commit()
                    # add_log_cf(LOG_ID, f"Нет настроек ({self.cur_file_count}/{self.total_file_count})", sender, price_code, color)
                    self.add_log(self.file_size_type, price_code, f"Нет настроек ({self.cur_file_count}/{self.total_file_count})", cur_time)
                    return

                # сопоставление столлцов
                try:
                    id_settig, rc_dict, new_frmt, reason = self.get_setting_id(price_table_settings, frmt, path_to_price)
                    if not id_settig:
                        self.cur_file_count += 1
                        if reason:
                            reason = f"({reason[:200]})"
                        sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).values(
                            info_message=f"Нет подходящих настроек столбцов {reason}", updated_at=new_update_time))
                        sess.commit()
                        # add_log_cf(LOG_ID, f"Нет подходящих настроек столбцов {reason} ({self.cur_file_count}/{self.total_file_count})", sender, price_code, color)
                        self.add_log(self.file_size_type, price_code,
                                     f"Нет подходящих настроек столбцов {reason} ({self.cur_file_count}/{self.total_file_count})", cur_time)
                        return
                except ValueError as val_ex:
                    self.cur_file_count += 1
                    sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).values(
                        info_message="Ошибка формата", updated_at=new_update_time))
                    sess.commit()
                    # add_log_cf(LOG_ID, "Ошибка формата", sender, price_code, color)
                    # sender.send(["log", LOG_ID, f"{price_code} Ошибка формата ({self.cur_file_count}/{self.total_file_count})",
                    #              f"<span style='background-color:hsl({color[0]}, {color[1]}%, {color[2]}%);'>"
                    #              f"{price_code}</span> Ошибка формата  "])
                    self.add_log(self.file_size_type, price_code, f"Ошибка формата", cur_time)
                    raise val_ex
                    # return

                # Удаление старой версии
                # sess.query(Price_1).where(Price_1._07supplier_code == price_code).delete()

                # загрузка сырых данных
                sett = sess.get(FileSettings, {'id': id_settig})

                if new_frmt == 'xml':
                    loaded = self.load_data_from_xml_to_db(sett, rc_dict, path_to_price, price_code, sess)
                else:
                    loaded = self.load_data_to_db(sett, rc_dict, frmt, path_to_price, price_code, sess)
                if not loaded:
                    self.cur_file_count += 1
                    sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).values(
                        info_message="Настройки столбцов не подошли", updated_at=new_update_time))
                    sess.commit()
                    # add_log_cf(LOG_ID, f"Настройки столбцов не подошли ({self.cur_file_count}/{self.total_file_count})", sender, price_code, color)
                    self.add_log(self.file_size_type, price_code,
                                 f"Настройки столбцов не подошли ({self.cur_file_count}/{self.total_file_count})", cur_time)
                    return

                sess.commit() #sess.flush()
                # add_log_cf(LOG_ID, "Загрузка сырых дынных завершена", sender, price_code, color, cur_time)
                self.add_log(self.file_size_type, price_code,"Загрузка сырых дынных завершена", cur_time)

                cur_time = datetime.datetime.now()
                # sender.send(["add", mp.current_process().name, price_code, 1, f"Обработка 1, 2, 3, 4, 14 ..."])
                self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, "Обработка 1, 2, 3, 4, 14 ...", False)

                # sess.execute(update(Price_1).where(func.nullif(Price_1.article_s, '')).values(article_s=None))
                # sess.execute(update(Price_1).where(Price_1.brand_s.like('')).values(brand_s=None))
                # sess.execute(update(Price_1).where(func.length(Price_1.brand_s) == 0).values(brand_s=None))
                # sess.execute(update(Price_1).where(Price_1.brand_s == ' ').values(brand_s=None))
                # sess.execute(update(Price_1).where(func.trim(Price_1.brand_s) == '').values(brand_s=None))
                # x_null = sess.query(Price_1).filter(Price_1.name_s == 'Отвертка 3-х лучевая, 6мм.').all()
                # x_null = sess.query(Price_1).where(func.length(Price_1.brand_s) == 1).all()
                # print(len(x_null))
                # print(f"|{x_null[0].brand_s}|")
                # regexp_replace(';', ',', 'g')
                # sess.execute(text(f"update {Price_1.__tablename__} set brand_s = NULL where brand_s = ''"))
                # sess.execute(text(f"update {Price_1.__tablename__} set brand_s = NULL where brand_s = ''"))
                # sess.execute(text("update price_1 set brand_s = NULL where brand_s like ''"))
                # sess.execute(update(Price_1).where(Price_1.brand_s.is_('')).values(brand_s=None))

                # n_dt = datetime.datetime.now()
                # замена пустых бренд п на значение по умолчанию
                sess.execute(update(self.TmpPrice_1).where(func.trim(self.TmpPrice_1.brand_s) == '').values(brand_s=None))
                if sett.replace_brand_s:
                    sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1.brand_s==None).values(_02brand=sett.replace_brand_s))
                # print(f"замена пустых бренд {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # исправление товаров поставщиков (01Артикул, 02Производитель, 03Наименование, 04Количество, 05Цена, 06Кратность)
                self.suppliers_goods_compare(price_code, sett, sess)
                # print(f"исправление товаров поставщиков {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # замена ; на ,
                text_cols = [self.TmpPrice_1.key1_s, self.TmpPrice_1.article_s, self.TmpPrice_1.brand_s, self.TmpPrice_1.name_s,
                             self.TmpPrice_1.currency_s, self.TmpPrice_1.notice_s]
                for c in text_cols:
                    sess.execute(update(self.TmpPrice_1).where(c != None).values({c.__dict__['name']: c.regexp_replace(';', ',', 'g')}))
                # print(f"замена ; на , {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # Исправление Номенклатуры
                self.cols_fix(price_code, sess)
                # print(f"Исправление Номенклатуры {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 01Артикул
                sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1._01article == None).values(_01article=func.upper(self.TmpPrice_1.article_s)))
                sess.execute(update(self.TmpPrice_1).values(_01article=func.upper(self.TmpPrice_1._01article.regexp_replace(' +', ' ', 'g')
                                                     .regexp_replace('^ | $', '', 'g'))))
                sess.execute(update(self.TmpPrice_1).values(_01article_comp=func.upper(self.TmpPrice_1._01article.regexp_replace(r'\W', '', 'g'))))
                # print(f"01Артикул {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 02Производитель
                sess.execute(update(self.TmpPrice_1).values(brand_s_low=func.lower(self.TmpPrice_1.brand_s.regexp_replace(r'\W', '', 'g'))))
                sess.execute(update(self.TmpPrice_1).where(and_(self.TmpPrice_1.brand_s_low == Brands.brand_low,
                                                        self.TmpPrice_1._02brand == None)).values(_02brand=Brands.correct_brand))
                # print(f"02Производитель {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 14Производитель заполнен
                sess.execute(update(self.TmpPrice_1).values(_14brand_filled_in=func.upper(func.coalesce(self.TmpPrice_1._02brand, self.TmpPrice_1.brand_s))))
                # print(f"14Производитель заполнен {datetime.datetime.now() - n_dt}")
                # sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1._02brand != None))
                #              .values(_14brand_filled_in=Price_1._02brand))

                # Изменение цены по условиям (Цена поставщика)
                # apply_discount(sess, price_code, 'Цена поставщика')

                # n_dt = datetime.datetime.now()
                # 03Наименование
                sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1._03name == None).values(_03name=self.TmpPrice_1.name_s))
                sess.execute(update(self.TmpPrice_1).values(_03name=self.TmpPrice_1._03name.regexp_replace(' +', ' ', 'g')
                                     .regexp_replace('^ | $', '', 'g')))
                # print(f"03Наименование {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 04Количество
                sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1._04count != None).values(_04count=self.TmpPrice_1.count_s-self.TmpPrice_1._04count))
                sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1._04count == None).values(_04count=self.TmpPrice_1.count_s))
                # print(f"04Количество {datetime.datetime.now() - n_dt}")

                sess.commit()#sess.flush()
                # add_log_cf(LOG_ID, "Обработка 1, 2, 3, 4, 14 завершена", sender, price_code, color, cur_time)
                self.add_log(self.file_size_type, price_code, "Обработка 1, 2, 3, 4, 14 завершена", cur_time)

                cur_time = datetime.datetime.now()
                # sender.send(["add", mp.current_process().name, price_code, 1, f"Обработка 5, 6, 12, 15, 17, 18, 20 ..."])
                self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, "Обработка 5, 6, 12, 15, 17, 18, 20 ...", False)

                # n_dt = datetime.datetime.now()
                # 05Цена
                sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1._05price == None).values(_05price=self.TmpPrice_1.price_s))
                # print(f"05Цена {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # для валют
                sess.execute(update(self.TmpPrice_1).where(
                    and_(self.TmpPrice_1.currency_s != None, ExchangeRate.code == func.upper(self.TmpPrice_1.currency_s)))
                             .values(_05price=self.TmpPrice_1._05price * ExchangeRate.rate))
                # print(f"для валют {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 12Сумма
                numeric_max = 9999999999 # numeric 12,2
                sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1.count_s * self.TmpPrice_1.price_s < numeric_max).
                             values(_12sum=self.TmpPrice_1.count_s * self.TmpPrice_1.price_s))
                sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1.count_s * self.TmpPrice_1.price_s >= numeric_max).values(_12sum=numeric_max))
                # print(f"12Сумма {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # Настройка строк: Вариант изменения цены
                if sett.change_price_type in ("- X %", "+ X %"):
                    percent = None
                    try:
                        percent = float(f"{sett.change_price_type[0]}{sett.change_price_val}")
                    except:
                        pass
                    if percent:
                        sess.execute(update(self.TmpPrice_1).values(_05price=self.TmpPrice_1._05price * (1+percent)))
                # print(f"Настройка строк {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # Изменение цены по условиям (05Цена)
                self.apply_discount(sess, price_code, '05Цена')
                # print(f"Изменение цены по условиям {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 06Кратность
                sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1._06mult == None).values(_06mult=self.TmpPrice_1.mult_s))
                sess.execute(update(self.TmpPrice_1).where(or_(self.TmpPrice_1._06mult == None, self.TmpPrice_1._06mult < 1)).values(_06mult=1))
                # print(f"06Кратность {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 15КодТутОптТорг
                sess.execute(update(self.TmpPrice_1).values(_15code_optt=(self.TmpPrice_1._01article+self.TmpPrice_1._14brand_filled_in).
                                                            regexp_replace(r"\W|_", "", 'g'))) # func.upper
                # print(f"15КодТутОптТорг {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 20ИсключитьИзПрайса
                self.words_except(sess, price_code)
                # print(f"20ИсключитьИзПрайса {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 17КодУникальности
                sess.execute(update(self.TmpPrice_1).values(_17code_unique=func.upper(self.TmpPrice_1._07supplier_code + self.TmpPrice_1._15code_optt + "ДАSS")))
                # print(f"17КодУникальности {datetime.datetime.now() - n_dt}")

                # n_dt = datetime.datetime.now()
                # 18КороткоеНаименование
                sess.execute(update(self.TmpPrice_1).values(_18short_name=func.regexp_substr(self.TmpPrice_1._03name, r'(\S+.){1,2}(\S+){0,1}')))
                # print(f"18КороткоеНаименование {datetime.datetime.now() - n_dt}")

                sess.commit()#sess.flush()
                # add_log_cf(LOG_ID, "Обработка 5, 6, 12, 15, 17, 18, 20 завершена", sender, price_code, color, cur_time)
                self.add_log(self.file_size_type, price_code, "Обработка 5, 6, 12, 15, 17, 18, 20 завершена", cur_time)

                cur_time = datetime.datetime.now()
                # sender.send(["add", mp.current_process().name, price_code, 1, f"Обработка 13 ..."])
                self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, "Обработка 13 ...", False)

                # 13Градация
                total_sum = sess.execute(
                    select(func.sum(self.TmpPrice_1._05price))).scalar()

                subq = select(self.TmpPrice_1.id, self.TmpPrice_1._07supplier_code,
                              func.floor((total_sum - func.sum(self.TmpPrice_1._05price).over(order_by=(self.TmpPrice_1._05price,
                                                                                                        self.TmpPrice_1.id))) / (total_sum / 100))).where(
                    and_(self.TmpPrice_1._20exclude == None, self.TmpPrice_1._05price > 0))

                sess.execute(insert(self.TmpSum).from_select(['id', 'price_code', 'prev_sum'], subq))
                sess.commit()#sess.flush()

                sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1.id == self.TmpSum.id).values(_13grad=self.TmpSum.prev_sum))
                sess.query(self.TmpSum).where(self.TmpSum.price_code == price_code).delete()

                # add_log_cf(LOG_ID, "Обработка 13 завершена", sender, price_code, color, cur_time)
                self.add_log(self.file_size_type, price_code, "Обработка 13 завершена", cur_time)

                csv_cols_dict = {"Ключ1 поставщика": self.TmpPrice_1.key1_s, "Артикул поставщика": self.TmpPrice_1.article_s,
                                 "Производитель поставщика": self.TmpPrice_1.brand_s, "Наименование поставщика": self.TmpPrice_1.name_s,
                                 "Количество поставщика": self.TmpPrice_1.count_s, "Цена поставщика": self.TmpPrice_1.price_s,
                                 "ВалютаП": self.TmpPrice_1.currency_s, "Кратность поставщика": self.TmpPrice_1.mult_s,
                                 "Примечание поставщика": self.TmpPrice_1.notice_s, "01Артикул": self.TmpPrice_1._01article,
                                 "02Производитель": self.TmpPrice_1._02brand,
                                 "14Производитель заполнен": self.TmpPrice_1._14brand_filled_in,
                                 "03Наименование": self.TmpPrice_1._03name, "04Количество": self.TmpPrice_1._04count,
                                 "05Цена": self.TmpPrice_1._05price, "12Сумма": self.TmpPrice_1._12sum, "06Кратность": self.TmpPrice_1._06mult,
                                 "15КодТутОптТорг": self.TmpPrice_1._15code_optt, "07Код поставщика": self.TmpPrice_1._07supplier_code,
                                 "20ИсключитьИзПрайса": self.TmpPrice_1._20exclude, "13Градация": self.TmpPrice_1._13grad,
                                 "17КодУникальности": self.TmpPrice_1._17code_unique,
                                 "18КороткоеНаименование": self.TmpPrice_1._18short_name,
                                 }

                self.cur_file_count += 1
                # to csv
                if not self.create_csv(sess, price_code, csv_cols_dict, start_calc_price_time,new_update_time):
                    return

                # cur_time = datetime.datetime.now()
                # sender.send(["add", mp.current_process().name, price_code, 1, f"Запись обработанных данных в БД ..."])
                # перенос данных в total
                sess.query(TotalPrice_1).where(TotalPrice_1._07supplier_code == price_code).delete()

                cols_for_total = [self.TmpPrice_1.key1_s, self.TmpPrice_1.article_s, self.TmpPrice_1.brand_s, self.TmpPrice_1.name_s,
                                  self.TmpPrice_1.count_s, self.TmpPrice_1.price_s, self.TmpPrice_1.currency_s, self.TmpPrice_1.mult_s, self.TmpPrice_1.notice_s,
                                  self.TmpPrice_1._01article, self.TmpPrice_1._01article_comp, self.TmpPrice_1._02brand, self.TmpPrice_1._14brand_filled_in, self.TmpPrice_1._03name,
                                  self.TmpPrice_1._04count, self.TmpPrice_1._05price, self.TmpPrice_1._12sum, self.TmpPrice_1._06mult,
                                  self.TmpPrice_1._15code_optt, self.TmpPrice_1._07supplier_code, self.TmpPrice_1._20exclude, self.TmpPrice_1._13grad,
                                  self.TmpPrice_1._17code_unique, self.TmpPrice_1._18short_name]
                cols_for_total = {i: i.__dict__['name'] for i in cols_for_total}
                total = select(*cols_for_total.keys())
                sess.execute(insert(TotalPrice_1).from_select(cols_for_total.values(), total))

                # add_log_cf(LOG_ID, "Запись обработанных данных в БД завершена", sender, price_code, color, cur_time)

                # дочерние прайсы
                children_prices = sess.execute(select(distinct(FileSettings.price_code))
                                               .where(and_(FileSettings.parent_code == price_code, FileSettings.parent_code != FileSettings.price_code,
                                                           func.upper(FileSettings.save) == 'ДА'))).scalars().all()
                # УДАЛИТЬ ИЗ БД
                # sess.query(Price_1).delete()

                sess.commit()

            self.TmpPrice_1.__table__.drop(engine)
            self.TmpSum.__table__.drop(engine)
                # sess.commit()
            # print(f"{children_prices=}")


                    # csv_cols_dict = {"Ключ1 поставщика": Price_1.key1_s, "Артикул поставщика": Price_1.article_s,
                #                      "Производитель поставщика": Price_1.brand_s, "Наименование поставщика": Price_1.name_s,
                #                      "Количество поставщика": Price_1.count_s, "Цена поставщика": Price_1.price_s,
                #                      "ВалютаП": Price_1.currency_s, "Кратность поставщика": Price_1.mult_s,
                #                      "Примечание поставщика": Price_1.notice_s,
                #                      }
                #     cols_for_total.pop(Price_1.id)
                #     cols_for_total.pop(Price_1._07supplier_code)
                #     cols_for_total.pop(Price_1._20exclude)
                #
                    # for children_price in children_prices:
                    #     # print(children_price)
                #         dupl = select(func.concat(children_price).label('_07supplier_code'), *cols_for_total.keys()).where(Price_1._07supplier_code == price_code)
                #         sess.execute(insert(Price_1).from_select(['_07supplier_code', *cols_for_total.values()], dupl))
                #         words_except(sess, Price_1, children_price)
                #
                #         sess.query(Price_1).where(
                #             and_(Price_1._07supplier_code == children_price, Price_1._20exclude != None)).delete()
                #
                #         create_children_csv(sess, sender, children_price, csv_cols_dict, color, price_code)
                #
                #         sess.query(Price_1).where(Price_1._07supplier_code == children_price).delete()

                        # start_calc_price_time = datetime.datetime.now()
                        # req = select(PriceReport).where(PriceReport.price_code == children_price)
                        # is_report_exists = sess.execute(req).scalar()
                        # if not is_report_exists:
                        #     sess.add(PriceReport(file_name=file_name, price_code=children_price))
                        #
                        # # if not check_price_time(price_code, file_name, sender, sess):
                        # #     sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).values(
                        # #         info_message="Не подходит по сроку обновления", updated_at=new_update_time)) # sess.execute(req)
                        # #     sess.commit()
                        # #     add_log_cf(LOG_ID, "Не подходит по сроку обновления", sender, children_price, color)
                        # #     continue
                        #
                        # sess.query(TotalPrice_1).where(TotalPrice_1._07supplier_code == children_price).delete()
                        # dupl = select(func.concat(children_price).label('_07supplier_code'), *cols_for_total.keys()).where(Price_1._07supplier_code == price_code)
                        # sess.execute(insert(TotalPrice_1).from_select(['_07supplier_code', *cols_for_total.values()], dupl))
                        # words_except(sess, TotalPrice_1, children_price)
                        # sess.query(TotalPrice_1).where(and_(TotalPrice_1._07supplier_code == children_price, TotalPrice_1._20exclude != None)).delete()
                        #
                        #
                        #
                        # create_csv(sess, sender, children_price, csv_cols_dict, color, start_calc_price_time,
                        #            datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"), add_text=f"(родитель: {price_code})")

                        # print(cols_for_total)
                        # break

                # sess.rollback()
                # cur_time = datetime.datetime.now()
                # sender.send(["add", mp.current_process().name, price_code, 1, f"Удаление временных таблиц ..."])
                # self.TmpPrice_1.__table__.drop(engine)
                # self.TmpSum.__table__.drop(engine)
                # add_log_cf(LOG_ID, "Временные таблицы удалены", sender, price_code, color, cur_time)

                # total_price_calc_time = str(datetime.datetime.now() - start_calc_price_time)[:7]
                # sender.send(
                #     ["log", LOG_ID, f"+ {price_code} готов! ({self.cur_file_count}/{self.total_file_count}) [{total_price_calc_time}]",
                #      f"<span style='color:{colors.green_log_color};font-weight:bold;'>✔</span> "
                #      f"<span style='background-color:hsl({color[0]}, {color[1]}%, {color[2]}%);'>"
                #      f"{price_code}</span> готов! ({self.cur_file_count}/{self.total_file_count}) [{total_price_calc_time}]"])
        except Exception as ex:
            ex_text = traceback.format_exc()
            # sender.send(["error", LOG_ID, f"ERROR ({file_name})", ex_text])
            self.log.error(LOG_ID, "ERROR", ex_text)
        finally:
            # sender.send(["end", mp.current_process().name, True if custom_price_code else False])
            self.SetProgressBarValue.emit(self.cur_file_count, self.total_file_count)  # +1

        if children_prices:
            for children_price in children_prices:
                # print(children_price)
                self.total_file_count += 1
                self.multi_calculate([file_name, children_price])

    def check_price_time(self, price_code, file_name, sess):
        '''Расчёт рабочих дней с последнего изменения прайса'''


        d1 = datetime.datetime.fromtimestamp(os.path.getmtime(fr"{settings_data['mail_files_dir']}/{file_name}"))
        d2 = datetime.datetime.today()
        days = (d2 - d1)
        days = days.days + days.seconds / 86400

        if days > 50:  # если прошло больше 50 дней, кол-во рабочих дней считается 'грубо'
            days -= (((d2 - d1) // 7) * 2).days
        else:
            tmp_d = d1
            for i in range(int(days)):  # расчёт рабочих дней
                tmp_d += datetime.timedelta(days=1)
                if tmp_d.weekday() in (5, 6):
                    days -= 1

            holidays_list = holidays.RU(years=d2.year).items()
            date_list = set()
            for i in holidays_list:  # выборка праздников, которые НЕ выпадают на субботу/воскресенье
                if i[0].weekday() not in (5, 6):
                    date_list.add(i[0])

            now = datetime.date.today()

            tmp_d = d1.date()
            for i in date_list:  # отнять праздники
                if tmp_d <= i and now >= i:
                    days -= 1

        # cur.execute(f"select Срок_обновление_не_более from Настройки_прайса_поставщика where Код_прайса = '{file_name_}'")
        req = select(SupplierPriceSettings.update_time).where(SupplierPriceSettings.price_code == price_code)
        days2 = sess.execute(req).scalar()
        if not days2:  # нет срока обновления
            # logger.info(f"Для прайса {file_name_} нет срока обновления")
            return 0
        if days > days2:
            # logger.info(f"{file_name_} не подходит по сроку обновления")
            return 0
        return 1

    def get_setting_id(self, price_table_settings, frmt, path_to_price):
        id_settig = None
        rc_dict = dict()
        new_frmt = None
        reason = ''
        for sett in price_table_settings:
            # print(sett.id)
            rc_dict = {"key1_s": [sett.r_key_s, sett.c_key_s, sett.name_key_s],
                       "article_s": [sett.r_article_s, sett.c_article_s, sett.name_article_s],
                       "brand_s": [sett.r_brand_s, sett.c_brand_s, sett.name_brand_s],
                       "name_s": [sett.r_name_s, sett.c_name_s, sett.name_name_s],
                       "count_s": [sett.r_count_s, sett.c_count_s, sett.name_count_s],
                       "price_s": [sett.r_price_s, sett.c_price_s, sett.name_price_s],
                       "mult_s": [sett.r_mult_s, sett.c_mult_s, sett.name_mult_s],
                       "notice_s": [sett.r_notice_s, sett.c_notice_s, sett.name_notice_s],
                       "currency_s": [sett.r_currency_s, sett.c_currency_s, sett.name_currency_s],
                       }

            cols = []
            rows = []
            del_from_dict = []
            for k in rc_dict:
                if rc_dict[k][0] != None:
                    rows.append(rc_dict[k][0])
                    cols.append(rc_dict[k][1])
                else:
                    del_from_dict.append(k)
                # if rc_dict[k][1]:
            for d in del_from_dict:
                del rc_dict[d]
            # print(f"{rc_dict=}")

            if not rows:
                # print('cnt', rows)
                continue

            max_col = int(max(cols))
            max_row = int(max(rows)) or 1
            # print(f"{max_row=} {max_col=}")

            table = pd.DataFrame
            if frmt in ('xls', 'xlsx'):
                pandas_monkeypatch()
                try:
                    table = pd.read_excel(path_to_price, header=None, engine='openpyxl', nrows=max_row)
                    # print(f"{table=}")
                except Exception as read_ex:
                    pass
                if table.empty:
                    try:
                        table = pd.read_excel(path_to_price, header=None, nrows=max_row)
                    except Exception as read_ex_2:
                        pass
                if table.empty:
                    try:
                        table = pd.read_excel(path_to_price, header=None, engine='calamine', nrows=max_row)
                    except Exception as read_ex_3:
                        pass

            elif frmt == 'csv':
                table = pd.read_csv(path_to_price, header=None, sep=';', encoding='windows-1251', nrows=max_row,
                                    encoding_errors='ignore')

            # для XML
            if table.empty and frmt in ('xls', 'xml'):
                try:
                    tree = ET.parse(path_to_price)
                    root = tree.getroot()
                    header = []
                    for i in root.findall('{urn:schemas-microsoft-com:office:spreadsheet}Worksheet/{urn:schemas-microsoft-com:office:spreadsheet}Table/{urn:schemas-microsoft-com:office:spreadsheet}Row'):
                        header = [j.text for j in i.findall('{urn:schemas-microsoft-com:office:spreadsheet}Cell/{urn:schemas-microsoft-com:office:spreadsheet}Data')]
                        break

                    if len(header) < max_col:
                        continue

                    brk = False
                    for r, c, name in rc_dict.values():
                        if not name:
                            continue
                        if name not in str(header[c-1]):
                            brk = True
                            break

                    if not brk:
                        id_settig = sett.id
                        new_frmt = 'xml'
                        break
                    else:
                        continue
                except Exception as read_xml_ex:
                    pass

            if table.empty:
                print(f"Неизвестный формат")
                reason = "Неизвестный формат"
                break

            if len(table.columns) < max_col:
                continue

            brk = False
            for r, c, name in rc_dict.values():
                if not name:
                    continue
                # print(r, c, name, name in table.loc[r-1, c -1])
                if name not in str(table.loc[r - 1, c - 1]):
                    reason = name
                    brk = True
                    break

            if not brk:
                id_settig = sett.id

                break

        return id_settig, rc_dict, new_frmt, reason

    def load_data_to_db(self, sett, rc_dict, frmt, path_to_price, price_code, sess):
        try:
            loaded_rows = sett.pass_up  # пропускаются указанное кол-во строк
            r_limit = CHUNKSIZE
            method = None
            # print(f"{rc_dict}")
            cols = [int(i[1] - 1) for i in rc_dict.values()]
            # print(f"{cols=}")
            # cols_len_1 = len(cols)
            cols = set(cols)
            # cols_len_2 = len(cols)
            # if cols_len_1 != cols_len_2:
            #     return False

            # print(f"{rc_dict=}")
            dup_cols = dict()
            new_cols_name = dict() #{rc_dict[k][1] - 1: k for k in rc_dict}
            for k in rc_dict:
                if not new_cols_name.get(rc_dict[k][1]-1, None):
                    new_cols_name[rc_dict[k][1]-1] = k
                else:
                    dup_cols[k] = rc_dict[k][1]-1

            # print(f"{new_cols_name=}")
            # print(f"{dup_cols=}")
            while True:
                table = pd.DataFrame
                if frmt in ('xls', 'xlsx'):
                    pandas_monkeypatch()

                    try:
                        table = pd.read_excel(path_to_price, usecols=[*cols], header=None,
                                              nrows=r_limit, skiprows=loaded_rows, engine='openpyxl', na_filter=False)
                        method = 1
                    except Exception as ex1:
                        # print(ex1)
                        pass
                    try:
                        if table.empty and not method:
                            table = pd.read_excel(path_to_price, usecols=[*cols], header=None, nrows=r_limit, skiprows=loaded_rows,
                                                  na_filter=False)
                            method = 2
                    except Exception as ex2:
                        # print(ex2)
                        pass

                    if table.empty and not method:
                        table = pd.read_excel(path_to_price, usecols=[*cols], header=None, nrows=r_limit, skiprows=loaded_rows,
                                              na_filter=False, engine='calamine')
                elif frmt == 'csv':
                    try:
                        table = pd.read_csv(path_to_price, header=None, sep=';', encoding='windows-1251',
                                            usecols=[*cols], nrows=r_limit, skiprows=loaded_rows, encoding_errors='ignore', na_filter=False)
                    except pd.errors.EmptyDataError:
                        break

                if table.empty:
                    break
                table_size = len(table)

                loaded_rows += table_size

                # удаление последний строк в соотвествии с параметром "Пропуск снизу" (skipF)
                if table_size == r_limit:
                    if frmt in ('xls', 'xlsx'):
                        pandas_monkeypatch()
                        last = pd.DataFrame
                        if method == 1:
                            last = pd.read_excel(path_to_price, usecols=[*cols], header=None,
                                                 nrows=sett.pass_down, skiprows=loaded_rows, engine='openpyxl', na_filter=False)
                        elif method == 2:
                            if last.empty:
                                last = pd.read_excel(path_to_price, usecols=[*cols], header=None,
                                                     nrows=sett.pass_down, skiprows=loaded_rows, na_filter=False)
                        if last.empty:
                            last = pd.read_excel(path_to_price, usecols=[*cols], header=None, engine='calamine',
                                                 nrows=sett.pass_down, skiprows=loaded_rows, na_filter=False)

                        last = len(last)
                    elif frmt == 'csv':
                        last = 0
                        try:
                            last_t = pd.read_csv(path_to_price, header=None, sep=';', encoding='windows-1251', usecols=[*cols],
                                                 nrows=sett.pass_down, skiprows=loaded_rows, encoding_errors='ignore', na_filter=False)
                            last = len(last_t)
                        except pd.errors.EmptyDataError:
                            pass

                    if last < sett.pass_down:
                        n = sett.pass_down - last
                        table = table[:-n]
                elif sett.pass_down:
                    table = table[:-sett.pass_down]

                # print(f"{new_cols_name=}")
                table = table.rename(columns=new_cols_name)
                table['_07supplier_code'] = [price_code] * len(table)

                empty_cols_dict = {k: 1 for k in rc_dict}
                for i in dup_cols:
                    empty_cols_dict.pop(i)
                # print(f"{empty_cols_dict=}")
                # print(f"{table['article_s']=}")
                table = self.get_correct_df(table, empty_cols_dict, sess.connection())
                for i in dup_cols:
                    table[i] = table[new_cols_name[dup_cols[i]]]
                # print(f"2 {table=}")

                table.to_sql(name=self.TmpPrice_1.__tablename__, con=sess.connection(), if_exists='append', index=False)
            return True
        except KeyError as ke:
            # print(ke)
            raise ke
            # return False

    def load_data_from_xml_to_db(self, rc_dict, path_to_price, price_code, sess):
        try:
            cols_count = len(rc_dict)
            use_cols = [rc_dict[k][1]-1 for k in rc_dict]
            tree = ET.parse(path_to_price)
            root = tree.getroot()
            arr = np.array([])
            for i in root.findall('{urn:schemas-microsoft-com:office:spreadsheet}Worksheet/{urn:schemas-microsoft-com:office:spreadsheet}Table/{urn:schemas-microsoft-com:office:spreadsheet}Row')[1:]:
                row = np.array([j.text for j in i.findall('{urn:schemas-microsoft-com:office:spreadsheet}Cell/{urn:schemas-microsoft-com:office:spreadsheet}Data')])
                row = row[use_cols]
                arr = np.append(arr, row)

            arr.shape = -1, cols_count
            df = pd.DataFrame(arr, columns=use_cols)
            new_cols_name = {rc_dict[k][1] - 1: k for k in rc_dict}
            df = df.rename(columns=new_cols_name)
            df['_07supplier_code'] = [price_code] * len(df)

            empty_cols_dict = {k: 1 for k in rc_dict}
            df = self.get_correct_df(df, empty_cols_dict, sess.connection())

            df.to_sql(name=self.TmpPrice_1.__tablename__, con=sess.connection(), if_exists='append', index=False)
            # print(df)

            return True
        except KeyError:
            return False

    def get_correct_df(self, df, cols, con):
        '''for varchar(x), real, numeric, integer'''
        pk = []
        # берутся столбцы из таблицы: название столбца, максимальная длина его поля
        select()
        res = con.execute(text(
            f"SELECT column_name, character_maximum_length FROM information_schema.columns WHERE table_name = '{self.TmpPrice_1.__tablename__}' "
            f"and column_name != 'id'")).all()
        for i in res:
            if cols.get(i[0], None):
                # print(f'append {i[0]} {i[1]}')
                cols[i[0]] = i[1]
        res = con.execute(text(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.TmpPrice_1.__tablename__}' and is_nullable = 'NO' "
            f"and column_name != 'id'")).all()
        pk = [i[0] for i in res]

        # df = pd.read_excel(path_to_file, usecols=[cols[c][0] for c in cols], na_filter=False, sheet_name=sheet_name)
        # df = df.rename(columns={cols[c][0]: c for c in cols})
        # print(df.columns)
        # print(f"{cols=}")
        for c in cols:
            char_limit = cols[c]
            if char_limit:  # str
                df[c] = df[c].apply(lambda x: str(x)[:char_limit] or None)
            elif c in ('count_s', 'mult_s'):  # float/int
                df[c] = df[c].apply(to_int)
            elif c == 'price_s':
                df[c] = df[c].apply(to_numeric)
                # df[c] = df[c].replace('', 0)
                # df = df[df[c].apply(is_float)]
                # df[c] = np.float64(df[c])
            if c in pk:  # для PK
                df = df[df[c].notna()]
        # print(df['price_s'])
        # df.to_csv('test.csv', sep=';', decimal=',', encoding="windows-1251", index=False)
        return df
        # df.to_sql(name=table_name, con=con, if_exists='append', index=False, index_label=False, chunksize=CHUNKSIZE)




    def suppliers_goods_compare(self, price_code, sett, sess):
        key_conditions = and_(SupplierGoodsFix.import_setting == price_code, self.TmpPrice_1.key1_s == SupplierGoodsFix.key1)
        article_brand_conditions = and_(SupplierGoodsFix.import_setting == price_code, self.TmpPrice_1.article_s == SupplierGoodsFix.article_s,
                                        self.TmpPrice_1.brand_s == SupplierGoodsFix.brand_s)
        article_name_conditions = and_(SupplierGoodsFix.import_setting == price_code, self.TmpPrice_1.article_s == SupplierGoodsFix.article_s,
                                       self.TmpPrice_1.name_s == SupplierGoodsFix.name_s)
        compare_vars = {"Ключ": key_conditions, "Артикул + Бренд": article_brand_conditions,
                        "Артикул + НаименованиеП": article_name_conditions}

        if sett.compare in compare_vars.keys():
            # req = update(Price_1).where(compare_vars[sett.compare]
            #                             ).values(_01article=SupplierGoodsFix.article, _02brand=SupplierGoodsFix.brand,
            #                                      _03name=SupplierGoodsFix.name, _04count=SupplierGoodsFix.put_away_count,
            #                                      _05price=SupplierGoodsFix.price_s, _06mult=SupplierGoodsFix.mult_s,
            #                                      _20exclude=SupplierGoodsFix.sales_ban)
            # sess.execute(req)
            sess.execute(update(self.TmpPrice_1).where(and_(compare_vars[sett.compare], SupplierGoodsFix.article != None)).values(_01article=SupplierGoodsFix.article))
            sess.execute(update(self.TmpPrice_1).where(and_(compare_vars[sett.compare], SupplierGoodsFix.brand != None)).values(_02brand=SupplierGoodsFix.brand))
            sess.execute(update(self.TmpPrice_1).where(and_(compare_vars[sett.compare], SupplierGoodsFix.name != None)).values(_03name=SupplierGoodsFix.name))
            sess.execute(update(self.TmpPrice_1).where(and_(compare_vars[sett.compare], SupplierGoodsFix.put_away_count != 0)).values(_04count=SupplierGoodsFix.put_away_count))
            sess.execute(update(self.TmpPrice_1).where(and_(compare_vars[sett.compare], SupplierGoodsFix.price_s != 0)).values(_05price=SupplierGoodsFix.price_s))
            sess.execute(update(self.TmpPrice_1).where(and_(compare_vars[sett.compare], SupplierGoodsFix.mult_s != 0)).values(_06mult=SupplierGoodsFix.mult_s))
            sess.execute(update(self.TmpPrice_1).where(and_(compare_vars[sett.compare], SupplierGoodsFix.sales_ban != None)).values(_20exclude=SupplierGoodsFix.sales_ban))

    def cols_fix(self, price_code, sess):
        cols_name = {"01Артикул": [self.TmpPrice_1.article_s, "_01article"], "03Наименование": [self.TmpPrice_1.name_s, "_03name"],
                     "Примечание поставщика": [self.TmpPrice_1.notice_s, "notice_s"]}
        custom_cols = {"Ключ1 поставщика": self.TmpPrice_1.key1_s.__dict__['name'],
                       "Артикул поставщика": self.TmpPrice_1.article_s.__dict__['name'],
                       "Производитель поставщика": self.TmpPrice_1.brand_s.__dict__['name'],
                       "Наименование поставщика": self.TmpPrice_1.name_s.__dict__['name'],
                       "Количество поставщика": self.TmpPrice_1.count_s.__dict__['name'],
                       "Цена поставщика": self.TmpPrice_1.price_s.__dict__['name'],
                       "Валюта поставщика": self.TmpPrice_1.currency_s.__dict__['name'], }
        change_types = {"Начинается с": [lambda tb, x: tb.startswith(x), '^{}'],
                        "Содержит": [lambda tb, x: tb.contains(x), '{}'],
                        "Заканчивается на": [lambda tb, x: tb.endswith(x), '{}$'],
                        "Равно": [lambda tb, x: tb == x, '{}$'],
                        "Не равно": [lambda tb, x: tb != x, '{}$'],
                        "Добавить в конце": True,
                        "Добавить в начале": True,
                        }
        req = select(ColsFix).where(and_(ColsFix.price_code == price_code, ColsFix.col_change.in_(cols_name.keys())))
        cols_settings = sess.execute(req).scalars().all()  # Price_1.article_s

        for cs in cols_settings:
            # print(f"{cs.col_change}|{cs.change_type}|{cs.find}|{cs.set}")#|{cols_name[cs.col_find]}")
            change_type = change_types.get(cs.change_type, None)
            col_name = cols_name.get(cs.col_change, None)
            # print(f"{change_type}|{col_name}")
            if change_type and col_name:  # Производитель поставщика: Цена поставщика р. (Количество поставщика шт.)
                if cs.change_type in ("Добавить в конце", "Добавить в начале"):
                    change = cs.set
                    not_null_check = "and "

                    for c in custom_cols:
                        new_change = re.sub(c, f"',{custom_cols[c]},'", change)
                        if new_change != change:
                            change = new_change
                            not_null_check += f"{custom_cols[c]} is not NULL and "

                    if change.startswith("',"):
                        change = change[2:]
                    else:
                        change = f"'{change}"
                    if change.endswith(",'"):
                        change = change[:-2]
                    else:
                        change = f"{change}'"
                    # print(change)
                    if not_null_check != "and ":
                        not_null_check = not_null_check[:-5]
                    else:
                        not_null_check = ''

                    if cs.change_type == "Добавить в конце":
                        add_order = f"{cols_name[cs.col_change][0].__dict__['name']}, {change}"
                    else:
                        add_order = f"{change},{cols_name[cs.col_change][0].__dict__['name']}"

                    sess.execute(text(f"update {self.TmpPrice_1.__tablename__} set {cols_name[cs.col_change][1]} = "
                                      f"CONCAT({add_order}) "
                                      f"where _07supplier_code = '{price_code}' {not_null_check}"))
                    # return
                    # pb = func.concat(Price_1.brand_s, ' ', func.cast(change.format(brand_s=Price_1.brand_s, article_s=Price_1.article_s), String))
                    # req = update(Price_1).where(Price_1._07supplier_code == price_code
                    #                             ).values({cols_name[a.col_name][1]: func.concat(cols_name[a.col_name][0],
                    #                                                                             # pb
                    #                         # change.format(brand_s=Price_1.brand_s, article_s=Price_1.article_s)
                    #                                                                             )
                    # })
                    sess.execute(req)
                else:
                    req = update(self.TmpPrice_1).where(
                        and_(self.TmpPrice_1._07supplier_code == price_code, change_type[0](cols_name[cs.col_change][0], cs.find))
                    ).values({cols_name[cs.col_change][1]: cols_name[cs.col_change][0].
                             regexp_replace(change_type[1].format(cs.find), "" if not cs.set else cs.set)})
                    sess.execute(req)

    def words_except(self, sess, price_code):
        cols_dict = {"Ключ1 поставщика": self.TmpPrice_1.key1_s, "Артикул поставщика": self.TmpPrice_1.article_s,
                     "Производитель поставщика": self.TmpPrice_1.brand_s,
                     "Наименование поставщика": self.TmpPrice_1.name_s, " Валюта поставщика": self.TmpPrice_1.currency_s,
                     "Примечание поставщика": self.TmpPrice_1.notice_s,
                     "Ключ1П": self.TmpPrice_1.key1_s, "АртикулП": self.TmpPrice_1.article_s, "ПроизводительП": self.TmpPrice_1.brand_s,
                     "НаименованиеП": self.TmpPrice_1.name_s, " ВалютаП": self.TmpPrice_1.currency_s, "ПримечаниеП": self.TmpPrice_1.notice_s,
                     }
        check_types = {"Начинается с": lambda col, x: col.startswith(x),  # '^{}'],
                       "Содержит": lambda col, x: col.contains(x),  # '{}'],
                       "Не содержит": lambda col, x: not_(col.contains(x)),  # '{}'],
                       "Заканчивается на": lambda col, x: col.endswith(x),  # '{}$'],
                       "Равно": lambda col, x: col == x,
                       "Не равно": lambda col, x: col != x,
                       }
        words = sess.execute(select(ColsFix).where(and_(ColsFix.price_code == price_code,
                                                        ColsFix.col_change == '20ИсключитьИзПрайса'))).scalars().all()

        not_like = dict()
        for w in words:
            # print(f"{w.col_find}|{w.change_type}|{w.set}")
            check = check_types.get(f"{w.change_type}", None)
            col = cols_dict.get(w.col_find, None)
            # print(check, col)
            if check and col:
                if w.change_type == 'Не содержит':
                    # not_like.append([col, w.find])
                    if not_like.get(w.col_find, None):
                        not_like[w.col_find].append(w.find)
                    else:
                        not_like[w.col_find] = [w.find]
                    continue
                sess.execute(update(self.TmpPrice_1).where(and_(check(col, w.find), self.TmpPrice_1._20exclude==None))
                             .values(_20exclude=w.set))

        if not_like:
            # print(not_like)
            for i in not_like:
                cond = []
                values = ''
                not_like_cols = set(not_like[i])
                for c in not_like_cols:
                    # print(c)
                    values += f"{c}, "
                    cond.append(cols_dict[i].contains(c))
                values = values[:-2]
                # print(cond)
                sess.execute(
                    update(self.TmpPrice_1).where(and_(not_(or_(*cond)), self.TmpPrice_1._20exclude == None))
                    .values(_20exclude=values[:50]))


    def apply_discount(self, sess, price_code, col):
        discounts = sess.execute(select(ColsFix).where(and_(ColsFix.price_code == price_code,
                                                            ColsFix.col_change.in_(['05Цена', 'Цена поставщика'])))).scalars().all()
        price_cols = {'05Цена': self.TmpPrice_1._05price, 'Цена поставщика': self.TmpPrice_1._05price} # price_s
        for dscnt in discounts:
            # print(dscnt.set, (1 + float(dscnt.set)), dscnt.col_change, dscnt.find)
            sess.execute(update(self.TmpPrice_1).where(self.TmpPrice_1._14brand_filled_in == dscnt.find).values(
                {price_cols[dscnt.col_change].__dict__['name']: price_cols[dscnt.col_change] * (1 + float(dscnt.set))}))


    def create_csv(self, sess, price_code, csv_cols_dict, start_calc_price_time, new_update_time):
        cur_time = datetime.datetime.now()
        # sender.send(["add", mp.current_process().name, price_code, 1, f"Формирование csv..."])
        self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, "Обработка 13 ...", False)

        try:
            df = pd.DataFrame(columns=csv_cols_dict.keys())
            df.to_csv(fr"{settings_data['exit_1_dir']}/{price_code}.csv", sep=';', decimal=',',
                      encoding="windows-1251", index=False, errors='ignore')

            limit = CHUNKSIZE
            loaded = 0
            while True:
                req = select(*[csv_cols_dict[k].label(k) for k in csv_cols_dict]).order_by(self.TmpPrice_1.id).offset(
                    loaded).limit(limit)
                df = pd.read_sql_query(req, sess.connection(), index_col=None)
                df_len = len(df)

                if not df_len:
                    break
                df.to_csv(fr"{settings_data['exit_1_dir']}/{price_code}.csv", mode='a',
                          sep=';', decimal=',', encoding="windows-1251", index=False, header=False, errors='ignore')
                loaded += df_len
                # print(df)
            # add_log_cf(LOG_ID, "csv сформирован", sender, price_code, color, cur_time)
            self.add_log(self.file_size_type, price_code, "csv сформирован", cur_time)

            total_price_calc_time = str(datetime.datetime.now() - start_calc_price_time)[:7]
            # sender.send(["log", LOG_ID, f"+ {price_code} готов! ({self.cur_file_count}/{self.total_file_count}) [{total_price_calc_time}]",
            #              f"<span style='color:{colors.green_log_color};font-weight:bold;'>✔</span> "
            #              f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
            #              f"{price_code}</span> готов! ({self.cur_file_count}/{self.total_file_count}) [{total_price_calc_time}]"])
            self.log.add(LOG_ID,
                         f"+ {price_code} готов! ({self.cur_file_count}/{self.total_file_count}) [{total_price_calc_time}]",
                         f"<span style='color:{colors.green_log_color};font-weight:bold;'>✔</span> "
                         f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{price_code}</span> готов! ({self.cur_file_count}/{self.total_file_count}) [{total_price_calc_time}]")
            cnt = sess.execute(select(func.count()).select_from(self.TmpPrice_1)).scalar()
            cnt_wo_article = sess.execute(select(func.count()).select_from(self.TmpPrice_1).where(self.TmpPrice_1._01article == None)).scalar()
            sess.execute(update(PriceReport).where(PriceReport.price_code == price_code)
                         .values(info_message="Ок", updated_at=new_update_time, row_count=cnt, row_wo_article=cnt_wo_article))
            return True
        except PermissionError:
            # sender.send(["log", LOG_ID, f"Не удалось сформировать прайс {price_code} ({self.cur_file_count}/{self.total_file_count})",
            #              f"<span style='color:{colors.orange_log_color};'>Не удалось сформировать прайс</span> "
            #              f"<span style='background-color:hsl({color[0]}, {color[1]}%, {color[2]}%);'>"
            #              f"{price_code}</span> ({self.cur_file_count}/{self.total_file_count})"])
            self.log.add(LOG_ID,
                         f"Не удалось сформировать прайс {price_code} ({self.cur_file_count}/{self.total_file_count})",
                         f"<span style='color:{colors.orange_log_color};'>Не удалось сформировать прайс</span> "
                         f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{price_code}</span> ({self.cur_file_count}/{self.total_file_count})")
            # self.add_log(self.file_size_type, price_code, f"Ошибка формата", cur_time)
            return False

    def add_log(self, row_id, price_code, msg, cur_time=None, new_row=False):
        # лог с выводом этапа в таблицу
        if cur_time:
            log_text = "{}{price}{} {log_main_text} [{time_spent}]"
            self.log.add(LOG_ID, log_text.format('', '', price=price_code, log_main_text=msg,
                                                                time_spent=str(datetime.datetime.now() - cur_time)[:7]),
                         log_text.format(f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>",
                                         '</span>',
                                         price=price_code, log_main_text=msg,
                                         time_spent=str(datetime.datetime.now() - cur_time)[:7]))

            self.UpdatePriceStatusTableSignal.emit(row_id, price_code, msg, new_row)
        else:
            log_text = "{}{price}{} {log_main_text}"
            self.log.add(LOG_ID, log_text.format('', '', price=price_code, log_main_text=msg),
                         log_text.format(f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>",
                                         '</span>', price=price_code, log_main_text=msg))
# def create_children_csv(sess, sender, price_code, csv_cols_dict, color, parent_price_code):
#     cur_time = datetime.datetime.now()
#     sender.send(["add", mp.current_process().name, price_code, 1, f"Формирование csv..."])
#
#     try:
#         df = pd.DataFrame(columns=csv_cols_dict.keys())
#         df.to_csv(fr"{settings_data['mail_files_dir']}/{price_code} дочерний парйс {parent_price_code}.csv", sep=';', decimal=',',
#                   encoding="windows-1251", index=False, errors='ignore')
#
#         limit = CHUNKSIZE
#         loaded = 0
#         while True:
#             req = (select(*[csv_cols_dict[k].label(k) for k in csv_cols_dict]).order_by(Price_1._17code_unique).offset(loaded).limit(limit))
#             df = pd.read_sql_query(req, sess.connection(), index_col=None)
#             df_len = len(df)
#             if not df_len:
#                 break
#             df.to_csv(fr"{settings_data['mail_files_dir']}/{price_code} дочерний парйс {parent_price_code}.csv", mode='a',
#                       sep=';', decimal=',', encoding="windows-1251", index=False, header=False, errors='ignore')
#             loaded += df_len
#             # print(df)
#             add_log_cf(LOG_ID, "сырой дочерний csv сформирован", sender, price_code, color, cur_time)
#             return True
#     except PermissionError:
#         return False

def to_int(x):
    try:
        x = int(re.search(r'\d+', f"{x}").group(0))
        if math.isnan(x) or math.isinf(x):
            return 0
        if 2147483647 < x < -2147483648:  # int
            return 0
        return x
    except:
        return 0

def to_numeric(x):
    try:
        # x = Decimal(str(x).replace(',', '.')).quantize(Decimal("1.00"), ROUND_FLOOR)
        x = float(str(x).replace(',', '.'))
        if math.isnan(x) or math.isinf(x):
            return 0
        if -9999999999 < x < 9999999999:  # numeric 12,2
            return x
        return 0
    except:
        return 0

class PriceReportUpdate(QThread):
    UpdateInfoTableSignal = Signal(list)
    UpdatePriceReportTime = Signal(str)

    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)
    def run(self):
        try:
            with session() as sess:
                reports = []
                req = select(PriceReport.price_code.label("Код прайса"), PriceReport.info_message.label("Статус"),
                                              PriceReport.updated_at.label("Время")).where(PriceReport.info_message!='Ок'
                                                                                           ).order_by(PriceReport.price_code)
                res = sess.execute(req).all()
                for r in res:
                    reports.append(r)
                req = select(PriceReport.price_code.label("Код прайса"), PriceReport.info_message.label("Статус"),
                                              PriceReport.updated_at.label("Время")).where(PriceReport.info_message=='Ок'
                                                                                           ).order_by(PriceReport.price_code)
                res = sess.execute(req).all()
                for r in res:
                    reports.append(r)
                for r in reports:
                    self.UpdateInfoTableSignal.emit(r)

                self.UpdatePriceReportTime.emit(str(datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))
                req = select(PriceReport.price_code.label("Код прайса"), PriceReport.info_message.label("Статус стандартизации"),
                             PriceReport.row_count.label("Кол-во позиций"), PriceReport.row_wo_article.label("Позиций с пустым артикулом"),
                             PriceReport.updated_at.label("Время (1)"), PriceReport.info_message2.label("Статус обработки"),
                             PriceReport.updated_at_2_step.label("Время (2)"), PriceReport.row_count_2.label("Итоговое кол-во"),
                             PriceReport.del_pos.label("Удалено")).order_by(PriceReport.price_code)
                # (func.round(PriceReport.del_pos / (PriceReport.row_count_2 + PriceReport.del_pos), 2)).label("Процент удалённых позиций"))
                df = pd.read_sql(req, engine)
                df.to_csv(fr"{settings_data['catalogs_dir']}/{REPORT_FILE}", sep=';', encoding="windows-1251",
                          index=False, header=True, errors='ignore')

                # self.log.add(LOG_ID, f"Отчёт обновлён", f"<span style='color:{colors.green_log_color};'>Отчёт обновлён</span>  ")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "PriceReportUpdate Error", ex_text)


class PriceReportReset(QThread):
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)
    def run(self):
        try:
            with session() as sess:
                sess.query(PriceReport).delete()
                sess.commit()
            self.log.add(LOG_ID, f"Отчёт обнулён", f"<span style='color:{colors.green_log_color};'>Отчёт обнулён</span>  ")

        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "PriceReportReset Error", ex_text)


class SaveMBVAlue(QThread):
    logs_id = [1, 3] # PriceReader, Calculate
    def __init__(self, module_id, var, log=None, parent=None):
        self.log = log
        self.module_id = module_id
        self.var = var
        QThread.__init__(self, parent)
    def run(self):
        try:
            with session() as sess:
                sess.execute(update(AppSettings).where(AppSettings.param == f"mb_limit_{self.module_id+1}").values(var=self.var))
                sess.commit()
            self.log.add(self.logs_id[self.module_id], f"Мин. лимит сохранён", f"<span style='color:{colors.green_log_color};'>Мин. лимит сохранён</span>  ")

        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(self.logs_id[self.module_id], "SaveMBVAlue Error", ex_text)