import psycopg2.errors
import sqlalchemy.exc
from PySide6.QtCore import QThread, Signal
import time
import traceback
import datetime
import random
import os
from sqlalchemy import text, select, delete, insert, update, Sequence, and_, not_, func, distinct, or_, String, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
import numpy as np
import pandas as pd
# from python_calamine.pandas import pandas_monkeypatch
# pd.set_option('future.no_silent_downcasting', True)
import warnings
warnings.filterwarnings('ignore')

import colors
from models import (Base2, Base2_1, Price_2, Price_2_2, PriceReport, TotalPrice_1, BasePrice, MassOffers, SupplierPriceSettings,
                    Data07, Data09, Data15, Data07_14, Buy_for_OS, TotalPrice_2, Brands, ExchangeRate, AppSettings)
import setting
engine = setting.get_engine()
# engine.echo = True
session = sessionmaker(engine)
settings_data = setting.get_vars()

CHUNKSIZE = int(settings_data["chunk_size"])
REPORT_FILE = r"price_report.csv"
LOG_ID = 3
TABLES = [Price_2, Price_2_2]
BASE = [Base2, Base2_1]

class CalculateClass(QThread):
    SetButtonEnabledSignal = Signal(bool)
    UpdatePriceStatusTableSignal = Signal(int, str, str, bool)
    ResetPriceStatusTableSignal = Signal(int)
    SetTotalTome = Signal(bool)
    SetProgressBarValue = Signal(int, int)
    TotalCountSignal = Signal(int)
    UpdatePriceReportTableSignal = Signal(bool)

    isPause = None
    total_file_count = 0
    cur_file_count = 0
    # last_total_update_time = datetime.datetime(2025, 1, 1)

    def __init__(self, file_size_limit, log=None, parent=None):
        self.log = log
        if file_size_limit[0] == '>':
            self.file_size_type = 1  # 0 - light, 1 - heavy
        else:
            self.file_size_type = 0
        self.TmpPrice_2 = TABLES[self.file_size_type]
        QThread.__init__(self, parent)


    def run(self):
        global session, engine
        # print('Поток', self.file_size_type)
        wait_sec = 15
        self.SetButtonEnabledSignal.emit(False)
        while not self.isPause:
            start_cycle_time = datetime.datetime.now()
            try:
                new_files = []
                with session() as sess:
                    self.mb_limit = int(sess.execute(select(AppSettings.var).where(AppSettings.param == 'mb_limit_2')).scalar())

                    # check new prices
                    for file in os.listdir(settings_data['exit_1_dir']):
                        price_code = '.'.join(file.split('.')[:-1])

                        req = select(SupplierPriceSettings.price_code).where(and_(SupplierPriceSettings.price_code == price_code,
                                                                                  func.upper(SupplierPriceSettings.calculate) == 'ДА',
                                                                                  func.upper(SupplierPriceSettings.works) == 'ДА'))
                        res = sess.execute(req).scalar()
                        if not res:
                            sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).
                                         values(info_message="Нет в условиях (4.0 - Настройка прайсов)",
                                                updated_at_2_step=datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))
                            continue

                        req = select(PriceReport.price_code).where(and_(PriceReport.price_code == price_code,
                                                                        PriceReport.updated_at != None,
                                                            or_(PriceReport.updated_at_2_step == None,
                                                            PriceReport.updated_at_2_step < PriceReport.updated_at)))
                        res = sess.execute(req).scalar()

                        req = select(Data07.update_time).where(Data07.setting == price_code)
                        update_time = sess.execute(req).scalar()
                        if not update_time:
                            sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).
                                         values(info_message2='Нет настроек или срока обновления в 07Данные',
                                                updated_at_2_step=datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))
                            # sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code == price_code).delete()
                            continue

                        if res:
                            time_edit = datetime.datetime.fromtimestamp(os.path.getmtime(fr"{settings_data['exit_1_dir']}/{file}"))

                            if (datetime.datetime.now() - time_edit).days - update_time >= 30:
                                sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).
                                             values(info_message2='Не подходит период обновления',
                                                    updated_at_2_step=datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))
                                continue
                            new_files.append(file)

                    sess.commit()


                # new_files = ['2ETP.csv',]
                # new_files = ['1IMP.csv', '1LAM.csv', '1STP.csv', '1АТХ.csv', '1МТЗ.csv', '2ETP.csv', ]
                files = []
                for f in new_files:
                    if self.check_file_condition(f):
                        files.append(f)

                if files:
                    self.total_file_count = len(files)
                    self.cur_file_count = 0
                    self.SetTotalTome.emit(True)
                    self.log.add(LOG_ID, f"Начало обработки [{self.file_size_type+1}]")
                    cur_time = datetime.datetime.now()

                    for f in files:
                        self.calculate_file(f)

                    self.log.add(LOG_ID, f"Обработка закончена [{str(datetime.datetime.now() - cur_time)[:7]}] [{self.file_size_type+1}]")
                    self.SetTotalTome.emit(False)
                    self.UpdatePriceReportTableSignal.emit(True)

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
        MB = os.path.getsize(f"{settings_data['exit_1_dir']}/{file_name}") / 1024 / 1024
        if self.file_size_type == 1:
            if MB > self.mb_limit:
                return True
            return False

        if MB <= self.mb_limit:
            return True
        return False

    def calculate_file(self, file):
        start_time = datetime.datetime.now()
        try:
            price_code = '.'.join(file.split('.')[:-1])
            self.color = [random.randrange(0, 360), random.randrange(55, 100), 90]

            inspct = inspect(engine)
            if inspct.has_table(self.TmpPrice_2.__tablename__):
                self.TmpPrice_2.__table__.drop(engine)

            BASE[self.file_size_type].metadata.create_all(engine)

            with session() as sess:
                sess.execute(text(f"ALTER TABLE {self.TmpPrice_2.__tablename__} SET (autovacuum_enabled = false);"))
                sess.commit()

                self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, 'Загрузка, удаление по первому условию, удаление дублей ...', True)
                cur_time = datetime.datetime.now()

                cols_for_price = [TotalPrice_1.key1_s, TotalPrice_1.article_s, TotalPrice_1.brand_s, TotalPrice_1.name_s,
                                  TotalPrice_1.count_s, TotalPrice_1.price_s, TotalPrice_1.currency_s, TotalPrice_1.mult_s,
                                  TotalPrice_1.notice_s, TotalPrice_1._01article, TotalPrice_1._01article_comp, TotalPrice_1._02brand,
                                  TotalPrice_1._14brand_filled_in, TotalPrice_1._03name, TotalPrice_1._04count,
                                  TotalPrice_1._05price, TotalPrice_1._06mult,
                                  TotalPrice_1._15code_optt, TotalPrice_1._07supplier_code, TotalPrice_1._20exclude,
                                  TotalPrice_1._13grad, TotalPrice_1._17code_unique, TotalPrice_1._18short_name]
                cols_for_price = {i: i.__dict__['name'] for i in cols_for_price}
                price = select(*cols_for_price.keys()).where(TotalPrice_1._07supplier_code == price_code)
                sess.execute(insert(self.TmpPrice_2).from_select(cols_for_price.values(), price))
                sess.commit()

                # Удаление по первому условию
                del_art = sess.query(self.TmpPrice_2).where(self.TmpPrice_2._01article == None).delete()
                del_brand = sess.query(self.TmpPrice_2).where(self.TmpPrice_2._14brand_filled_in == None).delete()
                del_price = sess.query(self.TmpPrice_2).where(or_(self.TmpPrice_2._05price <= 0, self.TmpPrice_2._05price == None)).delete()
                del_count = sess.query(self.TmpPrice_2).where(or_(self.TmpPrice_2._04count == None, self.TmpPrice_2._05price <= 0)).delete()
                del_20 = sess.query(self.TmpPrice_2).where(self.TmpPrice_2._20exclude != None).delete()
                del_positions_1 = del_art + del_brand + del_price + del_count + del_20

                # Удаление дублей 01Артикул, 14Производитель заполнен
                del_dupl = self.del_duples(sess)

                update_step_time = str(datetime.datetime.now() - cur_time)[:7]
                self.log.add(LOG_ID, f"{price_code} Загрузка, удаление по первому условию ({del_positions_1}), удаление дублей ({del_dupl}) [{update_step_time}]",
                             f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>{price_code}</span> "
                             f"Загрузка, удаление по первому условию (<span style='color:{colors.orange_log_color + ';font-weight:bold' if del_positions_1 else 'black'};'>{del_positions_1}</span>), "
                             f"удаление дублей (<span style='color:{colors.orange_log_color + ';font-weight:bold' if del_dupl else 'black'};'>{del_dupl}</span>) [{update_step_time}]")


                self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, 'data 07, 09 ...', False)
                cur_time = datetime.datetime.now()

                data7_set = sess.execute(select(Data07).where(Data07.setting == price_code)).scalar()
                sess.execute(update(self.TmpPrice_2).values(delay=data7_set.delay, to_price=data7_set.to_price, sell_for_OS=data7_set.sell_os,
                    markup_os=data7_set.markup_os, max_decline=data7_set.max_decline, markup_holidays=data7_set.markup_holidays,
                    markup_R=data7_set.markup_R, min_markup=data7_set.min_markup, min_wholesale_markup=data7_set.min_wholesale_markup,
                    markup_wh_goods=data7_set.markup_wholesale, grad_step=data7_set.grad_step, wh_step=data7_set.wholesale_step,
                    access_pp=data7_set.access_pp, unload_percent=data7_set.unload_percent))

                sess.execute(update(self.TmpPrice_2).values(_09code_supl_goods=func.upper(
                    self.TmpPrice_2._07supplier_code+self.TmpPrice_2._01article_comp+self.TmpPrice_2._02brand)))
                sess.execute(update(self.TmpPrice_2).where(self.TmpPrice_2._09code_supl_goods==Data09.code_09).
                             values(put_away_zp=Data09.put_away_zp, reserve_count=Data09.reserve_count))
                self.add_log(self.file_size_type, price_code, 'data 07, 09', cur_time)


                self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, 'Базовая цена, Предложений в опте ...', False)
                cur_time = datetime.datetime.now()

                sess.execute(update(self.TmpPrice_2).where(and_(self.TmpPrice_2._01article_comp == BasePrice.article,
                                                        self.TmpPrice_2._14brand_filled_in == BasePrice.brand))
                             .values(price_b=BasePrice.price_b, min_price=BasePrice.min_price, min_supplier=BasePrice.min_supplier))
                sess.execute(update(self.TmpPrice_2).where(and_(self.TmpPrice_2._01article_comp == MassOffers.article,
                                                        self.TmpPrice_2._14brand_filled_in == MassOffers.brand))
                             .values(offers_wh=MassOffers.offers_count))

                self.add_log(self.file_size_type, price_code, 'Базовая цена, Предложений в опте', cur_time)


                self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, 'data 07&14 ...', False)
                cur_time = datetime.datetime.now()

                sess.execute(update(self.TmpPrice_2).where(and_(self.TmpPrice_2._07supplier_code == Data07_14.setting,
                                                        self.TmpPrice_2._14brand_filled_in == Data07_14.correct))
                             .values(markup_pb=Data07_14.markup_pb)) # code_pb_p=Data07_14.code_pb_p

                self.add_log(self.file_size_type, price_code, 'data 07&14', cur_time)

                self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, '06Кратность, 05Цена плюс, data 15 ...', False)
                cur_time = datetime.datetime.now()

                sess.execute(update(self.TmpPrice_2).where(or_(self.TmpPrice_2.markup_holidays == None, self.TmpPrice_2.markup_holidays == 0))
                             .values(_06mult_new=self.TmpPrice_2._06mult))
                sess.execute(update(self.TmpPrice_2).where(and_(self.TmpPrice_2._06mult_new == None,
                                                        self.TmpPrice_2.markup_holidays > self.TmpPrice_2._05price * self.TmpPrice_2._04count))
                             .values(_06mult_new=self.TmpPrice_2._04count))

                sess.execute(update(self.TmpPrice_2).where(self.TmpPrice_2._06mult_new == None)
                             .values(_06mult_new=func.ceil(func.greatest(self.TmpPrice_2._06mult, self.TmpPrice_2.markup_holidays / self.TmpPrice_2._05price))))

                sess.execute(update(self.TmpPrice_2).where(and_(self.TmpPrice_2.markup_holidays > self.TmpPrice_2._05price * self.TmpPrice_2._04count, self.TmpPrice_2._04count>0)).
                             values(_05price_plus=self.TmpPrice_2.markup_holidays / self.TmpPrice_2._04count))
                sess.execute(update(self.TmpPrice_2).where(self.TmpPrice_2._05price_plus == None).values(_05price_plus=self.TmpPrice_2._05price))

                sess.execute(update(self.TmpPrice_2).where(self.TmpPrice_2._15code_optt==Buy_for_OS.article_producer).values(buy_count=Buy_for_OS.buy_count))

                sess.execute(update(self.TmpPrice_2).values(count=self.TmpPrice_2._04count))
                sess.execute(update(self.TmpPrice_2).where(self.TmpPrice_2.reserve_count > 0).values(count=self.TmpPrice_2._04count-self.TmpPrice_2.reserve_count))

                sess.execute(update(self.TmpPrice_2).where(self.TmpPrice_2.count < self.TmpPrice_2._06mult_new).values(mult_less='-'))

                self.add_log(self.file_size_type, price_code, '06Кратность, 05Цена плюс, data 15', cur_time)

                cur_time = datetime.datetime.now()
                # self.TmpPrice_2._10original, self.TmpPrice_2._19min_price, self.TmpPrice_2.low_price, self.TmpPrice_2.code_pb_p,
                cols_for_total = [self.TmpPrice_2.key1_s, self.TmpPrice_2.article_s, self.TmpPrice_2.brand_s, self.TmpPrice_2.name_s,
                                  self.TmpPrice_2.count_s, self.TmpPrice_2.price_s, self.TmpPrice_2.currency_s, self.TmpPrice_2.mult_s,
                                  self.TmpPrice_2.notice_s,
                                  self.TmpPrice_2._01article, self.TmpPrice_2._01article_comp, self.TmpPrice_2._02brand, self.TmpPrice_2._03name, self.TmpPrice_2._04count,
                                  self.TmpPrice_2._05price, self.TmpPrice_2._06mult, self.TmpPrice_2._07supplier_code,
                                  self.TmpPrice_2._09code_supl_goods,
                                  self.TmpPrice_2._13grad, self.TmpPrice_2._14brand_filled_in,
                                  self.TmpPrice_2._15code_optt,
                                  self.TmpPrice_2._17code_unique, self.TmpPrice_2._18short_name,
                                  self.TmpPrice_2._20exclude, self.TmpPrice_2.to_price,
                                  self.TmpPrice_2.delay, self.TmpPrice_2.sell_for_OS, self.TmpPrice_2.markup_os, self.TmpPrice_2.max_decline,
                                  self.TmpPrice_2.markup_holidays, self.TmpPrice_2.markup_R, self.TmpPrice_2.min_markup,
                                  self.TmpPrice_2.min_wholesale_markup, self.TmpPrice_2.markup_wh_goods,
                                  self.TmpPrice_2.grad_step, self.TmpPrice_2.wh_step, self.TmpPrice_2.access_pp, self.TmpPrice_2.unload_percent,
                                  self.TmpPrice_2.put_away_zp, self.TmpPrice_2.offers_wh, self.TmpPrice_2.price_b,
                                  self.TmpPrice_2.count, self.TmpPrice_2.markup_pb, self.TmpPrice_2._06mult_new,
                                  self.TmpPrice_2.mult_less, self.TmpPrice_2._05price_plus, self.TmpPrice_2.reserve_count, self.TmpPrice_2.buy_count,
                                  self.TmpPrice_2.min_price, self.TmpPrice_2.min_supplier,
                                  ]

                # формирование csv
                if not self.create_csv(sess, price_code, start_time):
                    return

                # перенос данных в total
                sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code == price_code).delete()

                cols_for_total = {i: i.__dict__['name'] for i in cols_for_total}
                total = select(*cols_for_total.keys())
                sess.execute(insert(TotalPrice_2).from_select(cols_for_total.values(), total))


                cnt = sess.execute(select(func.count()).select_from(self.TmpPrice_2)).scalar()


                sess.execute(update(PriceReport).where(PriceReport.price_code == price_code)
                             .values(info_message2="Ок", updated_at_2_step=start_time.strftime("%Y.%m.%d %H:%M:%S"),
                                     db_added=start_time.strftime("%Y.%m.%d %H:%M:%S"),
                                     row_count_2=cnt, del_art=del_art, del_brand=del_brand, del_price=del_price,
                                     del_count=del_count, del_20=del_20, del_dupl=del_dupl))
                total_cnt = sess.execute(select(func.count()).select_from(TotalPrice_2)).scalar()
                sess.commit()
                self.add_log(self.file_size_type, price_code, 'создание csv, загрузка в БД', cur_time)

            self.TotalCountSignal.emit(total_cnt)

            self.TmpPrice_2.__table__.drop(engine)

            total_price_calc_time = str(datetime.datetime.now() - start_time)[:7]
            self.log.add(LOG_ID, f"+ {price_code} готов! ({self.cur_file_count + 1}/{self.total_file_count}) [{total_price_calc_time}]",
                         f"<span style='color:{colors.green_log_color};font-weight:bold;'>✔</span> "
                         f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{price_code}</span> готов! ({self.cur_file_count + 1}/{self.total_file_count}) [{total_price_calc_time}]")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"ERROR ({price_code})", ex_text)
        finally:
            self.cur_file_count += 1
            self.SetProgressBarValue.emit(self.cur_file_count, self.total_file_count) # +1

    def del_duples(self, sess):
        # D для всех дублей
        duples = select(self.TmpPrice_2._15code_optt).group_by(self.TmpPrice_2._15code_optt).having(func.count(self.TmpPrice_2.id) > 1)
        sess.execute(update(self.TmpPrice_2).where(self.TmpPrice_2._15code_optt==duples.c._15code_optt).values(_20exclude='D'))

        # D1 не с мин. ценой среди D
        min_p_table = (select(self.TmpPrice_2._15code_optt, func.min(self.TmpPrice_2._05price).label('min_p')).
                       where(self.TmpPrice_2._20exclude=='D').group_by(self.TmpPrice_2._15code_optt))
        sess.execute(update(self.TmpPrice_2).where(and_(self.TmpPrice_2._15code_optt==min_p_table.c._15code_optt,
                                                   self.TmpPrice_2._05price==min_p_table.c.min_p)).values(_20exclude='D1'))

        # D2 не с макс. кол-вом среди D1
        max_c_table = (select(self.TmpPrice_2._15code_optt, func.max(self.TmpPrice_2._04count).label('max_c')).where(self.TmpPrice_2._20exclude == 'D1').
                       group_by(self.TmpPrice_2._15code_optt))
        sess.execute(update(self.TmpPrice_2).where(and_(self.TmpPrice_2._15code_optt==max_c_table.c._15code_optt,
                                                   self.TmpPrice_2._04count==max_c_table.c.max_c)).values(_20exclude='D2'))

        # D3 не с макс. id среди D2
        max_id_table = (select(self.TmpPrice_2._15code_optt, func.max(self.TmpPrice_2.id).label('max_id')).where(self.TmpPrice_2._20exclude == 'D2').
                        group_by(self.TmpPrice_2._15code_optt))
        sess.execute(update(self.TmpPrice_2).where(self.TmpPrice_2.id==max_id_table.c.max_id).values(_20exclude='D3'))


        sess.execute(update(self.TmpPrice_2).where(self.TmpPrice_2._20exclude=='D3').values(_20exclude=None))
        dup_del = sess.query(self.TmpPrice_2).where(self.TmpPrice_2._20exclude != None).delete()

        return dup_del

    def create_csv(self, sess, price_code, start_time):
        self.UpdatePriceStatusTableSignal.emit(self.file_size_type, price_code, 'Формирование csv ...', False)

        try:
            df = pd.DataFrame(columns=["Ключ1 поставщика", "Артикул поставщика", "Производитель поставщика",
                                           "Наименование поставщика",
                                           "Количество поставщика", "Цена поставщика", "Кратность поставщика",
                                           "Примечание поставщика", "01Артикул", "02Производитель",
                                           "03Наименование", "05Цена", "06Кратность-", "07Код поставщика",
                                           "09Код + Поставщик + Товар",
                                           "13Градация", "14Производитель заполнен", "15КодТутОптТорг",
                                           "17КодУникальности", "18КороткоеНаименование",
                                           "20ИслючитьИзПрайса", "В прайс", "Отсрочка", "Продаём для ОС",
                                           "Наценка для ОС", "Наценка Р",
                                           "Наценка ПБ", "Мин наценка", "Наценка на оптовые товары", "Шаг градации",
                                           "Шаг опт", "Разрешения ПП", "УбратьЗП", "Предложений опт",
                                           "ЦенаБ", "Кол-во", "06Кратность", "Кратность меньше", "05Цена+",
                                           "Количество закупок", "% Отгрузки",
                                           "Мин. Цена", "Мин. Поставщик"])
            df.to_csv(fr"{settings_data['exit_2_dir']}/{price_code}.csv", sep=';', decimal=',',
                      encoding="windows-1251", index=False, errors='ignore')

            limit = CHUNKSIZE
            loaded = 0
            while True:
                req = select(self.TmpPrice_2.key1_s, self.TmpPrice_2.article_s, self.TmpPrice_2.brand_s, self.TmpPrice_2.name_s,
                                  self.TmpPrice_2.count_s, self.TmpPrice_2.price_s, self.TmpPrice_2.mult_s, self.TmpPrice_2.notice_s,
                                  self.TmpPrice_2._01article, self.TmpPrice_2._02brand, self.TmpPrice_2._03name,
                                  self.TmpPrice_2._05price, self.TmpPrice_2._06mult, self.TmpPrice_2._07supplier_code, self.TmpPrice_2._09code_supl_goods,
                                  self.TmpPrice_2._13grad, self.TmpPrice_2._14brand_filled_in, self.TmpPrice_2._15code_optt,
                                  self.TmpPrice_2._17code_unique, self.TmpPrice_2._18short_name, self.TmpPrice_2._20exclude,
                                  self.TmpPrice_2.to_price, self.TmpPrice_2.delay, self.TmpPrice_2.sell_for_OS, self.TmpPrice_2.markup_os, self.TmpPrice_2.markup_R,
                                  self.TmpPrice_2.markup_pb, self.TmpPrice_2.min_markup, self.TmpPrice_2.markup_wh_goods,
                                  self.TmpPrice_2.grad_step, self.TmpPrice_2.wh_step,  self.TmpPrice_2.access_pp, self.TmpPrice_2.put_away_zp,
                                  self.TmpPrice_2.offers_wh, self.TmpPrice_2.price_b, self.TmpPrice_2.count,
                                  self.TmpPrice_2._06mult_new, self.TmpPrice_2.mult_less, self.TmpPrice_2._05price_plus,
                                  self.TmpPrice_2.buy_count, self.TmpPrice_2.unload_percent, self.TmpPrice_2.min_price, self.TmpPrice_2.min_supplier)\
                    .order_by(self.TmpPrice_2.id).offset(loaded).limit(limit)
                df = pd.read_sql_query(req, sess.connection(), index_col=None)
                df_len = len(df)

                if not df_len:
                    break
                df.to_csv(fr"{settings_data['exit_2_dir']}/{price_code}.csv", mode='a',
                          sep=';', decimal=',', encoding="windows-1251", index=False, header=False, errors='ignore')
                loaded += df_len

            return True
        except PermissionError:
            load_time = str(datetime.datetime.now() - start_time)[:7]
            self.log.add(LOG_ID,
                         f"Не удалось сформировать прайс {price_code} ({self.cur_file_count + 1}/{self.total_file_count}) [{load_time}]",
                         f"Не удалось сформировать прайс <span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{price_code}</span> ({self.cur_file_count + 1}/{self.total_file_count}) [{load_time}]")
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


class PriceReportUpdate_2(QThread):
    UpdateInfoTableSignal = Signal(list)
    UpdatePriceReportTime = Signal(str)
    ResetPriceReportTime = Signal(bool)

    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)
    def run(self):
        try:
            with session() as sess:
                self.ResetPriceReportTime.emit(True)
                reports = []
                req = select(PriceReport.price_code.label("Код прайса"), PriceReport.info_message2.label("Статус"),
                                              PriceReport.updated_at_2_step.label("Время")).where(PriceReport.info_message2!='Ок'
                                                                                           ).order_by(PriceReport.price_code)
                res = sess.execute(req).all()
                for r in res:
                    reports.append(r)
                req = select(PriceReport.price_code.label("Код прайса"), PriceReport.info_message2.label("Статус"),
                                              PriceReport.updated_at_2_step.label("Время")).where(PriceReport.info_message2=='Ок'
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
                             PriceReport.updated_at_2_step.label("Время (2)"), PriceReport.db_added.label("Время добавления в БД"),
                             PriceReport.row_count_2.label("Итоговое кол-во"),
                             PriceReport.del_art.label("Уд. 01Артикул"), PriceReport.del_brand.label("Уд. 14Производитель заполнен"),
                             PriceReport.del_price.label("Уд. 05Цена"), PriceReport.del_count.label("Уд. 04Количество "),
                             PriceReport.del_20.label("Уд. 20ИсключитьИзПрайса"), PriceReport.del_dupl.label("Уд. Дубли"),
                             ).order_by(PriceReport.price_code)
                # (func.round(PriceReport.del_pos / (PriceReport.row_count_2 + PriceReport.del_pos), 2)).label("Процент удалённых позиций"))
                df = pd.read_sql(req, engine)
                df.to_csv(fr"{settings_data['catalogs_dir']}/{REPORT_FILE}", sep=';', encoding="windows-1251",
                          index=False, header=True, errors='ignore')

        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "PriceReportUpdate Error", ex_text)