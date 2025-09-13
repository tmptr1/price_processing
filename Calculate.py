from PySide6.QtCore import QThread, Signal
import time
import re
import traceback
import datetime
import random
import os
from sqlalchemy import text, select, delete, insert, update, Sequence, and_, not_, func, distinct, or_, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
import numpy as np
import pandas as pd
# from python_calamine.pandas import pandas_monkeypatch
# pd.set_option('future.no_silent_downcasting', True)
import warnings
warnings.filterwarnings('ignore')
# from decimal import Decimal, ROUND_FLOOR
# mp.freeze_support()

import colors
from models import (Price_2, PriceReport, TotalPrice_1, BasePrice, MassOffers, SupplierPriceSettings, Data07, Data09,
                    Data15, Data07_14, Buy_for_OS, TotalPrice_2, Brands, ExchangeRate)
from Logs import add_log_cf
import setting
engine = setting.get_engine()
# engine.echo = True
session = sessionmaker(engine)
settings_data = setting.get_vars()

CHUNKSIZE = int(settings_data["chunk_size"])
REPORT_FILE = r"price_report.csv"
LOG_ID = 3



class CalculateClass(QThread):
    SetButtonEnabledSignal = Signal(bool)
    UpdatePriceStatusTableSignal = Signal(str, str, bool)
    ResetPriceStatusTableSignal = Signal(bool)
    SetTotalTome = Signal(bool)
    SetProgressBarValue = Signal(int, int)
    TotalCountSignal = Signal(int)
    UpdatePriceReportTableSignal = Signal(bool)

    isPause = None
    total_file_count = 0
    cur_file_count = 0

    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)


    def run(self):
        global session, engine
        wait_sec = 30
        self.SetButtonEnabledSignal.emit(False)
        while not self.isPause:
            start_cycle_time = datetime.datetime.now()
            try:
                # print(os.listdir(settings_data['exit_1_dir']))
                files = []
                with session() as sess:
                    for file in os.listdir(settings_data['exit_1_dir']):
                        price_code = '.'.join(file.split('.')[:-1])

                        req = select(SupplierPriceSettings.price_code).where(and_(SupplierPriceSettings.price_code == price_code,
                                                                                  SupplierPriceSettings.calculate == 'ДА'))
                        res = sess.execute(req).scalar()
                        if not res:
                            sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code == price_code).delete()
                            continue

                        req = select(PriceReport.price_code).where(and_(PriceReport.price_code == price_code,
                                                                        PriceReport.updated_at != None,
                                                            or_(PriceReport.updated_at_2_step == None,
                                                            PriceReport.updated_at_2_step < PriceReport.updated_at)))
                        res = sess.execute(req).scalar()

                        req = select(Data07.update_time).where(Data07.setting == price_code)
                        update_time = sess.execute(req).scalar()
                        if not update_time:
                            # print(price_code, 'Нет настроек')
                            sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).
                                         values(info_message2='Нет настроек или срока обновления в 07Данные',
                                                updated_at_2_step=datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))
                            sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code == price_code).delete()
                            continue

                        if res:
                            time_edit = datetime.datetime.fromtimestamp(os.path.getmtime(fr"{settings_data['exit_1_dir']}/{file}"))
                            if (datetime.datetime.now() - time_edit).days - update_time >= 30:
                                sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).
                                             values(info_message2='Не подходит период обновления',
                                                    updated_at_2_step=datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))
                                sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code == price_code).delete()
                                continue
                            # print(file, (datetime.datetime.now() - time_edit).days, (datetime.datetime.now() - time_edit).days - update_time,
                            #       (datetime.datetime.now() - time_edit).days - update_time < 30)
                            # self.calculate_file(file)
                            files.append(file)

                    # проверка неактуальных прайсов
                    loaded_prices = set(sess.execute(select(distinct(TotalPrice_2._07supplier_code))).scalars().all())
                    actual_prices = set(sess.execute(select(SupplierPriceSettings.price_code).where(SupplierPriceSettings.calculate == 'ДА')).scalars().all())
                    useless_prices = (loaded_prices - actual_prices)
                    # print(useless_prices)
                    sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code.in_(useless_prices)).delete()

                    sess.commit()

                    # print(files)
                    # files = ['1IMP.csv']
                    if files:
                        self.total_file_count = len(files)
                        self.cur_file_count = 0
                        self.SetTotalTome.emit(True)
                        self.log.add(LOG_ID, f"Начало обработки")
                        cur_time = datetime.datetime.now()

                        for f in files:
                            self.calculate_file(f)

                        self.log.add(LOG_ID, f"Обработка закончена [{str(datetime.datetime.now() - cur_time)[:7]}]")
                        self.SetTotalTome.emit(False)
                        self.UpdatePriceReportTableSignal.emit(True)
                # self.total_file_count = 1
                # self.cur_file_count = 0
                # self.calculate_file('1APR')
                # self.UpdatePriceStatusTableSignal.emit('1APR', '2 step', False)
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
                self.ResetPriceStatusTableSignal.emit(True)
                self.SetTotalTome.emit(False)

            # проверка на паузу
            finish_cycle_time = datetime.datetime.now()
            if wait_sec > (finish_cycle_time - start_cycle_time).seconds:
                for _ in range(wait_sec - (finish_cycle_time - start_cycle_time).seconds):
                    if self.isPause:
                        break
                    time.sleep(1)
        else:
            self.log.add(LOG_ID, "Пауза", f"<span style='color:{colors.orange_log_color};'>Пауза</span>  ")
            self.SetButtonEnabledSignal.emit(True)

    def calculate_file(self, file):
        start_time = datetime.datetime.now()
        try:
            price_code = '.'.join(file.split('.')[:-1])
            self.color = [random.randrange(0, 360), random.randrange(55, 100), 90]

            with session() as sess:

                self.UpdatePriceStatusTableSignal.emit(price_code, 'Загрузка, удаление по первому условию, удаление дублей ...', True)
                cur_time = datetime.datetime.now()

                sess.query(Price_2).delete()

                sess.execute(text(f"ALTER SEQUENCE {Price_2.__tablename__}_id_seq restart 1"))
                # перенос данных из total
                # sess.query(TotalPrice_1).where(TotalPrice_1._07supplier_code == price_code).delete()

                cols_for_price = [TotalPrice_1.key1_s, TotalPrice_1.article_s, TotalPrice_1.brand_s, TotalPrice_1.name_s,
                                  TotalPrice_1.count_s, TotalPrice_1.price_s, TotalPrice_1.currency_s, TotalPrice_1.mult_s,
                                  TotalPrice_1.notice_s, TotalPrice_1._01article, TotalPrice_1._02brand,
                                  TotalPrice_1._14brand_filled_in, TotalPrice_1._03name, TotalPrice_1._04count,
                                  TotalPrice_1._05price, TotalPrice_1._06mult,
                                  TotalPrice_1._15code_optt, TotalPrice_1._07supplier_code, TotalPrice_1._20exclude,
                                  TotalPrice_1._13grad, TotalPrice_1._17code_unique, TotalPrice_1._18short_name]
                cols_for_price = {i: i.__dict__['name'] for i in cols_for_price}
                price = select(*cols_for_price.keys()).where(TotalPrice_1._07supplier_code == price_code)
                sess.execute(insert(Price_2).from_select(cols_for_price.values(), price))
                # sess.execute(update(Price_2).values(_14brand_filled_in_low=Price_2._14brand_filled_in))

                # Удаление по первому условию
                del_positions_1 = sess.query(Price_2).where(or_(Price_2._04count < 1, Price_2._04count == None, Price_2._05price <= 0,
                                                  Price_2._05price == None, Price_2._14brand_filled_in == None, Price_2._01article == None,
                                                  Price_2._20exclude != None)).delete()
                # print('DEL (1):', del_positions_1)
                # if del_positions_1:
                #     self.add_log(price_code, f"Удалено по первому условию: {del_positions_1}")

                # Удаление дублей 01Артикул, 14Производитель заполнен
                del_positions_2 = self.del_duples(sess, price_code)
                del_total = del_positions_1 + del_positions_2
                # self.add_log(price_code, f"Загрузка, удаление по первому условию ({del_positions_1}), удаление дублей ({del_positions_2})", cur_time)
                update_step_time = str(datetime.datetime.now() - cur_time)[:7]
                self.log.add(LOG_ID, f"Загрузка, удаление по первому условию ({del_positions_1}), удаление дублей ({del_positions_2}) [{update_step_time}]",
                             f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>{price_code}</span> "
                             f"Загрузка, удаление по первому условию (<span style='color:{colors.orange_log_color + ';font-weight:bold' if del_positions_1 else 'black'};'>{del_positions_1}</span>), "
                             f"удаление дублей (<span style='color:{colors.orange_log_color + ';font-weight:bold' if del_positions_2 else 'black'};'>{del_positions_2}</span>) [{update_step_time}]")
                             #                         time_spent=, font-weight:bold;
                             # log_text.format(
                             #     f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>",
                             #     '</span>',
                             #     price=price_code, log_main_text=msg,
                             #     time_spent=str(datetime.datetime.now() - cur_time)[:7]))

                self.UpdatePriceStatusTableSignal.emit(price_code, 'data 07, 09 ...', False)
                cur_time = datetime.datetime.now()

                data7_set = sess.execute(select(Data07).where(Data07.setting == price_code)).scalar()
                # print(data7_set.setting, data7_set.markup_os, data7_set.delay)
                sess.execute(update(Price_2).values(delay=data7_set.delay, sell_for_OS=data7_set.sell_os,
                    markup_os=data7_set.markup_os, max_decline=data7_set.max_decline, markup_holidays=data7_set.markup_holidays,
                    markup_R=data7_set.markup_R, min_markup=data7_set.min_markup, min_wholesale_markup=data7_set.min_wholesale_markup,
                    markup_wh_goods=data7_set.markup_wholesale, grad_step=data7_set.grad_step, wh_step=data7_set.wholesale_step,
                    access_pp=data7_set.access_pp, unload_percent=data7_set.unload_percent))

                sess.execute(update(Price_2).where(Price_2._09code_supl_goods==Data09.code_09).
                             values(put_away_zp=Data09.put_away_zp))
                self.add_log(price_code, 'data 07, 09', cur_time)


                self.UpdatePriceStatusTableSignal.emit(price_code, 'Базовая цена, Предложений в опте ...', False)
                cur_time = datetime.datetime.now()

                sess.execute(update(Price_2).where(and_(Price_2._01article == BasePrice.article,
                                                        Price_2._14brand_filled_in == BasePrice.brand))
                             .values(price_b=BasePrice.price_b, min_price=BasePrice.min_price, min_supplier=BasePrice.min_supplier))
                sess.execute(update(Price_2).where(and_(Price_2._01article == MassOffers.article,
                                                        Price_2._14brand_filled_in == MassOffers.brand))
                             .values(offers_wh=MassOffers.offers_count))

                self.add_log(price_code, 'Базовая цена, Предложений в опте', cur_time)


                self.UpdatePriceStatusTableSignal.emit(price_code, 'data 07&14 ...', False)
                cur_time = datetime.datetime.now()

                sess.execute(update(Price_2).where(and_(Price_2._07supplier_code == Data07_14.setting,
                                                        Price_2._14brand_filled_in == Data07_14.correct))
                             .values(markup_pb=Data07_14.markup_pb, code_pb_p=Data07_14.code_pb_p))

                self.add_log(price_code, 'data 07&14', cur_time)


                self.UpdatePriceStatusTableSignal.emit(price_code, '06Кратность, 05Цена плюс, data 15 ...', False)
                cur_time = datetime.datetime.now()

                sess.execute(update(Price_2).where(Price_2.price_b != None).values(low_price=Price_2._05price/Price_2.price_b))
                sess.execute(update(Price_2).where(or_(Price_2.markup_holidays == None, Price_2.markup_holidays == 0))
                             .values(_06mult_new=Price_2._06mult))
                sess.execute(update(Price_2).where(and_(Price_2._06mult_new == None,
                                                        Price_2.markup_holidays > Price_2._05price * Price_2._04count))
                             .values(_06mult_new=Price_2._04count))
                sess.execute(update(Price_2).where(Price_2._06mult_new == None)
                             .values(_06mult_new=func.ceil(func.greatest(Price_2._06mult, Price_2.markup_holidays / Price_2._05price) /
                                                           Price_2._06mult) * Price_2._06mult))

                sess.execute(update(Price_2).where(Price_2.markup_holidays > Price_2._05price * Price_2._04count).values(_05price_plus=Price_2._05price))
                sess.execute(update(Price_2).where(Price_2._05price_plus == None).values(_05price_plus=Price_2._05price))

                sess.execute(update(Price_2).where(Price_2._15code_optt==Buy_for_OS.article_producer).values(buy_count=Buy_for_OS.buy_count))

                sess.execute(update(Price_2).values(count=Price_2._04count))

                self.add_log(price_code, '06Кратность, 05Цена плюс, data 15', cur_time)

                cols_for_total = [Price_2.key1_s, Price_2.article_s, Price_2.brand_s, Price_2.name_s,
                                  Price_2.count_s, Price_2.price_s, Price_2.currency_s, Price_2.mult_s,
                                  Price_2.notice_s,
                                  Price_2._01article, Price_2._02brand, Price_2._03name, Price_2._04count,
                                  Price_2._05price, Price_2._06mult, Price_2._07supplier_code,
                                  Price_2._09code_supl_goods,
                                  Price_2._10original, Price_2._13grad, Price_2._14brand_filled_in,
                                  Price_2._15code_optt,
                                  Price_2._17code_unique, Price_2._18short_name, Price_2._19min_price,
                                  Price_2._20exclude,
                                  Price_2.delay, Price_2.sell_for_OS, Price_2.markup_os, Price_2.max_decline,
                                  Price_2.markup_holidays, Price_2.markup_R, Price_2.min_markup,
                                  Price_2.min_wholesale_markup, Price_2.markup_wh_goods,
                                  Price_2.grad_step, Price_2.wh_step, Price_2.access_pp, Price_2.unload_percent,
                                  Price_2.put_away_zp, Price_2.offers_wh, Price_2.price_b, Price_2.low_price,
                                  Price_2.count, Price_2.markup_pb, Price_2.code_pb_p, Price_2._06mult_new,
                                  Price_2.mult_less, Price_2._05price_plus, Price_2.reserve_count, Price_2.buy_count,
                                  Price_2.min_price, Price_2.min_supplier,
                                  ]

                # формирование csv
                if not self.create_csv(sess, price_code, start_time):
                    return

                # перенос данных в total
                sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code == price_code).delete()

                cols_for_total = {i: i.__dict__['name'] for i in cols_for_total}
                total = select(*cols_for_total.keys())
                sess.execute(insert(TotalPrice_2).from_select(cols_for_total.values(), total))

                # self.UpdatePriceStatusTableSignal.emit(price_code, 'step 2', False)
                # cur_time = datetime.datetime.now()
                # time.sleep(2)
                # self.add_log(price_code, 'step 2', cur_time)

                cnt = sess.execute(select(func.count()).select_from(Price_2)).scalar()
                sess.query(Price_2).delete()
                # cnt_wo_article = sess.execute(select(func.count()).select_from(Price_1).where(Price_1._01article == None)).scalar()
                sess.execute(update(PriceReport).where(PriceReport.price_code == price_code)
                             .values(info_message2="Ок", updated_at_2_step=start_time.strftime("%Y.%m.%d %H:%M:%S"), row_count_2=cnt,
                                     del_pos=del_total))
                total_cnt = sess.execute(select(func.count()).select_from(TotalPrice_2)).scalar()
                sess.commit()
                self.TotalCountSignal.emit(total_cnt)

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

    def del_duples(self, sess, price_code):
        duples = sess.execute(select(Price_2._01article, Price_2._14brand_filled_in).
                              group_by(Price_2._01article, Price_2._14brand_filled_in).having(
            func.count(Price_2.id) > 1))

        for art, brnd in duples:
            # print(art, brnd)
            # DEL для всех повторений
            sess.execute(
                update(Price_2).where(and_(Price_2._01article == art, Price_2._14brand_filled_in == brnd)).values(
                    _20exclude='DEL'))
            # Устанавливается 'not DEL' в каждой группе повторения, если цена в группе минимальная
            min_price = select(func.min(Price_2._05price)).where(
                and_(Price_2._01article == art, Price_2._14brand_filled_in == brnd))
            sess.execute((update(Price_2)).where(and_(Price_2._20exclude == 'DEL', Price_2._01article == art,
                                                      Price_2._14brand_filled_in == brnd,
                                                      Price_2._05price == min_price))
                         .values(_20exclude='not DEL'))
            # Среди записей с 'not DEL' ищутся записи не с максимальным кол-вом и на них устанавливается DEL
            max_count = select(func.max(Price_2._04count)).where(
                and_(Price_2._01article == art, Price_2._14brand_filled_in == brnd,
                     Price_2._20exclude == 'not DEL'))
            sess.execute(update(Price_2).where(and_(Price_2._20exclude == 'not DEL', Price_2._01article == art,
                                                    Price_2._14brand_filled_in == brnd, Price_2._04count != max_count))
                         .values(_20exclude='DEL'))
            # В оставшихся группах, где совпадает мин. цена и макс. кол-вл, остаются лишь записи с максимальным id
            max_id = select(func.max(Price_2.id)).where(
                and_(Price_2._01article == art, Price_2._14brand_filled_in == brnd,
                     Price_2._20exclude == 'not DEL'))
            sess.execute(update(Price_2).where(and_(Price_2._01article == art, Price_2._14brand_filled_in == brnd,
                                                    Price_2.id != max_id)).values(_20exclude='DEL'))

        del_positions_2 = sess.query(Price_2).where(Price_2._20exclude == 'DEL').delete()
        # if del_positions_2:
        #     self.add_log(price_code, f"Удалено дблей: {del_positions_2}")
        sess.execute(update(Price_2).values(_20exclude=None))
        return del_positions_2

    def create_csv(self, sess, price_code, start_time):
        self.UpdatePriceStatusTableSignal.emit(price_code, 'Формирование csv ...', False)
        cur_time = datetime.datetime.now()

        try:
            df = pd.DataFrame(columns=["Ключ1 поставщика", "Артикул поставщика", "Производитель поставщика",
                                           "Наименование поставщика",
                                           "Количество поставщика", "Цена поставщика", "Кратность поставщика",
                                           "Примечание поставщика", "01Артикул", "02Производитель",
                                           "03Наименование", "05Цена", "06Кратность-", "07Код поставщика",
                                           "09Код + Поставщик + Товар", "10Оригинал",
                                           "13Градация", "14Производитель заполнен", "15КодТутОптТорг",
                                           "17КодУникальности", "18КороткоеНаименование",
                                           "19МинЦенаПоПрайсу", "20ИслючитьИзПрайса", "Отсрочка", "Продаём для ОС",
                                           "Наценка для ОС", "Наценка Р",
                                           "Наценка ПБ", "Мин наценка", "Наценка на оптовые товары", "Шаг градаци",
                                           "Шаг опт", "Разрешения ПП", "УбратьЗП", "Предложений опт",
                                           "ЦенаБ", "Кол-во", "Код ПБ_П", "06Кратность", "Кратность меньше", "05Цена+",
                                           "Количество закупок", "% Отгрузки",
                                           "Мин. Цена", "Мин. Поставщик"])
            df.to_csv(fr"{settings_data['exit_2_dir']}/{price_code}.csv", sep=';', decimal=',',
                      encoding="windows-1251", index=False, errors='ignore')

            limit = CHUNKSIZE
            loaded = 0
            while True:
                req = select(Price_2.key1_s, Price_2.article_s, Price_2.brand_s, Price_2.name_s,
                                  Price_2.count_s, Price_2.price_s, Price_2.mult_s, Price_2.notice_s,
                                  Price_2._01article, Price_2._02brand, Price_2._03name,
                                  Price_2._05price, Price_2._06mult, Price_2._07supplier_code, Price_2._09code_supl_goods,
                                  Price_2._10original, Price_2._13grad, Price_2._14brand_filled_in, Price_2._15code_optt,
                                  Price_2._17code_unique, Price_2._18short_name, Price_2._19min_price, Price_2._20exclude,
                                  Price_2.delay, Price_2.sell_for_OS, Price_2.markup_os, Price_2.markup_R,
                                  Price_2.markup_pb, Price_2.min_markup, Price_2.markup_wh_goods,
                                  Price_2.grad_step, Price_2.wh_step,  Price_2.access_pp, Price_2.put_away_zp,
                                  Price_2.offers_wh, Price_2.price_b, Price_2.count, Price_2.code_pb_p,
                                  Price_2._06mult_new, Price_2.mult_less, Price_2._05price_plus,
                                  Price_2.buy_count, Price_2.unload_percent, Price_2.min_price, Price_2.min_supplier)\
                    .order_by(Price_2.id).offset(loaded).limit(limit)
                df = pd.read_sql_query(req, sess.connection(), index_col=None)
                df_len = len(df)

                if not df_len:
                    break
                df.to_csv(fr"{settings_data['exit_2_dir']}/{price_code}.csv", mode='a',
                          sep=';', decimal=',', encoding="windows-1251", index=False, header=False, errors='ignore')
                loaded += df_len
                # print(df)

            self.add_log(price_code, 'csv сформирован', cur_time)

            # cnt = sess.execute(select(func.count()).select_from(Price_1)).scalar()
            # cnt_wo_article = sess.execute(
            #     select(func.count()).select_from(Price_1).where(Price_1._01article == None)).scalar()
            return True
        except PermissionError:
            load_time = str(datetime.datetime.now() - start_time)[:7]
            self.log.add(LOG_ID,
                         f"Не удалось сформировать прайс {price_code} ({self.cur_file_count + 1}/{self.total_file_count}) [{load_time}]",
                         f"Не удалось сформировать прайс <span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{price_code}</span> ({self.cur_file_count + 1}/{self.total_file_count}) [{load_time}]")
            return False

    def add_log(self, price_code, msg, cur_time=None, new_row=False):
        # лог с выводом этапа в таблицу
        if cur_time:
            log_text = "{}{price}{} {log_main_text} [{time_spent}]"
            self.log.add(LOG_ID, log_text.format('', '', price=price_code, log_main_text=msg,
                                                                time_spent=str(datetime.datetime.now() - cur_time)[:7]),
                         log_text.format(f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>",
                                         '</span>',
                                         price=price_code, log_main_text=msg,
                                         time_spent=str(datetime.datetime.now() - cur_time)[:7]))

            self.UpdatePriceStatusTableSignal.emit(price_code, msg, new_row)
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