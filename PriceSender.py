import time
from PySide6.QtCore import QThread, Signal
from sqlalchemy import text, select, delete, insert, update, and_, not_, func, cast, distinct, or_, inspect, REAL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
from models import (TotalPrice_2, FinalPrice, FinalComparePrice, Base3, SaleDK, BuyersForm, Data07, PriceException,
                    SuppliersForm, Brands_3, PriceSendTime, FinalPriceHistory, FinalPriceInfo)
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.header import Header
import pandas as pd
import traceback
import datetime
import re
import os
import shutil
import random

import colors
import setting
engine = setting.get_engine()
session = sessionmaker(engine)
settings_data = setting.get_vars()
CHUNKSIZE = int(settings_data["chunk_size"])

LOG_ID = 4
WEEKDAYS = {0: "пон", 1: "втор", 2: "сре", 3: "чет", 4: "пят", 5: "суб", 6: "вос"}
REPORT_FILE = r"final_price_report.csv"

# из "Анкета покупателя" взять прайсы (Имя прайса) если "Включен?" == "да"
# сначала формировать прайсы, где "Срок" == 1
# сопоставить "Наименование" в Справочник_Бренд3
# "Куда запрещено" "Код покупателя" (%PLG%) and "Примечание" == "Действует в экселе"
#

class Sender(QThread):
    UpdateReportSignal = Signal(bool)
    SetButtonEnabledSignal = Signal(bool)
    StartCreationSignal = Signal(bool)
    SetProgressBarValue = Signal(int, int)
    isPause = None
    need_to_send = True
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)
    def run(self):
        global session, engine
        self.SetButtonEnabledSignal.emit(False)
        wait_sec = 15

        while not self.isPause:
            start_cycle_time = datetime.datetime.now()
            try:

                # СНАЧАЛА С МИН СРОКОМ
                # в бета версии сравнивать с временем изменения прайса, далее отталкиваться от времени отправки в справочнике
                # name = "Прайс Профит-Лига"
                # name = "2 Прайс Профит-Лига"
                # name = "Прайс ABS"
                # name = "2дня Прайс 2-ABS"
                # name = "3дня Прайс ABS"
                price_name_list = []
                with session() as sess:
                    prices = sess.execute(select(BuyersForm).where(func.upper(BuyersForm.included)=='ДА').order_by(BuyersForm.period)).scalars().all()
                    for p in prices:
                        send_times = [p.time1, p.time2, p.time3, p.time4, p.time5, p.time6]
                        last_send = sess.execute(select(PriceSendTime.send_time).where(PriceSendTime.price_code==p.buyer_price_code)).scalar()
                        for st in send_times:
                            if st is not None:
                                try:
                                    t_now = datetime.datetime.now()
                                    h, m = map(int, str(st).split(':')[:2])
                                    t = t_now.replace(hour=h, minute=m)
                                    # print(p.buyer_price_code, t)

                                    if t_now > t and (not last_send or last_send < t):
                                        price_name_list.append(p.price_name)
                                        break
                                except:
                                    pass

                # print(price_name_list)
                    # print(prices)
                self.cur_file_count = 0
                self.total_file_count = len(price_name_list)

                # return
                # price_name_list = ["2 Прайс АвтоПитер", ]
                if price_name_list:
                    self.StartCreationSignal.emit(True)
                    start_creating = datetime.datetime.now()
                    self.log.add(LOG_ID, f"Начало формирования ...")
                    for name in price_name_list:
                        try:
                            self.create_price(name)
                            # print('ok')
                            # return
                        except Exception as create_ex:
                            ex_text = traceback.format_exc()
                            self.log.error(LOG_ID, "create_ex ERROR:", ex_text)
                        finally:
                            self.cur_file_count += 1
                            self.SetProgressBarValue.emit(self.cur_file_count, self.total_file_count)
                    self.log.add(LOG_ID, f"Формирование закончено [{str(datetime.datetime.now() - start_creating)[:7]}]")
                    self.StartCreationSignal.emit(False)
                    self.UpdateReportSignal.emit(True)

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
                self.log.error(LOG_ID, "ERROR:", ex_text)

            finish_cycle_time = datetime.datetime.now()
            if wait_sec > (finish_cycle_time - start_cycle_time).seconds:
                for _ in range(wait_sec - (finish_cycle_time - start_cycle_time).seconds):
                    if self.isPause:
                        break
                    time.sleep(1)
        else:
            self.log.add(LOG_ID, f"Пауза", f"<span style='color:{colors.orange_log_color};'>Пауза</span>  ")
            self.SetButtonEnabledSignal.emit(True)


    def create_price(self, name):
        start_time = datetime.datetime.now()
        cur_time = datetime.datetime.now()
        self.color = [random.randrange(0, 360), random.randrange(55, 100), 90]

        inspct = inspect(engine)
        if inspct.has_table(FinalPrice.__tablename__):
            FinalPrice.__table__.drop(engine)
        if inspct.has_table(FinalComparePrice.__tablename__):
            FinalComparePrice.__table__.drop(engine)
        # if inspct.has_table(FinalPriceDuplDel.__tablename__):
        #     FinalPriceDuplDel.__table__.drop(engine)

        Base3.metadata.create_all(engine)

        with session() as sess:
            sess.execute(text(f"ALTER TABLE {FinalPrice.__tablename__} SET (autovacuum_enabled = false);"))
            sess.execute(text(f"ALTER TABLE {FinalComparePrice.__tablename__} SET (autovacuum_enabled = false);"))
            # sess.execute(text(f"ALTER TABLE {FinalPriceDuplDel.__tablename__} SET (autovacuum_enabled = false);"))
            sess.commit()
            self.price_settings = sess.execute(select(BuyersForm).where(BuyersForm.price_name == name)).scalar()
            self.add_log(self.price_settings.buyer_price_code,f" ...")

            allow_brands = sess.execute(
                select(Brands_3.correct, Brands_3.short_name).where(Brands_3.zp_brands_setting == self.price_settings.zp_brands_setting)).all()
            if not allow_brands:
                self.add_log(self.price_settings.buyer_price_code,f"Не указаны бренды в Справочник_Бренд3")
                sess.query(PriceSendTime).where(
                    PriceSendTime.price_code == self.price_settings.buyer_price_code).delete()
                sess.add(PriceSendTime(price_code=self.price_settings.buyer_price_code,
                                  send_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                  info_msg='Не указаны бренды в Справочник_Бренд3'))
                sess.commit()
                return

            allow_prices = self.get_allow_prises(sess)

            cols_for_price = [TotalPrice_2.article_s, TotalPrice_2.brand_s, TotalPrice_2._01article,
                              TotalPrice_2._03name, TotalPrice_2._05price, TotalPrice_2._05price_plus,
                              TotalPrice_2._06mult_new, TotalPrice_2._07supplier_code, TotalPrice_2._13grad,
                              TotalPrice_2._14brand_filled_in, TotalPrice_2._15code_optt, TotalPrice_2._17code_unique,
                              TotalPrice_2._18short_name, TotalPrice_2.delay, TotalPrice_2.sell_for_OS,
                              TotalPrice_2.markup_os, TotalPrice_2.markup_R, TotalPrice_2.markup_pb,
                              TotalPrice_2.min_markup, TotalPrice_2.min_wholesale_markup, TotalPrice_2.grad_step,
                              TotalPrice_2.wh_step, TotalPrice_2.access_pp, TotalPrice_2.put_away_zp,
                              TotalPrice_2.offers_wh, TotalPrice_2.price_b, TotalPrice_2.count,
                              TotalPrice_2.code_pb_p, TotalPrice_2.mult_less, TotalPrice_2.buy_count,
                              TotalPrice_2.unload_percent, TotalPrice_2.min_price, TotalPrice_2.to_price]
            cols_for_price = {i: i.__dict__['name'] for i in cols_for_price}
            price = select(*cols_for_price.keys()).where(and_(TotalPrice_2.to_price == self.price_settings.period,
                                                              TotalPrice_2._07supplier_code.in_(allow_prices)))
            sess.execute(insert(FinalPrice).from_select(cols_for_price.values(), price))

            # self.add_log(self.price_settings.buyer_price_code, f"Загружено", cur_time)

            sess.commit()

            # cur_time = datetime.datetime.now()
            sess.query(FinalPrice).where(and_(FinalPrice.put_away_zp!=None,
                FinalPrice.put_away_zp.notlike(f"%{self.price_settings.zp_brands_setting}%"))).delete()

            # self.add_log(self.price_settings.buyer_price_code, f"Удалено", cur_time)

            # cur_time = datetime.datetime.now()
            self.delete_exceptions(sess)
            count_after_first_filter = sess.execute(func.count(FinalPrice.id)).scalar()
            # self.add_log(self.price_settings.buyer_price_code, f"Слова иск, тотал: {count_after_first_filter}", cur_time)
            self.add_log(self.price_settings.buyer_price_code, f"Кол-во строк после первого фильтра: {count_after_first_filter}",
                         cur_time)

            # sess.commit()
            # print('ok')
            # return

            cur_time = datetime.datetime.now()

            self.update_count_and_short_name(sess, allow_brands)

            self.update_price(sess)

            self.add_log(self.price_settings.buyer_price_code, f"Расчёт цены и количества", cur_time)

            cur_time = datetime.datetime.now()
            self.del_duples(sess)
            self.add_log(self.price_settings.buyer_price_code, f"Удаление дублей", cur_time)

            cur_time = datetime.datetime.now()
            sess.execute(update(FinalPrice).where(and_(FinalPrice.price_b != None,
                                                       FinalPrice.price_b / self.price_settings.kb_price < FinalPrice.price)).
                         values(over_base_price=True))
            del_price_b = sess.query(FinalPrice).where(FinalPrice.over_base_price == True).delete()
            if del_price_b:
                self.add_log(self.price_settings.buyer_price_code, f"Удалено: {del_price_b} (ЦенаБ)")

            self.del_over_price(sess)

            self.set_rating(sess)
            self.add_log(self.price_settings.buyer_price_code, f"Расчёт рейтинга, удаление ЦенаБ", cur_time)

            cur_time = datetime.datetime.now()
            self.file_name = f"{str(self.price_settings.file_name).replace('.xlsx', '')}.csv"
            self.create_csv(sess)
            self.add_log(self.price_settings.buyer_price_code, f"csv создан", cur_time)

            total_rows = sess.execute(func.count(FinalPrice.id)).scalar()
            self.add_log(self.price_settings.buyer_price_code, f"Итоговое кол-во строк: {total_rows}")

            self.send_time = sess.execute(select(PriceSendTime.send_time).where(PriceSendTime.price_code==self.price_settings.buyer_price_code)).scalar()
            if self.need_to_send:
                self.send_mail(sess)

                info_row = FinalPriceInfo(price_code=self.price_settings.buyer_price_code, send_time=self.send_time)
                sess.add(info_row)
                sess.flush()

                cols_for_price = [FinalPrice.article_s, FinalPrice.brand_s, FinalPrice._01article,
                                  FinalPrice._03name, FinalPrice._05price, FinalPrice._05price_plus,
                                  FinalPrice._06mult_new, FinalPrice._07supplier_code, FinalPrice._14brand_filled_in,
                                  FinalPrice._15code_optt, FinalPrice._17code_unique,
                                  FinalPrice.count, FinalPrice.price]
                cols_for_price = {i: i.__dict__['name'] for i in cols_for_price}
                price = select(info_row.id, *cols_for_price.keys())
                sess.execute(insert(FinalPriceHistory).from_select(['info_id', *cols_for_price.values()], price))

            sess.query(PriceSendTime).where(PriceSendTime.price_code==self.price_settings.buyer_price_code).delete()
            sess.add(PriceSendTime(price_code=self.price_settings.buyer_price_code,
                                   update_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), send_time=self.send_time,
                                   info_msg='Ок', count=total_rows, count_after_filter=count_after_first_filter,
                                   del_price_b=del_price_b, exception_words_del=self.exception_words_del,
                                   count_mult_del=self.count_mult_del, correct_brands_del=self.correct_brands_del,
                                   price_del=self.price_del, dup_del=self.dup_del, price_compare_del=self.price_compare_del))

            sess.commit()
            total_price_calc_time = str(datetime.datetime.now() - start_time)[:7]
            self.log.add(LOG_ID,
                         f"+ {self.price_settings.buyer_price_code} готов! ({self.cur_file_count + 1}/{self.total_file_count}) [{total_price_calc_time}]",
                         f"<span style='color:{colors.green_log_color};font-weight:bold;'>✔</span> "
                         f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{self.price_settings.buyer_price_code}</span> готов! ({self.cur_file_count + 1}/{self.total_file_count}) [{total_price_calc_time}]")

        FinalPrice.__table__.drop(engine)
        FinalComparePrice.__table__.drop(engine)
        # FinalPriceDuplDel.__table__.drop(engine)


    def get_allow_prises(self, sess):
        allow_prices = set(sess.execute(select(Data07.setting).where(
            Data07.access_pp.like(f"%{self.price_settings.buyer_price_code}%"))).scalars().all())
        # print(allow_prices)
        weekday = WEEKDAYS[datetime.datetime.now().weekday()]
        # weekday = 'сре'

        allow_prices_wd_settings = sess.execute(select(SuppliersForm.setting, SuppliersForm.days).
                                                where(SuppliersForm.days.like(f"%{weekday}%"))).all()

        allow_prices_wd = set()
        for price in allow_prices_wd_settings:
            # if price.setting in ['AVT0', 'CCXT']:
            # print(price.setting, price.days)
            price_time = re.search(weekday + ' \d{1,2}:\d{2}', str(price.days))
            if price_time:
                price_time = price_time.group()
                price_time = price_time.strip(weekday).strip()
                h, m = map(int, price_time.split(':'))
                price_time = datetime.time(hour=h, minute=m)
                cur_time = datetime.datetime.now().time()
                # print(cur_time, price_time)
                if price_time < cur_time:
                    continue
            allow_prices_wd.add(price.setting)
            # print(price_time)
        allow_prices = allow_prices & allow_prices_wd
        # print(allow_prices)
        return select(Data07.setting).where(Data07.setting.in_(allow_prices))

    def delete_exceptions(self, sess):
        cols = {"03Наименование": FinalPrice._03name, "15КодТутОптТорг": FinalPrice._15code_optt, }
        conditions = {"Начинается с": lambda col, x: col.ilike(f"{x}%"),  # '^{}'], startswith
                      "Содержит": lambda col, x: col.ilike(f"%{x}%"),  # '{}'],
                      "Не содержит": lambda col, x: not_(col.contains(x)),  # '{}'],
                      "Заканчивается на": lambda col, x: col.ilike(f"%{x}"),  # '{}$'], endswith
                      "Равно": lambda col, x: func.upper(col) == x,
                      # "Не равно": lambda col, x: func.upper(col) != x,
                      }
        exceptions = sess.execute(
            select(PriceException).where(and_(PriceException.deny.like(f"%{self.price_settings.buyer_code}"),
                                              PriceException.extra == "Действует в экселе"))).scalars().all()

        for e in exceptions:
            # print(e.text, e.condition)
            condition = conditions.get(e.condition, None)
            col = cols.get(e.find, None)
            if condition and col:
                sess.execute(update(FinalPrice).where(condition(col, e.text)).values(mult_less='0'))

        self.exception_words_del = sess.query(FinalPrice).where(FinalPrice.mult_less != None).delete()
        if self.exception_words_del:
            self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.exception_words_del} (Слова исключения)")

    def update_count_and_short_name(self, sess, allow_brands):
        self.count_mult_del = 0
        # расчёт кол-ва
        if self.price_settings.us_above is not None:
            sess.execute(update(FinalPrice).where(and_(FinalPrice.unload_percent != 1,
                                                       FinalPrice.unload_percent < self.price_settings.us_above)).
                         values(count=func.floor(FinalPrice.count * self.price_settings.us_above)))
            self.count_mult_del = sess.query(FinalPrice).where(or_(FinalPrice.count < 1, FinalPrice._06mult_new > FinalPrice.count)).delete()
            if self.count_mult_del:
                self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.count_mult_del} (Кол-во или кратность)")

        allow_brands_set = set(b.correct for b in allow_brands)
        # print(allow_brands_set)
        self.correct_brands_del = sess.query(FinalPrice).where(FinalPrice._14brand_filled_in.not_in(allow_brands_set)).delete()
        if self.correct_brands_del:
            self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.correct_brands_del} (Правильные бренды)")

        short_name = set()
        for b in allow_brands:
            if str(b.short_name).upper() == 'ДА':
                short_name.add(b.correct)

        sess.execute(update(FinalPrice).where(FinalPrice._14brand_filled_in.in_(short_name)).
                     values(_03name=FinalPrice._18short_name))

    def update_price(self, sess):
        sess.execute(update(FinalPrice).where(and_(SaleDK.agr == self.price_settings.buyer_code,
                                                   SaleDK.price_code == FinalPrice._07supplier_code,
                                                   SaleDK.val is not None)).
                     values(price=FinalPrice._05price_plus * cast(SaleDK.val, REAL)))
        if str(self.price_settings.sell_for_kos).upper() == 'ДА':
            sess.execute(update(FinalPrice).where(and_(FinalPrice.price == None,
                                                       FinalPrice.sell_for_OS == 'ДА',
                                                       FinalPrice.buy_count == None,
                                                       FinalPrice.delay > self.price_settings.delay,
                                                       FinalPrice.offers_wh != None)).
                         values(price=FinalPrice._05price_plus * (FinalPrice.min_markup - (FinalPrice.markup_os *
                                                                                           (
                                                                                                       FinalPrice.delay - self.price_settings.delay))) * (
                                                  self.price_settings.kos_markup + 1)))

        # БазоваяНаценка, МножительПокупателя
        sess.execute(update(FinalPrice).where(and_(FinalPrice.price == None,
                                                   FinalPrice.offers_wh == None)).
                     values(base_markup=FinalPrice.min_markup + (FinalPrice.grad_step * FinalPrice._13grad),
                            buyer_mult=self.price_settings.final_markup + 1))
        sess.execute(update(FinalPrice).where(and_(FinalPrice.price == None,
                                                   FinalPrice.base_markup == None)).
                     values(base_markup=FinalPrice.min_wholesale_markup + (FinalPrice.wh_step * FinalPrice._13grad),
                            buyer_mult=self.price_settings.markup_buyer_wh + 1))
        # ЦенаСНаценкой
        sess.execute(update(FinalPrice).where(FinalPrice.price == None).
                     values(
            price_with_markup=FinalPrice._05price_plus * FinalPrice.base_markup * FinalPrice.buyer_mult))
        # ЦенаР
        sess.execute(update(FinalPrice).where(FinalPrice.price == None).
                     values(price_r=FinalPrice._05price_plus + FinalPrice.markup_R))
        # ОбычнаяЦена
        sess.execute(update(FinalPrice).where(FinalPrice.price == None).
                     values(
            default_price=func.greatest(FinalPrice.price_r, FinalPrice.price_with_markup) * (FinalPrice.markup_pb + 1)))

        sess.execute(update(FinalPrice).where(and_(FinalPrice.price == None, FinalPrice.min_price != None)).
                     values(price=func.least(FinalPrice.default_price,
                                             func.greatest(FinalPrice.price_r, FinalPrice.price_with_markup,
                                                           FinalPrice.min_price))))
        sess.execute(update(FinalPrice).where(FinalPrice.price == None).values(price=FinalPrice.default_price))

        self.price_del = sess.query(FinalPrice).where(or_(FinalPrice.price<=0, FinalPrice.price==None)).delete()
        if self.price_del:
            self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.price_del} (Цена меньше/равна 0)")


    # def del_duples(self, sess):
    #     duples = sess.execute(select(FinalPrice._15code_optt).group_by(FinalPrice._15code_optt).
    #                           having(func.count(FinalPrice.id) > 1)).scalars().all()
    #     # print('dupl:', len(duples))
    #     del_cnt = 0
    #
    #     for d in duples:
    #         # DEL для всех повторений (mult_less уже не нужен на этом этапе)
    #         sess.execute(update(FinalPrice).where(FinalPrice._15code_optt == d).values(mult_less='D'))
    #         # Устанавливается 'not DEL' в каждой группе повторения, если цена в группе минимальная
    #         min_price = select(func.min(FinalPrice.price)).where(FinalPrice._15code_optt == d)
    #         sess.execute((update(FinalPrice)).where(
    #             and_(FinalPrice.mult_less == 'D', FinalPrice.price == min_price)).values(mult_less='n D'))
    #         # Среди записей с 'not DEL' ищутся записи не с максимальным кол-вом и на них устанавливается DEL
    #         max_count = select(func.max(FinalPrice.count)).where(FinalPrice.mult_less == 'n D')
    #         sess.execute(update(FinalPrice).where(
    #             and_(FinalPrice.mult_less == 'n D', FinalPrice.count != max_count)).values(mult_less='D'))
    #         # В оставшихся группах, где совпадает мин. цена и макс. кол-вл, остаются лишь записи с максимальным id
    #         max_id = select(func.max(FinalPrice.id)).where(FinalPrice.mult_less == 'n D')
    #         sess.execute(update(FinalPrice).where(
    #             and_(FinalPrice._15code_optt == d, FinalPrice.id != max_id)).values(mult_less='D'))
    #
    #         del_cnt += sess.query(FinalPrice).where(FinalPrice.mult_less == 'D').delete()
    #         sess.execute(update(FinalPrice).where(FinalPrice.mult_less == 'n D').values(mult_less=None))
    #
    #     if del_cnt:
    #         self.add_log(self.price_settings.buyer_price_code,
    #                      f"{self.price_settings.buyer_price_code} Удалено: {del_cnt} (Дубли)")


    def del_duples(self, sess):
        # D для всех дублей
        duples = select(FinalPrice._15code_optt).group_by(FinalPrice._15code_optt).having(func.count(FinalPrice.id) > 1)
        sess.execute(update(FinalPrice).where(FinalPrice._15code_optt.in_(duples)).values(mult_less='D'))

        # D1 не с мин. ценой среди D
        min_p_table = (select(FinalPrice._15code_optt, func.min(FinalPrice.price).label('min_p')).
                       where(FinalPrice.mult_less=='D').group_by(FinalPrice._15code_optt))
        sess.execute(update(FinalPrice).where(and_(FinalPrice._15code_optt==min_p_table.c._15code_optt,
                                                   FinalPrice.price==min_p_table.c.min_p)).values(mult_less='D1'))

        # D2 не с макс. кол-вом среди D1
        max_c_table = select(FinalPrice._15code_optt, func.max(FinalPrice.count).label('max_c')).where(FinalPrice.mult_less == 'D1').group_by(FinalPrice._15code_optt)
        sess.execute(update(FinalPrice).where(and_(FinalPrice._15code_optt==max_c_table.c._15code_optt,
                                                   FinalPrice.count==max_c_table.c.max_c)).values(mult_less='D2'))

        # D3 не с макс. id среди D2
        max_id_table = select(FinalPrice._15code_optt, func.max(FinalPrice.id).label('max_id')).where(FinalPrice.mult_less == 'D2').group_by(FinalPrice._15code_optt)
        sess.execute(update(FinalPrice).where(FinalPrice.id==max_id_table.c.max_id).values(mult_less='D3'))

        sess.execute(update(FinalPrice).where(FinalPrice.mult_less=='D3').values(mult_less=None))
        self.dup_del = sess.query(FinalPrice).where(FinalPrice.mult_less != None).delete()

        if self.dup_del:
            self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.dup_del} (Дубли)")

    # def del_duples2(self, sess):
    #     duples = sess.execute(select(FinalPrice._15code_optt).group_by(FinalPrice._15code_optt).
    #                           having(func.count(FinalPrice.id) > 1)).scalars().all()
    #     # print('dupl:', len(duples))
    #     # pd_count = len(duples)
    #     # self.log.add(LOG_ID, f"dp {pd_count}")
    #     self.dup_del = 0
    #
    #     for d in duples:
    #         # ct = datetime.datetime.now()
    #         # DEL для всех повторений (mult_less уже не нужен на этом этапе)
    #         sess.execute(update(FinalPrice).where(FinalPrice._15code_optt == d).values(mult_less='D'))
    #         # Устанавливается 'not DEL' в каждой группе повторения, если цена в группе минимальная
    #         min_price = select(func.min(FinalPrice.price)).where(FinalPrice._15code_optt == d)
    #         sess.execute((update(FinalPrice)).where(
    #             and_(FinalPrice._15code_optt == d, FinalPrice.mult_less == 'D', FinalPrice.price == min_price)).values(mult_less='n D'))
    #         # Среди записей с 'not DEL' ищутся записи не с максимальным кол-вом и на них устанавливается DEL
    #         max_count = select(func.max(FinalPrice.count)).where(and_(FinalPrice._15code_optt == d, FinalPrice.mult_less == 'n D'))
    #         sess.execute(update(FinalPrice).where(
    #             and_(FinalPrice._15code_optt == d, FinalPrice.mult_less == 'n D', FinalPrice.count != max_count)).values(mult_less='D'))
    #         # В оставшихся группах, где совпадает мин. цена и макс. кол-вл, остаются лишь записи с максимальным id
    #         max_id = select(func.max(FinalPrice.id)).where(and_(FinalPrice._15code_optt == d, FinalPrice.mult_less == 'n D'))
    #         sess.execute(update(FinalPrice).where(
    #             and_(FinalPrice._15code_optt == d, FinalPrice.id != max_id)).values(mult_less='D'))
    #         # self.log.add(LOG_ID, f"1) {d} {str(datetime.datetime.now() - ct)[:7]}")
    #         # if i % 3000 == 0:
    #         #     self.log.add(LOG_ID, f"del d {i}/{pd_count}")
    #
    #         # ct = datetime.datetime.now()
    #         self.dup_del += sess.query(FinalPrice).where(and_(FinalPrice._15code_optt == d, FinalPrice.mult_less == 'D')).delete()
    #         sess.execute(update(FinalPrice).where(and_(FinalPrice._15code_optt == d, FinalPrice.mult_less == 'n D')).values(mult_less=None))
    #         # self.log.add(LOG_ID, f"2) {d} {str(datetime.datetime.now() - ct)[:7]}")
    #
    #     if self.dup_del:
    #         self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.dup_del} (Дубли)")

    def del_over_price(self, sess):
        self.price_compare_del = 0
        if self.price_settings.period != 1 and self.price_settings.main_price:
            main_prices = list(map(str.strip, str(self.price_settings.main_price).split(',')))
            # print('MPs', main_prices)
            for p in main_prices:
                base_name = str(p).rstrip('.xlsx')
                price_path = fr"{settings_data['send_dir']}/{base_name}.csv"
                if os.path.exists(price_path):
                    period = sess.execute(select(BuyersForm.period).where(BuyersForm.file_name == p)).scalar()
                    if period is None:
                        continue
                    # print('MP', price_path)
                    main_price = pd.read_csv(price_path, header=0, sep=';', encoding='windows-1251',
                                             usecols=["Артикул", "Бренд", "Цена"], encoding_errors='ignore',
                                             na_filter=False)
                    main_price = main_price.rename(columns={"Артикул": FinalComparePrice._01article.__dict__['name'],
                                                            "Бренд": FinalComparePrice._14brand_filled_in.__dict__['name'],
                                                            "Цена": FinalComparePrice.price.__dict__['name']})
                    main_price['period'] = period
                    main_price['price'] = main_price['price'].str.replace(',', '.')
                    main_price.to_sql(name=FinalComparePrice.__tablename__, con=sess.connection(), if_exists='append',
                                      index=False)
                    sess.commit()

                    sess.execute(update(FinalPrice).where(and_(FinalPrice._01article == FinalComparePrice._01article,
                                                               FinalPrice._14brand_filled_in == FinalComparePrice._14brand_filled_in,
                                                               FinalPrice.price > FinalComparePrice.price)).
                                 values(over_base_price=True))
                    self.price_compare_del += sess.query(FinalPrice).where(FinalPrice.over_base_price == True).delete()
                    sess.query(FinalComparePrice).delete()

            if self.price_compare_del:
                self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.price_compare_del} (Сравнение цены с осн. прайсом)")

    def set_rating(self, sess):
        sess.execute(update(FinalPrice).where(FinalPrice._07supplier_code == SuppliersForm.setting).
                     values(rating=SuppliersForm.rating))
        sess.execute(update(FinalPrice).values(rating=FinalPrice.rating * FinalPrice.price))

        ratings = select(FinalPrice.rating).order_by(FinalPrice.rating.desc()).limit(self.price_settings.max_rows)
        min_rating = sess.execute(select(func.min(ratings.c.rating))).scalar()
        # print('min r:', min_rating)
        # self.log.add(LOG_ID, f"min r: {min_rating}")
        if min_rating:
            sess.query(FinalPrice).where(FinalPrice.rating < min_rating).delete()  # для оптимизации
            # self.log.add(LOG_ID, f"удалено по мин. рейтингу: {del_cnt}")

    def create_csv(self, sess):
        try:
            csv_path = fr"{settings_data['catalogs_dir']}/pre Отправка"
            df = pd.DataFrame(columns=["Артикул", "Бренд", "Наименование", "Кол-во", "Цена", "Кратность",
                                       "17КодУникальности"])
            df.to_csv(fr"{csv_path}/_{self.file_name}", sep=';', decimal=',',
                      encoding="windows-1251", index=False, errors='ignore')

            limit = CHUNKSIZE
            loaded = 0
            while True:
                if self.price_settings.max_rows < loaded + limit:
                    limit = self.price_settings.max_rows - loaded
                req = select(FinalPrice._01article, FinalPrice._14brand_filled_in, FinalPrice._03name,
                             FinalPrice.count, FinalPrice.price, FinalPrice._06mult_new, FinalPrice._17code_unique
                             ).order_by(FinalPrice.rating.desc()).offset(loaded).limit(limit)
                df = pd.read_sql_query(req, sess.connection(), index_col=None)
                df = df.sort_values(FinalPrice.price.__dict__['name'])

                df_len = len(df)

                if not df_len:
                    break
                # print(df)
                df.to_csv(fr"{csv_path}/_{self.file_name}", mode='a',
                          sep=';', decimal=',', encoding="windows-1251", index=False, header=False,
                          errors='ignore')
                loaded += df_len
                # print(df) # settings_data['send_dir']
            shutil.copy(fr"{csv_path}/_{self.file_name}", fr"{settings_data['send_dir']}/{self.file_name}")
            # return True
        except PermissionError:
            self.log.add(LOG_ID,
                         f"Не удалось сформировать прайс {self.price_settings.buyer_price_code} ({self.cur_file_count + 1}/{self.total_file_count})",
                         f"Не удалось сформировать прайс <span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{self.price_settings.buyer_price_code}</span> ({self.cur_file_count + 1}/{self.total_file_count})")

            # return False

    def send_mail(self, sess):
        if sess.execute(func.count(FinalPrice.id)).scalar() == 0:
            self.add_log(self.price_settings.buyer_price_code, "Итоговое кол-во 0, не отправлен")
            return
        send_to = "ytopttorg@mail.ru"
        msg = MIMEMultipart()
        msg["Subject"] = Header(f"{self.price_settings.buyer_price_code}")
        msg["From"] = settings_data['mail_login']
        msg["To"] = send_to
        # msg.attach(MIMEText("price PL3", 'plain'))

        s = smtplib.SMTP("smtp.yandex.ru", 587, timeout=100)

        try:
            s.starttls()
            s.login(settings_data['mail_login'], settings_data['mail_imap_password'])

            file_path = fr"{settings_data['send_dir']}\{self.file_name}"

            with open(file_path, 'rb') as f:
                file = MIMEApplication(f.read())

            file.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
            msg.attach(file)

            s.sendmail(msg["From"], send_to, msg.as_string())

            shutil.copy(fr"{settings_data['send_dir']}/{self.file_name}",
                        fr"{settings_data['catalogs_dir']}/Последнее отправленное/{self.file_name}")
            self.send_time = datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")
        except Exception as mail_ex:
            raise mail_ex
        finally:
            s.quit()

        self.add_log(self.price_settings.buyer_price_code, f"Отправлено")

    def add_log(self, price_code, msg, cur_time=None):
        # лог с выводом этапа в таблицу
        if cur_time:
            log_text = "{}{price}{} {log_main_text} [{time_spent}]"
            self.log.add(LOG_ID, log_text.format('', '', price=price_code, log_main_text=msg,
                                                                time_spent=str(datetime.datetime.now() - cur_time)[:7]),
                         log_text.format(f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>",
                                         '</span>',
                                         price=price_code, log_main_text=msg,
                                         time_spent=str(datetime.datetime.now() - cur_time)[:7]))

        else:
            log_text = "{}{price}{} {log_main_text}"
            self.log.add(LOG_ID, log_text.format('', '', price=price_code, log_main_text=msg),
                         log_text.format(f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>",
                                         '</span>', price=price_code, log_main_text=msg))



class FinalPriceReportReset(QThread):
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)
    def run(self):
        try:
            with session() as sess:
                sess.query(PriceSendTime).delete()
                sess.commit()
            self.log.add(LOG_ID, f"Отчёт обнулён", f"<span style='color:{colors.green_log_color};'>Отчёт обнулён</span>  ")

        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "PriceReportReset Error", ex_text)

class FinalPriceReportUpdate(QThread):
    UpdateInfoTableSignal = Signal(list)
    UpdatePriceReportTime = Signal(str)

    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)
    def run(self):
        try:
            with (session() as sess):
                cols = (PriceSendTime.price_code, PriceSendTime.info_msg, PriceSendTime.send_time,)
                req = select(*cols).order_by(PriceSendTime.price_code)
                res = sess.execute(req).all()

                for r in res:
                    self.UpdateInfoTableSignal.emit(r)

                self.UpdatePriceReportTime.emit(str(datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))

                cols = (PriceSendTime.price_code.label("Код прайса покупателя"), PriceSendTime.info_msg.label("Статус"),
                        PriceSendTime.update_time.label("Время последнего формирования"), PriceSendTime.send_time.label("Время последней отправки"),
                        PriceSendTime.count.label("Итоговое кол-во"), PriceSendTime.count_after_filter.label("Кол-во после первого фильтра"),
                        PriceSendTime.del_price_b.label("Уд. ЦенаБ"), PriceSendTime.exception_words_del.label("Уд. Слова исключения"),
                        PriceSendTime.count_mult_del.label("Уд. Кол-во и Кратность"), PriceSendTime.correct_brands_del.label("Уд. правильные бренды"),
                        PriceSendTime.price_del.label("Уд. Нулевая/отрицательная цена"), PriceSendTime.dup_del.label("Уд. Дубли"),
                        PriceSendTime.price_compare_del.label("Уд. Сравнение цены с осн. прайсами"),
                        )
                req = select(*cols).order_by(PriceSendTime.price_code)

                df = pd.read_sql(req, engine)
                df.to_csv(fr"{settings_data['catalogs_dir']}/{REPORT_FILE}", sep=';', encoding="windows-1251",
                          index=False, header=True, errors='ignore')

                # self.log.add(LOG_ID, f"Отчёт обновлён", f"<span style='color:{colors.green_log_color};'>Отчёт обновлён</span>  ")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "FinalPriceReportUpdate Error", ex_text)