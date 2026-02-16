import time
from PySide6.QtCore import QThread, Signal
from sqlalchemy import text, select, delete, insert, update, and_, not_, func, cast, distinct, or_, inspect, REAL, literal_column, case
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
from models import (TotalPrice_2, FinalPrice, FinalComparePrice, Base3, SaleDK, BuyersForm, Data07, PriceException,
                    SuppliersForm, Brands_3, PriceSendTime, FinalPriceHistory, AppSettings, PriceReport, PriceSendTimeHistory,
                    FinalPriceHistoryDel, SupplierPriceSettings)
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email import encoders
from telebot import TeleBot
import pandas as pd
import traceback
import datetime
import re
import os
import shutil
from zipfile import ZipFile, ZIP_DEFLATED
import random

from tg_users_id import USERS, TG_TOKEN
import colors
import setting
engine = setting.get_engine()
session = sessionmaker(engine)
settings_data = setting.get_vars()
CHUNKSIZE = int(settings_data["chunk_size"])
tg_bot = TeleBot(TG_TOKEN)

LOG_ID = 4
WEEKDAYS = {0: "пон", 1: "втор", 2: "сре", 3: "чет", 4: "пят", 5: "суб", 6: "вос"}
REPORT_FILE = r"final_price_report.csv"


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
            self.send_tg_msg()
            start_cycle_time = datetime.datetime.now()
            try:
                # with session() as sess:
                #     miss_email = sess.execute(select(distinct(PriceSendTime.price_code)).where(and_(PriceSendTime.send_time == None,
                #                                                                                     PriceSendTime.price_code == BuyersForm.buyer_price_code,
                #                                                                                     func.upper(BuyersForm.for_send) == 'ДА')).
                #                               order_by(PriceSendTime.price_code)).scalars().all()
                #     print(miss_email)

                # СНАЧАЛА С МИН СРОКОМ
                weekday = WEEKDAYS[datetime.datetime.now().weekday()]

                price_name_list = []
                with session() as sess:
                    prices = sess.execute(select(BuyersForm).where(func.upper(BuyersForm.included)=='ДА').order_by(BuyersForm.period)).scalars().all()
                    for p in prices:
                        if not datetime_check(weekday, p.send_days):
                            continue
                        send_times = [p.time1, p.time2, p.time3, p.time4, p.time5, p.time6]
                        last_update = sess.execute(select(PriceSendTime.update_time).where(PriceSendTime.price_code==p.buyer_price_code)).scalar()
                        for st in send_times:
                            if st is not None:
                                try:
                                    t_now = datetime.datetime.now()
                                    h, m = map(int, str(st).split(':')[:2])
                                    t = t_now.replace(hour=h, minute=m)
                                    # print(p.buyer_price_code, t)

                                    if t_now > t and (not last_update or last_update < t):
                                        price_name_list.append(p.id)
                                        break
                                except:
                                    pass

                # price_name_list = [10, ]
                # price_name_list = ["Прайс AvtoTO", ]

                self.cur_file_count = 0
                self.total_file_count = len(price_name_list)

                # return
                if price_name_list:
                    self.StartCreationSignal.emit(True)
                    start_creating = datetime.datetime.now()
                    self.log.add(LOG_ID, f"Начало формирования ...")
                    for id in price_name_list:
                        try:
                            self.create_price(id)
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


    def send_tg_msg(self):
        try:
            cur_time = datetime.datetime.now()
            if cur_time.hour < 13 or cur_time.hour > 20:
                return

            with session() as sess:
                last_tg_send_time = sess.execute(select(AppSettings.var).where(AppSettings.param=='last_tg_price_send')).scalar()
                last_tg_send_time = datetime.datetime.strptime(last_tg_send_time, "%Y-%m-%d %H:%M:%S")
                if (cur_time - last_tg_send_time).total_seconds() < 20 * 60 * 60:
                    return

                last_time = sess.execute(select(func.max(PriceSendTime.send_time))).scalar()
                if not last_time or last_time.day != cur_time.day:
                    msg = f"❗❗ СЕГОДНЯ ПРАЙСЫ НЕ ОТПРАВЛЯЛИСЬ ❗❗"
                else:
                    msg = f"Последняя отправка прайсов была {last_time}"

                for u in USERS:
                    tg_bot.send_message(chat_id=u, text=msg, parse_mode='HTML')
                # self.last_tg_send = datetime.datetime.now()
                sess.execute(update(AppSettings).where(AppSettings.param=='last_tg_price_send').values(var=cur_time.strftime("%Y-%m-%d %H:%M:%S")))
                self.log.add(LOG_ID, f"Уведомление отправлено", f"<span style='color:{colors.green_log_color};'>Уведомление отправлено</span>  ")
                sess.commit()

        except Exception as tg_ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "ERROR:", ex_text)

    def create_price(self, id):
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
            self.price_settings = sess.execute(select(BuyersForm).where(and_(BuyersForm.id == id,
                                                                             func.upper(BuyersForm.included)=='ДА'))).scalar()
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

            self.time_for_history_del = f"'{datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')}'"
            allow_prices = self.get_allow_prises(sess)
            # TotalPrice_2.code_pb_p
            cols_for_price = [TotalPrice_2.key1_s, TotalPrice_2.article_s, TotalPrice_2.brand_s, TotalPrice_2.name_s,
                              TotalPrice_2.count_s, TotalPrice_2.price_s, TotalPrice_2.currency_s, TotalPrice_2.mult_s,
                              TotalPrice_2.notice_s, TotalPrice_2._01article_comp, TotalPrice_2._01article, TotalPrice_2._02brand,
                              TotalPrice_2._03name,TotalPrice_2._04count, TotalPrice_2._05price, TotalPrice_2._05price_plus,
                              TotalPrice_2._06mult_new, TotalPrice_2._07supplier_code, TotalPrice_2._13grad,
                              TotalPrice_2._14brand_filled_in, TotalPrice_2._15code_optt, TotalPrice_2._17code_unique,
                              TotalPrice_2._18short_name, TotalPrice_2.delay, TotalPrice_2.sell_for_OS,
                              TotalPrice_2.markup_os, TotalPrice_2.markup_R, TotalPrice_2.markup_pb,
                              TotalPrice_2.min_markup, TotalPrice_2.min_wholesale_markup, TotalPrice_2.grad_step,
                              TotalPrice_2.wh_step, TotalPrice_2.access_pp, TotalPrice_2.put_away_zp,
                              TotalPrice_2.offers_wh, TotalPrice_2.price_b, TotalPrice_2.count,
                              TotalPrice_2.mult_less, TotalPrice_2.buy_count,
                              TotalPrice_2.unload_percent, TotalPrice_2.min_price, TotalPrice_2.to_price]
            cols_for_price = {i: i.__dict__['name'] for i in cols_for_price}
            price = select(TotalPrice_2._03name, TotalPrice_2.count, *cols_for_price.keys()).where(and_(TotalPrice_2.to_price == self.price_settings.period,
                                                              TotalPrice_2._07supplier_code.in_(allow_prices)))
            sess.execute(insert(FinalPrice).from_select(['_03name_old', 'count_old', *cols_for_price.values()], price))

            # sess.commit()

            sess.query(FinalPrice).where(and_(FinalPrice.put_away_zp!=None,
                FinalPrice.put_away_zp.notlike(f"%{self.price_settings.zp_brands_setting}%"))).delete()


            self.delete_exceptions(sess)
            count_after_first_filter = sess.execute(func.count(FinalPrice.id)).scalar()

            self.add_log(self.price_settings.buyer_price_code, f"Кол-во строк после первого фильтра: {count_after_first_filter}",
                         cur_time)

            total_prices_result = dict()
            price_count = sess.execute(select(FinalPrice._07supplier_code, func.count(FinalPrice.id).label('cnt')).
                                       group_by(FinalPrice._07supplier_code).order_by(FinalPrice._07supplier_code))

            for p, c in price_count:
                total_prices_result[p] = c

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

            del_price_b = self.add_dels_in_history(sess, (FinalPrice.over_base_price == True), 'ЦенаБ')
            if del_price_b:
                sess.query(FinalPrice).where(FinalPrice.over_base_price == True).delete()
                self.add_log(self.price_settings.buyer_price_code, f"Удалено: {del_price_b} (ЦенаБ)")

            self.del_over_price(sess)

            self.set_rating(sess)
            self.add_log(self.price_settings.buyer_price_code, f"Расчёт рейтинга, удаление ЦенаБ", cur_time)

            cur_time = datetime.datetime.now()
            self.file_name = f"{str(self.price_settings.file_name).replace('.xlsx', '')}.csv"
            self.create_csv(sess)
            self.add_log(self.price_settings.buyer_price_code, f"csv создан", cur_time)

            total_rows = sess.execute(func.count(FinalPrice.id)).scalar()
            log_msg = f"Итоговое кол-во строк: {total_rows}"
            if self.price_settings.max_rows:
                log_msg += f". Лимит: {self.price_settings.max_rows}"
            self.add_log(self.price_settings.buyer_price_code, log_msg)

            cur_time = datetime.datetime.now()
            # is_sended = False
            self.send_time = sess.execute(select(PriceSendTime.send_time).where(PriceSendTime.price_code==self.price_settings.buyer_price_code)).scalar()
            recent_sent = False
            if self.send_time:
                if (datetime.datetime.now()-self.send_time) < datetime.timedelta(seconds=15*60):
                    recent_sent = True

            self.new_info_msg = None
            self.new_send_time = None
            if total_rows == 0:
                self.add_log(self.price_settings.buyer_price_code, "Итоговое кол-во 0, не отправлен")
                self.new_info_msg = 'Итоговое кол-во 0, не отправлен'
            elif self.need_to_send and str(self.price_settings.for_send).upper() == 'ДА' and not recent_sent:
                if self.send_mail():
                    last_supplier_price_updates = select(PriceReport.price_code, PriceReport.db_added)
                    sess.execute(update(FinalPrice).where(FinalPrice._07supplier_code == last_supplier_price_updates.c.price_code).
                                 values(supplier_update_time=last_supplier_price_updates.c.db_added))

                    # sess.query(FinalPriceHistory).where(and_(FinalPriceHistory.price_code==self.price_settings.buyer_price_code,
                    #                                          FinalPriceHistory._15code_optt==FinalPrice._15code_optt)).delete()

                    # is_sended = True

            cols_for_price = [FinalPrice.key1_s, FinalPrice.article_s, FinalPrice.brand_s, FinalPrice.name_s,
                              FinalPrice.count_s, FinalPrice.price_s, FinalPrice.currency_s, FinalPrice.mult_s,
                              FinalPrice.notice_s,
                              FinalPrice._01article_comp, FinalPrice._01article, FinalPrice._02brand, FinalPrice.brand,
                              FinalPrice._03name_old, FinalPrice._03name, FinalPrice._04count, FinalPrice._05price,
                              FinalPrice._05price_plus,
                              FinalPrice._06mult_new, FinalPrice._07supplier_code, FinalPrice._14brand_filled_in,
                              FinalPrice._15code_optt, FinalPrice._17code_unique, FinalPrice.count_old,
                              FinalPrice.count, FinalPrice.price, FinalPrice.supplier_update_time]
            cols_for_price = {i: i.__dict__['name'] for i in cols_for_price}

            if self.new_send_time is not None:
                send_time_val = f"'{datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')}'"
            elif self.send_time is not None:
                send_time_val = f"'{self.send_time}'"
            else:
                send_time_val = "NULL"

            now_dt = f"'{datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')}'"
            price = select(literal_column(f"'{self.price_settings.buyer_price_code}'"),
                           literal_column(now_dt), *cols_for_price.keys()) # send_time_val
            sess.execute(
                insert(FinalPriceHistory).from_select(['price_code', 'send_time', *cols_for_price.values()], price))

            price_count = sess.execute(
                select(FinalPrice._07supplier_code, func.count(FinalPrice.id).label('cnt')).group_by(
                    FinalPrice._07supplier_code))

            prices_count_msg = ''
            new_total_prices_result = dict()
            for p, c in price_count:
                new_total_prices_result[p] = c

            for k in total_prices_result:
                prices_count_msg += f"{k} {total_prices_result[k]}/{new_total_prices_result.get(k, 0)}; "

            if prices_count_msg: prices_count_msg = prices_count_msg[:-1]

            count_for_report = self.price_settings.max_rows if self.price_settings.max_rows > total_rows else total_rows

            sess.query(PriceSendTime).where(PriceSendTime.price_code==self.price_settings.buyer_price_code).delete()
            now_dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sess.add(PriceSendTime(price_code=self.price_settings.buyer_price_code,
                                   update_time=now_dt, send_time=None if send_time_val == "NULL" else send_time_val,
                                   info_msg=self.new_info_msg if self.new_info_msg else 'Ок',
                                   count=count_for_report, count_after_filter=count_after_first_filter,
                                   del_price_b=del_price_b, exception_words_del=self.exception_words_del,
                                   count_mult_del=self.count_mult_del, correct_brands_del=self.correct_brands_del,
                                   price_del=self.price_del, dup_del=self.dup_del, price_compare_del=self.price_compare_del,
                                   prices_count=prices_count_msg))
            sess.add(PriceSendTimeHistory(price_code=self.price_settings.buyer_price_code,
                                   update_time=now_dt, send_time=None if send_time_val == "NULL" else send_time_val,
                                   info_msg=self.new_info_msg if self.new_info_msg else 'Ок',
                                   count=count_for_report, count_after_filter=count_after_first_filter,
                                   del_price_b=del_price_b, exception_words_del=self.exception_words_del,
                                   count_mult_del=self.count_mult_del, correct_brands_del=self.correct_brands_del,
                                   price_del=self.price_del, dup_del=self.dup_del, price_compare_del=self.price_compare_del,
                                   prices_count=prices_count_msg))

            sess.query(FinalPrice).delete()
            sess.commit()
            # if is_sended:
            self.add_log(self.price_settings.buyer_price_code, f"Отправка, сохранение прайса в истории", cur_time)

            total_price_calc_time = str(datetime.datetime.now() - start_time)[:7]
            self.log.add(LOG_ID,
                         f"+ {self.price_settings.buyer_price_code} готов! ({self.cur_file_count + 1}/{self.total_file_count}) [{total_price_calc_time}]",
                         f"<span style='color:{colors.green_log_color};font-weight:bold;'>✔</span> "
                         f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{self.price_settings.buyer_price_code}</span> готов! ({self.cur_file_count + 1}/{self.total_file_count}) [{total_price_calc_time}]")

        FinalPrice.__table__.drop(engine)
        FinalComparePrice.__table__.drop(engine)
        # FinalPriceDuplDel.__table__.drop(engine)


    def add_dels_in_history(self, sess, condition, reason):
        cols_for_del_history = [FinalPrice.key1_s, FinalPrice.article_s, FinalPrice.brand_s, FinalPrice.name_s,
                          FinalPrice.count_s, FinalPrice.price_s, FinalPrice.currency_s, FinalPrice.mult_s,
                          FinalPrice.notice_s,
                          FinalPrice._01article_comp, FinalPrice._01article, FinalPrice._02brand, FinalPrice.brand,
                          FinalPrice._03name_old, FinalPrice._03name, FinalPrice._04count, FinalPrice._05price,
                          FinalPrice._05price_plus,
                          FinalPrice._06mult_new, FinalPrice._07supplier_code, FinalPrice._14brand_filled_in,
                          FinalPrice._15code_optt, FinalPrice._17code_unique, FinalPrice.count_old,
                          FinalPrice.count, FinalPrice.price, FinalPrice.supplier_update_time]

        price = select(literal_column(f"'{reason}'"),
                       literal_column(f"'{self.price_settings.buyer_price_code}'"),
                       literal_column(self.time_for_history_del), *cols_for_del_history).where(condition)
        cnt = sess.execute(insert(FinalPriceHistoryDel).from_select(['reason', 'price_code', 'send_time', *cols_for_del_history], price)).rowcount
        return cnt

    def get_allow_prises(self, sess):
        all_prices = set(sess.execute(select(Data07.setting)).scalars().all())
        allow_prices = set(sess.execute(select(Data07.setting).where(and_(Data07.access_pp.like(f"%{self.price_settings.buyer_price_code}%")),
                             SupplierPriceSettings.price_code==Data07.setting, func.upper(SupplierPriceSettings.works)=='ДА', func.upper(SupplierPriceSettings.calculate)=='ДА',
                                                                     PriceReport.price_code==Data07.setting, PriceReport.info_message2=='Ок')).scalars().all())
        weekday = WEEKDAYS[datetime.datetime.now().weekday()]

        allow_prices_wd_settings = sess.execute(select(SuppliersForm.setting, SuppliersForm.days).
                                                where(SuppliersForm.days.like(f"%{weekday}%"))).all()

        allow_prices_wd = set()
        for price in allow_prices_wd_settings:
            # print(price.setting, price.days)
            if datetime_check(weekday, price.days):
                allow_prices_wd.add(price.setting)

        self.add_log(self.price_settings.buyer_price_code, f"{allow_prices_wd=}")
        allow_prices = allow_prices & allow_prices_wd

        self.add_log(self.price_settings.buyer_price_code, f"{allow_prices=}")
        self.add_log(self.price_settings.buyer_price_code, f"Не прошли: {all_prices-allow_prices}")


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

        self.exception_words_del = self.add_dels_in_history(sess, (FinalPrice.mult_less != None), 'Слова исключения')
        if self.exception_words_del:
            sess.query(FinalPrice).where(FinalPrice.mult_less != None).delete()
            self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.exception_words_del} (Слова исключения)")

    def update_count_and_short_name(self, sess, allow_brands):
        self.count_mult_del = 0
        # расчёт кол-ва
        if self.price_settings.us_above is not None:
            sess.execute(update(FinalPrice).where(and_(FinalPrice.unload_percent != 1,
                                                       FinalPrice.unload_percent < self.price_settings.us_above)).
                         values(count=func.floor(FinalPrice.count * self.price_settings.us_above)))
            self.count_mult_del = self.add_dels_in_history(sess, (or_(FinalPrice.count < 1, FinalPrice._06mult_new > FinalPrice.count)), 'Кол-во или кратность')
            if self.count_mult_del:
                sess.query(FinalPrice).where(or_(FinalPrice.count < 1, FinalPrice._06mult_new > FinalPrice.count)).delete()
                self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.count_mult_del} (Кол-во или кратность)")

        allow_brands_set = set(b.correct for b in allow_brands)

        self.correct_brands_del = self.add_dels_in_history(sess, (FinalPrice._14brand_filled_in.not_in(allow_brands_set)), 'Правильные бренды')
        if self.correct_brands_del:
            sess.query(FinalPrice).where(FinalPrice._14brand_filled_in.not_in(allow_brands_set)).delete()
            self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.correct_brands_del} (Правильные бренды)")

        short_name = set()
        for b in allow_brands:
            if str(b.short_name).upper() == 'ДА':
                short_name.add(b.correct)

        sess.execute(update(FinalPrice).where(FinalPrice._14brand_filled_in.in_(short_name)).
                     values(_03name=FinalPrice._18short_name))

        sess.execute(update(FinalPrice).where(and_(Brands_3.zp_brands_setting == self.price_settings.zp_brands_setting,
                                                   FinalPrice._14brand_filled_in == Brands_3.correct)).values(brand=Brands_3.brand))


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
                                        (FinalPrice.delay - self.price_settings.delay))) * (self.price_settings.kos_markup + 1)))

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

        self.price_del = self.add_dels_in_history(sess, (or_(FinalPrice.price <= 0, FinalPrice.price == None)), 'Цена меньше/равна 0')
        if self.price_del:
            sess.query(FinalPrice).where(or_(FinalPrice.price <= 0, FinalPrice.price == None)).delete()
            self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.price_del} (Цена меньше/равна 0)")

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

        self.dup_del = self.add_dels_in_history(sess, (FinalPrice.mult_less != None),'Дубли')
        if self.dup_del:
            sess.query(FinalPrice).where(FinalPrice.mult_less != None).delete()
            self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.dup_del} (Дубли)")

    def del_over_price(self, sess):
        self.price_compare_del = 0
        if self.price_settings.period != 1 and self.price_settings.main_price:
            main_prices = list(map(str.strip, str(self.price_settings.main_price).split(',')))
            for p in main_prices:
                base_name = str(p).rstrip('.xlsx')
                price_path = fr"{settings_data['send_dir']}/{base_name}.csv"
                if os.path.exists(price_path):
                    if self.price_settings.period is None:
                        continue
                    main_price = pd.read_csv(price_path, header=0, sep=';', encoding='windows-1251',
                                             usecols=["Артикул", "Бренд", "Цена"], encoding_errors='ignore',
                                             na_filter=False)
                    main_price = main_price.rename(columns={"Артикул": FinalComparePrice._01article.__dict__['name'],
                                                            "Бренд": FinalComparePrice._14brand_filled_in.__dict__['name'],
                                                            "Цена": FinalComparePrice.price.__dict__['name']})
                    main_price['period'] = self.price_settings.period
                    main_price['price'] = main_price['price'].str.replace(',', '.')
                    main_price.to_sql(name=FinalComparePrice.__tablename__, con=sess.connection(), if_exists='append',
                                      index=False)
                    sess.commit()

                    sess.execute(update(FinalPrice).where(and_(FinalPrice._01article == FinalComparePrice._01article,
                                                               FinalPrice._14brand_filled_in == FinalComparePrice._14brand_filled_in,
                                                               FinalPrice.price > FinalComparePrice.price)).
                                 values(over_base_price=True))
                    new_del = self.add_dels_in_history(sess, (FinalPrice.over_base_price == True), 'Сравнение цены с осн. прайсом')
                    if new_del:
                        sess.query(FinalPrice).where(FinalPrice.over_base_price == True).delete()
                        self.price_compare_del += new_del
                    sess.query(FinalComparePrice).delete()

            if self.price_compare_del:
                self.add_log(self.price_settings.buyer_price_code, f"Удалено: {self.price_compare_del} (Сравнение цены с осн. прайсом)")

    def set_rating(self, sess):
        sess.execute(update(FinalPrice).where(FinalPrice._07supplier_code == SuppliersForm.setting).
                     values(rating=SuppliersForm.rating))
        sess.execute(update(FinalPrice).values(rating=FinalPrice.rating * FinalPrice.price))

        ratings = select(FinalPrice.rating).order_by(FinalPrice.rating.desc()).limit(self.price_settings.max_rows)
        min_rating = sess.execute(select(func.min(ratings.c.rating))).scalar()
        if min_rating:
            sess.query(FinalPrice).where(FinalPrice.rating < min_rating).delete()  # для оптимизации
            # self.log.add(LOG_ID, f"удалено по мин. рейтингу: {del_cnt}")

    def create_csv(self, sess):
        try:
            csv_path = fr"{settings_data['catalogs_dir']}/pre Отправка"

            headers_patterns = {"Артикул": FinalPrice._01article, "Бренд": FinalPrice.brand, "Наименование": FinalPrice._03name,
                                "Кол-во": FinalPrice.count, "Цена": FinalPrice.price, "Кратность": FinalPrice._06mult_new,
                                       "17КодУникальности": FinalPrice._17code_unique}
            cols = [self.price_settings.col_1, self.price_settings.col_2, self.price_settings.col_3, self.price_settings.col_4,
                    self.price_settings.col_5, self.price_settings.col_6, self.price_settings.col_7]
            headers = dict()

            for c in cols:
                col_name = headers_patterns.get(c, None)
                if col_name:
                    headers[c] = col_name

            # "Артикул", "Бренд", "Наименование", "Кол-во", "Цена", "Кратность", "17КодУникальности"
            df = pd.DataFrame(columns=[*headers.keys()])
            df.to_csv(fr"{csv_path}/_{self.file_name}", sep=';', decimal=',',
                      encoding="windows-1251", index=False, errors='ignore')

            limit = CHUNKSIZE
            loaded = 0
            while True:
                if self.price_settings.max_rows < loaded + limit:
                    limit = self.price_settings.max_rows - loaded
                req = select(*headers.values()).order_by(FinalPrice.rating.desc()).offset(loaded).limit(limit)
                df = pd.read_sql_query(req, sess.connection(), index_col=None)
                df = df.sort_values(FinalPrice.price.__dict__['name'])

                df_len = len(df)

                if not df_len:
                    break

                # df[FinalPrice._01article.__dict__['name']] = df[FinalPrice._01article.__dict__['name']].apply(lambda x: f'="{x}"' if str(x).startswith('0') else x)
                df.to_csv(fr"{csv_path}/_{self.file_name}", mode='a',
                          sep=';', decimal=',', encoding="windows-1251", index=False, header=False,
                          errors='ignore')
                loaded += df_len

            shutil.copy(fr"{csv_path}/_{self.file_name}", fr"{settings_data['send_dir']}/{self.file_name}")

        except PermissionError:
            self.log.add(LOG_ID,
                         f"Не удалось сформировать прайс {self.price_settings.buyer_price_code} ({self.cur_file_count + 1}/{self.total_file_count})",
                         f"Не удалось сформировать прайс <span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{self.price_settings.buyer_price_code}</span> ({self.cur_file_count + 1}/{self.total_file_count})")


    def send_mail(self):
        # emails = []
        emails = self.price_settings.emails
        if not emails:
            emails = []
        elif ',' in emails:
            emails = list(map(str.strip, str(self.price_settings.emails).split(',')))
        else:
            emails = [str(emails).strip()]

        if emails:
            for send_to in [*emails, "ytopttorg@mail.ru"]:
                # self.add_log(self.price_settings.buyer_price_code, f"SEND {send_to}")
                msg = MIMEMultipart()
                msg["Subject"] = Header(f"{self.price_settings.price_name}")
                msg["From"] = settings_data['mail_login']
                msg["To"] = send_to
                # msg.attach(MIMEText("price PL3", 'plain'))

                s = smtplib.SMTP("smtp.yandex.ru", 587, timeout=100)

                try:
                    s.starttls()
                    s.login(settings_data['mail_login'], settings_data['mail_imap_password'])

                    if self.price_settings.file_extension == 'zip':
                        file_path_csv = fr"{settings_data['send_dir']}/{self.file_name}"
                        file_path_zip = fr"{settings_data['send_dir']}/{os.path.splitext(self.file_name)[0]}.zip"
                        with ZipFile(file_path_zip, 'w', compression=ZIP_DEFLATED) as zf:
                            zf.write(file_path_csv, arcname=os.path.basename(file_path_csv))

                        with open(file_path_zip, 'rb') as f:
                            file = MIMEApplication(f.read())

                        file.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path_zip))
                        msg.attach(file)

                        s.sendmail(msg["From"], send_to, msg.as_string())

                        shutil.copy(file_path_csv, fr"{settings_data['catalogs_dir']}/Последнее отправленное/{self.file_name}")
                    else:
                        file_path = fr"{settings_data['send_dir']}/{self.file_name}"
                        with open(file_path, 'rb') as f:
                            file = MIMEBase('application', 'vnd.ms-excel')
                            file.set_payload(f.read())

                        encoders.encode_base64(file)
                        file.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
                        msg.attach(file)

                        s.sendmail(msg["From"], send_to, msg.as_string())

                        shutil.copy(file_path, fr"{settings_data['catalogs_dir']}/Последнее отправленное/{self.file_name}")

                except Exception as mail_ex:
                    raise mail_ex
                finally:
                    s.quit()

            self.new_send_time = datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")
            self.add_log(self.price_settings.buyer_price_code, f"Отправлено ({self.price_settings.emails})")
            return True
        else:
            self.add_log(self.price_settings.buyer_price_code, f"НЕ отправлено, почта для отправки не указана")
            self.new_info_msg = 'Не отправлено, почта для отправки не указана'
            return False

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


def datetime_check(weekday, days):
    price_times = re.search(weekday + r' \d{1,2}:\d{2}.?-.?\d{1,2}:\d{2}', str(days))
    if price_times:
        price_times = price_times.group()
        price_times = price_times.strip(weekday).strip()
        time_list = [None] * 2
        time_list[0], time_list[1] = map(str.strip, price_times.split('-'))
        time_list = [datetime.time(hour=int(t.split(':')[0]), minute=int(t.split(':')[1])) for t in time_list]

        cur_time = datetime.datetime.now().time()
        if time_list[0] > cur_time or time_list[1] < cur_time:
            return False
    else:
        price_time = re.search(weekday + ' \d{1,2}:\d{2}', str(days))
        if price_time:
            price_time = price_time.group()
            price_time = price_time.strip(weekday).strip()
            h, m = map(int, price_time.split(':'))
            price_time = datetime.time(hour=h, minute=m)
            cur_time = datetime.datetime.now().time()
            if price_time < cur_time:
                return False
    return True


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
                        PriceSendTime.prices_count.label("Кол-во в разрезе прайсов поставщиков")
                        )
                req = select(*cols).order_by(PriceSendTime.price_code)

                df = pd.read_sql(req, engine)
                df.to_csv(fr"{settings_data['catalogs_dir']}/{REPORT_FILE}", sep=';', encoding="windows-1251",
                          index=False, header=True, errors='ignore')

        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "FinalPriceReportUpdate Error", ex_text)