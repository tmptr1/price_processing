import time
from PySide6.QtCore import QThread, Signal
from sqlalchemy import text, select, delete, insert, update, and_, not_, func, cast, distinct, or_, inspect, REAL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
from models import (TotalPrice_2, FinalPrice, FinalComparePrice, Base3, SaleDK, BuyersForm, Data07, PriceException,
                    SuppliersForm, Brands_3, )
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

# из "Анкета покупателя" взять прайсы (Имя прайса) если "Включен?" == "да"
# сначала формировать прайсы, где "Срок" == 1
# сопоставить "Наименование" в Справочник_Бренд3
# "Куда запрещено" "Код покупателя" (%PLG%) and "Примечание" == "Действует в экселе"
#

class Sender(QThread):
    SetButtonEnabledSignal = Signal(bool)
    isPause = None
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)
    def run(self):
        global session, engine
        self.SetButtonEnabledSignal.emit(False)
        wait_sec = 2

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
                with session() as sess:
                    prices = sess.execute(select(BuyersForm.price_name).order_by(BuyersForm.period)).scalars().all()
                    # print(prices)
                self.cur_file_count = 0
                self.total_file_count = len(prices)

                # prices = ["2дня Прайс 2-ABS", ]
                if prices:
                    start_creating = datetime.datetime.now()
                    self.log.add(LOG_ID, f"Начало формирования ...")
                    for name in prices:
                        try:
                            # print('\n')
                            # print(name)
                            self.create_price(name)
                        except Exception as create_ex:
                            ex_text = traceback.format_exc()
                            self.log.error(LOG_ID, "create_ex ERROR:", ex_text)
                        finally:
                            self.cur_file_count += 1
                    self.log.add(LOG_ID, f"Формирование закончено [{str(datetime.datetime.now() - start_creating)[:7]}]")


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

        Base3.metadata.create_all(engine)

        with session() as sess:
            self.price_settings = sess.execute(select(BuyersForm).where(BuyersForm.price_name == name)).scalar()
            self.add_log(self.price_settings.buyer_price_code,
                         f"{self.price_settings.buyer_price_code} ...")

            allow_prices = self.get_allow_prises(sess)

            cols_for_price = [TotalPrice_2._01article, TotalPrice_2._03name, TotalPrice_2._05price_plus,
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
            price = select(*cols_for_price.keys()).where(and_(TotalPrice_2._07supplier_code.in_(allow_prices),
                                                              TotalPrice_2.to_price == self.price_settings.period))
            sess.execute(insert(FinalPrice).from_select(cols_for_price.values(), price))

            sess.commit()
            sess.query(FinalPrice).where(
                FinalPrice.put_away_zp.notlike(f"%{self.price_settings.zp_brands_setting}%")).delete()

            # cnt = sess.execute(func.count(FinalPrice.id)).scalar()
            # print(cnt)
            # if cnt < 0:
            #     print('---')  # continue

            self.add_log(self.price_settings.buyer_price_code,
                         f"{self.price_settings.buyer_price_code} Начальное кол-во строк: {sess.execute(func.count(FinalPrice.id)).scalar()}")

            self.delete_exceptions(sess)
            # шаг удалениедублей перенесен
            self.del_duples(sess)

            self.update_count_and_short_name(sess)

            self.update_price(sess)

            sess.execute(update(FinalPrice).where(and_(FinalPrice.price_b != None,
                                                       FinalPrice.price_b / self.price_settings.kb_price < FinalPrice.price)).
                         values(over_base_price=True))
            del_cnt = sess.query(FinalPrice).where(FinalPrice.over_base_price == True).delete()
            if del_cnt:
                self.add_log(self.price_settings.buyer_price_code,
                             f"{self.price_settings.buyer_price_code} Удалено: {del_cnt} (ЦенаБ)")

            self.del_over_price(sess)

            self.set_rating(sess)

            try:
                file_name = f"{str(self.price_settings.file_name).rstrip('.xlsx')}.csv"
                csv_path = fr"{settings_data['catalogs_dir']}/pre Отправка"
                df = pd.DataFrame(columns=["Артикул", "Бренд", "Наименование", "Кол-во", "Цена", "Кратность",
                                           "17КодУникальности"])
                df.to_csv(fr"{csv_path}/_{file_name}", sep=';', decimal=',',
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
                    df.to_csv(fr"{csv_path}/_{file_name}", mode='a',
                              sep=';', decimal=',', encoding="windows-1251", index=False, header=False,
                              errors='ignore')
                    loaded += df_len
                    # print(df) # settings_data['send_dir']
                shutil.copy(fr"{csv_path}/_{file_name}", fr"{settings_data['send_dir']}/{file_name}")
                # return True
            except PermissionError:
                self.log.add(LOG_ID,
                             f"Не удалось сформировать прайс {self.price_settings.buyer_price_code} ({self.cur_file_count + 1}/{self.total_file_count})",
                             f"Не удалось сформировать прайс <span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                             f"{self.price_settings.buyer_price_code}</span> ({self.cur_file_count + 1}/{self.total_file_count})")

                # return False

            self.add_log(self.price_settings.buyer_price_code,
                         f"{self.price_settings.buyer_price_code} Итоговое кол-во строк: {sess.execute(func.count(FinalPrice.id)).scalar()}")

            sess.commit()
            total_price_calc_time = str(datetime.datetime.now() - start_time)[:7]
            self.log.add(LOG_ID,
                         f"+ {self.price_settings.buyer_price_code} готов! ({self.cur_file_count + 1}/{self.total_file_count}) [{total_price_calc_time}]",
                         f"<span style='color:{colors.green_log_color};font-weight:bold;'>✔</span> "
                         f"<span style='background-color:hsl({self.color[0]}, {self.color[1]}%, {self.color[2]}%);'>"
                         f"{self.price_settings.buyer_price_code}</span> готов! ({self.cur_file_count + 1}/{self.total_file_count}) [{total_price_calc_time}]")

        # FinalPrice.__table__.drop(engine)
        # FinalComparePrice.__table__.drop(engine)


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
        return allow_prices

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

        del_cnt = sess.query(FinalPrice).where(FinalPrice.mult_less != None).delete()
        if del_cnt:
            self.add_log(self.price_settings.buyer_price_code,
                         f"{self.price_settings.buyer_price_code} Удалено: {del_cnt} (слова исключения)")

    def update_count_and_short_name(self, sess):
        # расчёт кол-ва
        if self.price_settings.us_above is not None:
            sess.execute(update(FinalPrice).where(and_(FinalPrice.unload_percent != 1,
                                                       FinalPrice.unload_percent < self.price_settings.us_above)).
                         values(count=func.floor(FinalPrice.count * self.price_settings.us_above)))
            del_cnt = sess.query(FinalPrice).where(or_(FinalPrice.count < 1, FinalPrice._06mult_new > FinalPrice.count)).delete()
            if del_cnt:
                self.add_log(self.price_settings.buyer_price_code,
                             f"{self.price_settings.buyer_price_code} Удалено: {del_cnt} (кол-во или кратность)")

        allow_brands = sess.execute(
            select(Brands_3.correct, Brands_3.short_name).where(Brands_3.agr == self.price_settings.name)).all()
        allow_brands_set = set(b.correct for b in allow_brands)
        # print(allow_brands_set)
        del_cnt = sess.query(FinalPrice).where(FinalPrice._14brand_filled_in.not_in(allow_brands_set)).delete()
        if del_cnt:
            self.add_log(self.price_settings.buyer_price_code,
                         f"{self.price_settings.buyer_price_code} Удалено: {del_cnt} (Правильные бренды)")

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

        del_cnt = sess.query(FinalPrice).where(or_(FinalPrice.price<=0, FinalPrice.price==None)).delete()
        if del_cnt:
            self.add_log(self.price_settings.buyer_price_code,
                         f"{self.price_settings.buyer_price_code} Удалено: {del_cnt} (Цена меньше/равна 0)")


    def del_duples(self, sess):
        duples = sess.execute(select(FinalPrice._15code_optt).group_by(FinalPrice._15code_optt).
                              having(func.count(FinalPrice.id) > 1)).scalars().all()
        # print('dupl:', len(duples))
        del_cnt = 0

        for d in duples:
            # DEL для всех повторений (mult_less уже не нужен на этом этапе)
            sess.execute(update(FinalPrice).where(FinalPrice._15code_optt == d).values(mult_less='D'))
            # Устанавливается 'not DEL' в каждой группе повторения, если цена в группе минимальная
            min_price = select(func.min(FinalPrice.price)).where(FinalPrice._15code_optt == d)
            sess.execute((update(FinalPrice)).where(
                and_(FinalPrice.mult_less == 'D', FinalPrice.price == min_price)).values(mult_less='n D'))
            # Среди записей с 'not DEL' ищутся записи не с максимальным кол-вом и на них устанавливается DEL
            max_count = select(func.max(FinalPrice.count)).where(FinalPrice.mult_less == 'n D')
            sess.execute(update(FinalPrice).where(
                and_(FinalPrice.mult_less == 'n D', FinalPrice.count != max_count)).values(mult_less='D'))
            # В оставшихся группах, где совпадает мин. цена и макс. кол-вл, остаются лишь записи с максимальным id
            max_id = select(func.max(FinalPrice.id)).where(FinalPrice.mult_less == 'n D')
            sess.execute(update(FinalPrice).where(
                and_(FinalPrice._15code_optt == d, FinalPrice.id != max_id)).values(mult_less='D'))

            del_cnt += sess.query(FinalPrice).where(FinalPrice.mult_less == 'D').delete()
            sess.execute(update(FinalPrice).where(FinalPrice.mult_less == 'n D').values(mult_less=None))

        if del_cnt:
            self.add_log(self.price_settings.buyer_price_code,
                         f"{self.price_settings.buyer_price_code} Удалено: {del_cnt} (Дубли)")

    def del_over_price(self, sess):
        if self.price_settings.period != 1 and self.price_settings.main_price:
            main_prices = list(map(str.strip, str(self.price_settings.main_price).split(',')))
            # print('MPs', main_prices)
            del_cnt = 0
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
                    del_cnt += sess.query(FinalPrice).where(FinalPrice.over_base_price == True).delete()
                    sess.query(FinalComparePrice).delete()

            if del_cnt:
                self.add_log(self.price_settings.buyer_price_code,
                             f"{self.price_settings.buyer_price_code} Удалено: {del_cnt} (сравнение цены с осн. прайсом)")

    def set_rating(self, sess):
        sess.execute(update(FinalPrice).where(FinalPrice._07supplier_code == SuppliersForm.setting).
                     values(rating=SuppliersForm.rating))
        sess.execute(update(FinalPrice).values(rating=FinalPrice.rating * FinalPrice.price))

        ratings = select(FinalPrice.rating).order_by(FinalPrice.rating.desc()).limit(self.price_settings.max_rows)
        min_rating = sess.execute(select(func.min(ratings.c.rating))).scalar()
        # print('min r:', min_rating)
        if min_rating:
            sess.query(FinalPrice).where(FinalPrice.rating < min_rating).delete()  # для оптимизации

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