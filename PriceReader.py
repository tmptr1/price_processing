from PySide6.QtCore import QThread, Signal
import random
import multiprocessing as mp
import time
import traceback
import datetime
import math
import os
import holidays
import holidays_ru
from sqlalchemy import text, select, delete, insert, update, Sequence, and_, not_, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
import numpy as np
import pandas as pd
from python_calamine.pandas import pandas_monkeypatch
pd.set_option('future.no_silent_downcasting', True)
import openpyxl
import colors

from models import (Base, PriceReport, Price_1, BasePrice, MassOffers, CatalogUpdateTime, SupplierPriceSettings, FileSettings,
                    ArticleFix, Brands, PriceChange, WordsOfException, SupplierGoodsFix, ExchangeRate)
from Logs import add_log_cf
import setting
engine = setting.get_engine()
# engine.echo = True
session = sessionmaker(engine)
settings_data = setting.get_vars()

CHUNKSIZE = 50000
LOG_ID = 1
REPORT_FILE = r"price_report.csv"


class MainWorker(QThread):
    StartTotalTimeSignal = Signal(bool)
    SetButtonEnabledSignal = Signal(bool)
    isPause = None

    def __init__(self, log=None, sender=None, threads_count=None, parent=None):
        self.log = log
        self.sender = sender
        self.threads_count = threads_count
        QThread.__init__(self, parent)


    def run(self):
        global session, engine
        wait_sec = 30
        self.SetButtonEnabledSignal.emit(False)
        while not self.isPause:
            start_cycle_time = datetime.datetime.now()
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
                    for file in files:
                        file_name = '.'.join(file.split('.')[:-1])
                        if len(file_name) < 4:
                            continue
                        price_code = file_name[:4]
                        new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(fr"{settings_data['mail_files_dir']}/{file}")
                                                                          ).strftime("%Y-%m-%d %H:%M:%S")

                        req = select(SupplierPriceSettings.standard).where(SupplierPriceSettings.price_code == price_code)
                        standard = sess.execute(req).scalar()
                        if not standard:
                            sess.add(PriceReport(file_name=file, price_code=price_code, info_message="Нет в условиях",
                                                 updated_at=new_update_time))
                            self.log.add(LOG_ID, f"{price_code} Нет в условиях", f"<span style='color:{colors.orange_log_color};"
                                                                                 f"font-weight:bold;'>{price_code}</span> Нет в условиях")
                            continue
                        elif str(standard).upper() != 'ДА':
                            req = update(PriceReport).where(PriceReport.file_name == file).values(info_message="Не указано сохранение",
                                                                                                  updated_at=new_update_time)
                            sess.execute(req)
                            self.log.add(LOG_ID, f"{price_code} Не указано сохранение",
                                         f"<span style='color:{colors.orange_log_color};"
                                         f"font-weight:bold;'>{price_code}</span> Не указано сохранение")
                            continue

                        req = select(PriceReport.updated_at).where(PriceReport.price_code == price_code)
                        last_update_tile = sess.execute(req).scalar()
                        # print(price_code, in_cond, last_update_tile)
                        if not last_update_tile:
                            new_files.append(file)
                            continue
                        if str(last_update_tile) < new_update_time:
                            new_files.append(file)
                    sess.commit()

                # print(f"{new_files=}")
                # return

                # new_files = ['1FRA Прайс ФорвардАвто Краснодар.xlsx']#'1MTK Остатки оригинал Bobcat Doosan.xlsx'] #'1AVX AVEX.xlsx']
                # new_files = ['1MTK Остатки оригинал Bobcat Doosan.xlsx']

                if new_files:
                    self.log.add(LOG_ID, "Начало обработки")
                    cur_time = datetime.datetime.now()
                    if len(new_files) < self.threads_count:
                        self.threads_count = len(new_files)

                    self.StartTotalTimeSignal.emit(True)
                    self.sender.send(["new", len(new_files)])

                    with mp.Pool(processes=self.threads_count) as pool:
                        args = [[file, self.sender] for file in new_files]
                        pool.map(multi_calculate, args)

                    self.log.add(LOG_ID, f"Обработка закончена [{str(datetime.datetime.now() - cur_time)[:7]}]")

                    self.StartTotalTimeSignal.emit(False)
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


def multi_calculate(args):
    file_name, sender = args
    color = [random.randrange(0, 360), random.randrange(55, 100), 90]
    try:
        price_code = file_name[:4]
        sender.send(["add", mp.current_process().name, price_code, 1, f"Загрузка сырых дынных...", True])

        path_to_price = fr"{settings_data['mail_files_dir']}/{file_name}"
        frmt = file_name.split('.')[-1]
        new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_price)).strftime("%Y-%m-%d %H:%M:%S")

        start_calc_price_time = datetime.datetime.now()
        cur_time = datetime.datetime.now()
        with (session() as sess):
            req = select(PriceReport).where(PriceReport.file_name == file_name)
            is_report_exists = sess.execute(req).scalar()
            if not is_report_exists:
                sess.add(PriceReport(file_name=file_name, price_code=price_code))

            if not check_price_time(price_code, file_name, sender, sess):
                sess.execute(update(PriceReport).where(PriceReport.file_name == file_name).values(
                    info_message="Не подходит по сроку обновления", updated_at=new_update_time)) # sess.execute(req)
                sess.commit()
                add_log_cf(LOG_ID, "Не подходит по сроку обновления", sender, price_code, color)
                return


            req = select(FileSettings).where(FileSettings.price_code == price_code)
            price_table_settings = sess.execute(req).scalars().all()
            if not price_table_settings:
                sess.execute(update(PriceReport).where(PriceReport.file_name == file_name).values(
                    info_message="Нет настроек", updated_at=new_update_time))
                sess.commit()
                add_log_cf(LOG_ID, "Нет настроек", sender, price_code, color)
                return

            # сопоставление столлцов
            id_settig, rc_dict = get_setting_id(price_table_settings, frmt, path_to_price)
            if not id_settig:
                sess.execute(update(PriceReport).where(PriceReport.file_name == file_name).values(
                    info_message="Нет подходящих настроек", updated_at=new_update_time))
                sess.commit()
                add_log_cf(LOG_ID, "Нет подходящих настроек", sender, price_code, color)
                return

            sett = sess.get(FileSettings, {'id': id_settig})

            # Удаление старой версии
            sess.query(Price_1).where(Price_1._07supplier_code == price_code).delete()


            load_data_to_db(sett, rc_dict, frmt, path_to_price, price_code, sess)

            add_log_cf(LOG_ID, "Загрузка сырых дынных завершена", sender, price_code, color, cur_time)

            cur_time = datetime.datetime.now()
            sender.send(["add", mp.current_process().name, price_code, 1, f"Обработка 1-6, 12, 14, 15 ..."])

            # исправление товаров поставщиков (01Артикул, 02Производитель, 03Наименование, 04Количество, 05Цена, 06Кратность)
            suppliers_goods_compare(price_code, sett, sess)


            req = select(ArticleFix).where(ArticleFix.price_code == price_code)
            article_settings = sess.execute(req).scalars().all()
            change_types = {"Начинается с": [lambda x: Price_1.article_s.startswith(x), '^{}'],
                            "Содержит": [lambda x: Price_1.article_s.contains(x), '{}'],
                            "Заканчивается на": [lambda x: Price_1.article_s.endswith(x), '{}$'],
            }
            for a in article_settings:
                # print(a.price_code, a.change_type, a.find, a.change)
                change_type = change_types.get(a.change_type, None)
                if change_type:
                    req = update(Price_1).where(and_(Price_1._07supplier_code == price_code, change_type[0](a.find))
                                                ).values(_01article=Price_1.article_s.regexp_replace(
                                                        change_type[1].format(a.find), "" if not a.change else a.change))
                    sess.execute(req)

            # 01Артикул
            sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1._01article == None)
                                        ).values(_01article=Price_1.article_s))

            # 02Производитель
            sess.execute(update(Price_1).where(Price_1._07supplier_code == price_code).values(brand_s_low = func.lower(Price_1.brand_s)))
            sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1.brand_s_low == Brands.brand_low)
                                        ).values(_02brand=Brands.correct_brand))

            # 14Производитель заполнен
            sess.execute(update(Price_1).where(Price_1._07supplier_code == price_code).values(_14brand_filled_in=Price_1.brand_s))
            sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1._02brand != None))
                         .values(_14brand_filled_in=Price_1._02brand))

            # 03Наименование
            sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1._03name == None))
                         .values(_03name=Price_1.name_s))

            # 04Количество
            sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1._04count == None))
                         .values(_04count=Price_1.count_s))

            # 05Цена
            sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1._05price == None))
                         .values(_05price=Price_1.price_s))
            # 12Сумма
            sess.execute(update(Price_1).where(Price_1._07supplier_code == price_code).values(
                _12sum=Price_1.count_s * Price_1.price_s))
            # для валют
            sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1.currency_s != None,
                                                    ExchangeRate.code == func.upper(Price_1.currency_s)))
                         .values(_05price=Price_1._05price * ExchangeRate.rate, _12sum=Price_1.count_s * Price_1.price_s * ExchangeRate.rate))

            # Настройка строк: Вариант изменения цены
            if sett.change_price_type in ("- X %", "+ X %"):
                percent = None
                try:
                    percent = float(f"{sett.change_price_type[0]}{sett.change_price_val}")
                except:
                    pass
                if percent:
                    sess.execute(update(Price_1).where(Price_1._07supplier_code == price_code)
                        .values(_05price=Price_1._05price * (1+percent)))

            # Изменение цены по условиям
            discounts = sess.execute(select(PriceChange).where(PriceChange.price_code == price_code)).scalars().all()
            for dscnt in discounts:
                sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1._14brand_filled_in == dscnt.brand))
                             .values(_05price=Price_1._05price * (1 - dscnt.discount/100)))

            # 06Кратность
            sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1._06mult == None))
                         .values(_06mult=Price_1.mult_s))

            # 15КодТутОптТорг
            sess.execute(update(Price_1).where(Price_1._07supplier_code == price_code)
                         .values(_15code_optt=func.upper(Price_1._01article+Price_1._14brand_filled_in).regexp_replace(r"\W", "", 'g')))

            add_log_cf(LOG_ID, "Обработка 1-6, 12, 14, 15 завершена", sender, price_code, color, cur_time)

            cur_time = datetime.datetime.now()
            sender.send(["add", mp.current_process().name, price_code, 1, f"Обработка 13, 17, 18, 20 ..."])

            # 20ИслючитьИзПрайса
            cols_dict = {"Ключ1 поставщика": Price_1.key1_s, "Артикул поставщика": Price_1.article_s, "Производитель поставщика": Price_1.brand_s,
                         "Наименование поставщика": Price_1.name_s, " ВалютаП": Price_1.currency_s, "Примечание поставщика": Price_1.notice_s,
                         }
            check_types = {"Начинается с": lambda col, x: col.startswith(x), #'^{}'],
                            "Содержит": lambda col, x: col.contains(x), #'{}'],
                            "Не содержит": lambda col, x: not_(col.contains(x)), # '{}'],
                            "Заканчивается на": lambda col, x: col.endswith(x), #'{}$'],
            }
            words = sess.execute(select(WordsOfException).where(WordsOfException.price_code == price_code)).scalars().all()
            for w in words:
                # print(f"{w.colunm_name}|{w.condition}|{w.text}")
                check = check_types.get(f"{w.condition}", None)
                col = cols_dict.get(w.colunm_name, None)
                # print(check, col)
                if check and col:
                    # print(check, col)
                    sess.execute(update(Price_1).where(and_(Price_1._07supplier_code == price_code, check(col, w.text)))
                             .values(_20exclude=w.text))

            # 13Градация
            # with sum_table as (select id, _04count, _05price, sum(_05price) over (ORDER BY _05price, id) as prev_sum from price_1
            # where _07supplier_code = price_code and _20exclude is NULL and _05price > 0)
            # update price_1 set _13grad = FLOOR((total_sum-prev_sum)/(total_sum/100)) from sum_table where _07supplier_code = price_code and price_1.id = sum_table.id
            total_sum = sess.execute(
                select(func.sum(Price_1._05price)).where(Price_1._07supplier_code == price_code)).scalar()

            subq = select(Price_1.id,
                          func.sum(Price_1._05price).over(order_by=(Price_1._05price, Price_1.id)).label(
                              'prev_sum')
                          ).where(
                and_(Price_1._07supplier_code == price_code, Price_1._20exclude == None, Price_1._05price > 0))
            cte = update(Price_1).where(and_(Price_1._07supplier_code == price_code, Price_1.id == subq.c.id)
                                        ).values(
                _13grad=func.floor((total_sum - subq.c.prev_sum) / (total_sum / 100)))
            sess.execute(cte)

            # 17КодУникальности
            sess.execute(update(Price_1).where(Price_1._07supplier_code == price_code)
                         .values(_17code_unique=func.upper(Price_1._07supplier_code + Price_1._15code_optt + "ДАSS")))

            # 18КороткоеНаименование
            sess.execute(update(Price_1).where(Price_1._07supplier_code == price_code)
                         .values(_18short_name=func.regexp_substr(Price_1._03name, r'(\S+.){1,2}(\S+){0,1}')))

            add_log_cf(LOG_ID, "Обработка 13, 17, 18, 20 завершена", sender, price_code, color, cur_time)

            cur_time = datetime.datetime.now()
            sender.send(["add", mp.current_process().name, price_code, 1, f"Формирование csv..."])

            # to csv
            csv_cols_dict = {"Ключ1 поставщика": Price_1.key1_s, "Артикул поставщика": Price_1.article_s,
                             "Производитель поставщика": Price_1.brand_s, "Наименование поставщика": Price_1.name_s,
                             "Количество поставщика": Price_1.count_s, "Цена поставщика": Price_1.price_s,
                             "ВалютаП": Price_1.currency_s, "Кратность поставщика": Price_1.mult_s,
                             "Примечание поставщика": Price_1.notice_s, "01Артикул": Price_1._01article,
                             "02Производитель": Price_1._02brand, "14Производитель заполнен": Price_1._14brand_filled_in,
                             "03Наименование": Price_1._03name, "04Количество": Price_1._04count,
                             "05Цена": Price_1._05price, "12Сумма": Price_1._12sum, "06Кратность": Price_1._06mult,
                             "15КодТутОптТорг": Price_1._15code_optt, "07Код поставщика": Price_1._07supplier_code,
                             "20ИслючитьИзПрайса": Price_1._20exclude, "13Градация": Price_1._13grad,
                             "17КодУникальности": Price_1._17code_unique, "18КороткоеНаименование": Price_1._18short_name,
                             }
            df = pd.DataFrame(columns=csv_cols_dict.keys())
            df.to_csv(fr"{settings_data['exit_1_dir']}/{price_code}.csv", sep=';', decimal=',',
                      encoding="windows-1251", index=False, errors='ignore')

            limit = CHUNKSIZE
            loaded = 0
            while True:
                req = select(*[csv_cols_dict[k].label(k) for k in csv_cols_dict]).order_by(Price_1._17code_unique).offset(loaded).limit(limit)
                df = pd.read_sql_query(req, sess.connection(), index_col=None)
                df_len = len(df)
                if not df_len:
                    break
                df.to_csv(fr"{settings_data['exit_1_dir']}/{price_code}.csv", mode='a',
                          sep=';', decimal=',', encoding="windows-1251", index=False, header=False, errors='ignore')
                loaded += df_len
                # print(df)

            # УДАЛИТЬ ИЗ БД ПОСЛЕ ФОРМИРОВАНИЯ CSV
            sess.query(Price_1).where(Price_1._07supplier_code == price_code).delete()

            sess.execute(update(PriceReport).where(PriceReport.price_code == price_code).values(info_message="Ок", updated_at=new_update_time))

            sess.commit()

            add_log_cf(LOG_ID, "csv сформирован", sender, price_code, color, cur_time)




        # cur_time = datetime.datetime.now()
        # sender.send(["add", mp.current_process().name, price_code, 1, f"Загрузка сырых дынных...", True])
        # time.sleep(random.randrange(7))
        # add_log_cf(LOG_ID, "Загрузка сырых дынных завершена", sender, price_code, cur_time, color)
        #
        # cur_time = datetime.datetime.now()
        # sender.send(["add", mp.current_process().name, price_code, 1, f"Удаление дублей..."])
        # time.sleep(random.randrange(7))
        # add_log_cf(LOG_ID,"Дубли удалены", sender, price_code, cur_time, color)
        # add_log_cf(LOG_ID, "Загрузка сырых дынных завершена", sender, price_code, color, cur_time)
        total_price_calc_time = str(datetime.datetime.now() - start_calc_price_time)[:7]
        sender.send(["log", LOG_ID, f"+ {price_code} готов! [{total_price_calc_time}]",
                        f"<span style='color:{colors.green_log_color};font-weight:bold;'>✔</span> "
                        f"<span style='background-color:hsl({color[0]}, {color[1]}%, {color[2]}%);'>"
                        f"{price_code}</span> готов! [{total_price_calc_time}]"])

    except Exception as ex:
        ex_text = traceback.format_exc()
        sender.send(["error", LOG_ID, "M ERROR", ex_text])
    finally:
        sender.send(["end", mp.current_process().name])


def check_price_time(price_code, file_name, sender, sess):
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
        # print(price_code, '-')
        return 0
    if days > days2:
        # logger.info(f"{file_name_} не подходит по сроку обновления")
        # logger_report.info(f"{file_name_} не подходит по сроку обновления")
        # print(price_code, '-')
        return 0
    # print(price_code, '+')
    return 1

def get_setting_id(price_table_settings, frmt, path_to_price):
    id_settig = None
    rc_dict = dict()
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
            if rc_dict[k][0]:
                cols.append(rc_dict[k][0])
                rows.append(rc_dict[k][1])
            else:
                del_from_dict.append(k)
            # if rc_dict[k][1]:
        for d in del_from_dict:
            del rc_dict[d]
        # print(f"{rc_dict=}")

        max_col = max(cols)
        max_row = max(rows)

        if frmt in ('xls', 'xlsx'):
            pandas_monkeypatch()
            table = pd.read_excel(path_to_price, header=None, engine='calamine', nrows=max_row)
        elif frmt == 'csv':
            table = pd.read_csv(path_to_price, header=None, sep=';', encoding='windows-1251', nrows=max_row,
                                encoding_errors='ignore')
        else:
            # print(f"Неизвестный формат")
            break

        if len(table.columns) < max_col:
            continue

        brk = False
        for r, c, name in rc_dict.values():
            if not name:
                continue
            # print(r, c, name, name in table.loc[r-1, c -1])
            if name not in table.loc[r - 1, c - 1]:
                brk = True

        if not brk:
            id_settig = sett.id

            break

    return id_settig, rc_dict

def load_data_to_db(sett, rc_dict, frmt, path_to_price, price_code, sess):
    loaded_rows = sett.pass_up  # пропускаются указанное кол-во строк
    r_limit = CHUNKSIZE
    method = None
    cols = [i[1] - 1 for i in rc_dict.values()]
    # print(f"{rc_dict=}")
    new_cols_name = {rc_dict[k][1] - 1: k for k in rc_dict}

    while True:
        if frmt in ('xls', 'xlsx'):
            pandas_monkeypatch()

            table = pd.DataFrame
            try:
                table = pd.read_excel(path_to_price, usecols=[*cols], header=None,
                                      nrows=r_limit, skiprows=loaded_rows, engine='calamine')
                method = 1
            except:
                pass

            if table.empty and method != 1:
                table = pd.read_excel(path_to_price, usecols=[*cols], header=None, nrows=r_limit, skiprows=loaded_rows)
        elif frmt == 'csv':
            try:
                table = pd.read_csv(path_to_price, header=None, sep=';', encoding='windows-1251',
                                    usecols=[*cols], nrows=r_limit, skiprows=loaded_rows, encoding_errors='ignore')
            except pd.errors.EmptyDataError:
                break
        table_size = len(table)
        if not table_size:
            break

        loaded_rows += table_size

        # удаление последний строк в соотвествии с параметром "Пропуск снизу" (skipF)
        if table_size == r_limit:
            if frmt in ('xls', 'xlsx'):
                pandas_monkeypatch()
                # last = pd.read_excel(fr"{path_to_prices}\{file_name}", usecols=[*m], header=None,
                #                       nrows=skipF, skiprows=loaded_rows, engine='calamine')
                last = pd.DataFrame
                try:
                    last = pd.read_excel(path_to_price, usecols=[*cols], header=None,
                                         nrows=sett.pass_down, skiprows=loaded_rows, engine='calamine')
                except:
                    pass
                if last.empty:
                    last = pd.read_excel(path_to_price, usecols=[*cols], header=None,
                                         nrows=sett.pass_down, skiprows=loaded_rows)

                last = len(last)
            elif frmt == 'csv':
                last = 0
                try:
                    last_t = pd.read_csv(path_to_price, header=None, sep=';', encoding='windows-1251', usecols=[*cols],
                                         nrows=sett.pass_down, skiprows=loaded_rows, encoding_errors='ignore')
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
        # print(f"{table=}")
        table = get_correct_df(table, empty_cols_dict, sess.connection())
        # print(f"2 {table=}")

        table.to_sql(name=Price_1.__tablename__, con=sess.connection(), if_exists='append', index=False)

def get_correct_df(df, cols, con):
    # print(f"{cols=}")
    '''for varchar(x), real, numeric, integer'''
    pk = []
    # берутся столбцы из таблицы: название столбца, максимальная длина его поля
    # sess.commit()
    select()
    res = con.execute(text(
        f"SELECT column_name, character_maximum_length FROM information_schema.columns WHERE table_name = '{Price_1.__tablename__}' "
        f"and column_name != 'id'")).all()
    for i in res:
        if cols.get(i[0], None):
            # print(f'append {i[0]} {i[1]}')
            cols[i[0]] = i[1]
    res = con.execute(text(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{Price_1.__tablename__}' and is_nullable = 'NO' "
        f"and column_name != 'id'")).all()
    pk = [i[0] for i in res]

    # df = pd.read_excel(path_to_file, usecols=[cols[c][0] for c in cols], na_filter=False, sheet_name=sheet_name)
    # df = df.rename(columns={cols[c][0]: c for c in cols})

    for c in cols:
        char_limit = cols[c]
        if char_limit:  # str
            df[c] = df[c].apply(lambda x: str(x)[:char_limit] or None)
        else:  # float/int
            df[c] = df[c].apply(to_float)
            # df[c] = df[c].replace('', 0)
            # df = df[df[c].apply(is_float)]
            # df[c] = np.float64(df[c])
        if c in pk:  # для PK
            df = df[df[c].notna()]
    return df
    # print(df)
    # df.to_sql(name=table_name, con=con, if_exists='append', index=False, index_label=False, chunksize=CHUNKSIZE)

def to_float(x):
    try:
        x = float(str(x).replace(',', '.'))
        if math.isnan(x) or math.isinf(x):
            return 0
        if 1E+37 < x < 1E-37:  # real
            return 0
        return x
    except:
        return 0


def suppliers_goods_compare(price_code, sett, sess):
    key_conditions = and_(Price_1._07supplier_code == price_code, SupplierGoodsFix.import_setting == price_code,
                          Price_1.key1_s == SupplierGoodsFix.key1)
    article_brand_conditions = and_(Price_1._07supplier_code == price_code, SupplierGoodsFix.import_setting == price_code,
                                    Price_1.article_s == SupplierGoodsFix.article_s, Price_1.brand_s == SupplierGoodsFix.brand_s)
    article_name_conditions = and_(Price_1._07supplier_code == price_code, SupplierGoodsFix.import_setting == price_code,
                                   Price_1.article_s == SupplierGoodsFix.article_s, Price_1.name_s == SupplierGoodsFix.name)
    compare_vars = {"Ключ": key_conditions, "Артикул + Бренд": article_brand_conditions,
                    "Артикул + НаименованиеП": article_name_conditions}

    if sett.compare in compare_vars.keys():
        req = update(Price_1).where(compare_vars[sett.compare]
                                    ).values(_01article=SupplierGoodsFix.article, _02brand=SupplierGoodsFix.brand,
                                             _03name=SupplierGoodsFix.name, _04count=SupplierGoodsFix.put_away_count,
                                             _05price=SupplierGoodsFix.price_s, _06mult=SupplierGoodsFix.mult_s)
        sess.execute(req)

class PriceReportUpdate(QThread):
    UpdateInfoTableSignal = Signal(list)
    UpdatePriceReportTime = Signal(str)
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)
    def run(self):
        try:
            with session() as sess:
                req = select(PriceReport.price_code.label("Код прайса"), PriceReport.info_message.label("Статус"),
                                              PriceReport.updated_at.label("Время"))
                reports = sess.execute(req).all()
                for r in reports:
                    self.UpdateInfoTableSignal.emit(r)
                self.UpdatePriceReportTime.emit(str(datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))
                df = pd.read_sql(req, engine)
                df.to_csv(fr"{settings_data['catalogs_dir']}/{REPORT_FILE}", sep=';', encoding="windows-1251",
                          index=False, header=True, errors='ignore')

                self.log.add(LOG_ID, f"Отчёт обновлён", f"<span style='color:{colors.green_log_color};'>Отчёт обновлён</span>  ")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "PriceReportUpdate Error", ex_text)

        # try:
        #     with session() as sess:
        #         req = select(MailReport.sender.label("Sender"), MailReport.file_name.label("File name"),
        #                      MailReport.date.label("Date"))
        #         db_data = sess.execute(req).all()
        #         for i in db_data:
        #             self.UpdateInfoTableSignal.emit(i[0], i[1], i[2])
        #
        #         sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == REPORT_FILE).delete()
        #         sess.add(CatalogUpdateTime(catalog_name=REPORT_FILE,
        #                                    updated_at=str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))
        #         sess.commit()
        #
        #     self.UpdateMailReportTime.emit(str(datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))
        #
        #     if not self.first_start:
        #         df = pd.read_sql(req, engine)
        #         df.to_csv(fr"{settings_data['catalogs_dir']}/{REPORT_FILE}", sep=';', encoding="windows-1251",
        #                   index=False, header=True, errors='ignore')  # ['Sender', 'File name', 'Date']
        #
        #     self.first_start = False
        #
        #     self.log.add(LOG_ID, f"Отчёт обновлён",
        #                  f"<span style='color:{colors.green_log_color};'>Отчёт обновлён</span>  ")
        # except Exception as ex:
        #     ex_text = traceback.format_exc()
        #     self.log.error(LOG_ID, f"MailReportUpdate Error", ex_text)