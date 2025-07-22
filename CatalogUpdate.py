from PySide6.QtCore import QThread, Signal
import multiprocessing as mp
import time
import math
import datetime
import traceback
import requests
import os
import pandas as pd
from sqlalchemy import text, select, delete, insert, update, Sequence, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
from models import (Base, BasePrice, MassOffers, CatalogUpdateTime, SupplierPriceSettings, FileSettings,
                    ArticleFix, Brands, PriceChange, WordsOfException, SupplierGoodsFix, AppSettings, ExchangeRate)
import colors

import setting
engine = setting.get_engine()
session = sessionmaker(engine)
settings_data = setting.get_vars()

green_log_color = "#38b04c"

PARAM_LIST = ["base_price_update", "mass_offers_update"]
CHUNKSIZE = int(settings_data["chunk_size"])
LOG_ID = 2

class CatalogUpdate(QThread):
    SetButtonEnabledSignal = Signal(bool)
    isPause = None

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
                self.update_currency()
                self.update_price_settings_catalog()
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

            # проверка на паузу
            finish_cycle_time = datetime.datetime.now()
            if wait_sec > (finish_cycle_time - start_cycle_time).seconds:
                for _ in range(wait_sec - (finish_cycle_time - start_cycle_time).seconds):
                    if self.isPause:
                        break
                    time.sleep(1)
        else:
            self.SetButtonEnabledSignal.emit(True)

    def update_currency(self):
        try:
            with session() as sess:
                now = datetime.datetime.now()#.strftime("%Y-%m-%d %H:%M:%S")
                req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name == 'currency')
                res = sess.execute(req).scalar()

                if res:
                    if now.strftime("%Y-%m-%d") == res.strftime("%Y-%m-%d"):
                        return

                sess.query(ExchangeRate).delete()
                valute_data = requests.get('https://www.cbr-xml-daily.ru/daily_json.js').json()
                valute_dict = dict()
                for c in valute_data['Valute']:
                    info = valute_data['Valute'][c]
                    valute_dict[info["CharCode"]] = info["Value"] / info["Nominal"]

                df = pd.DataFrame.from_dict(valute_dict, orient='index')
                df = df.reset_index()
                df = df.rename(columns={"index": "code", 0: "rate"})
                df.to_sql(name='exchange_rate', con=sess.connection(), if_exists='append', index=False)

                sess.query(CatalogUpdateTime).where(CatalogUpdateTime.catalog_name == 'currency').delete()
                sess.add(CatalogUpdateTime(catalog_name='currency', updated_at=now.strftime("%Y-%m-%d %H:%M:%S")))

                sess.commit()

                self.log.add(LOG_ID, "Курс валют обновлён", f"<span style='color:{colors.green_log_color};font-weight:bold;'>Курс валют</span> обновлён")

        except (OperationalError, UnboundExecutionError) as db_ex:
            raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "update_currency Error", ex_text)

    def update_price_settings_catalog(self):
        try:
            path_to_file = fr"{settings_data['catalogs_dir']}\4.0 Настройка прайсов поставщиков.xlsx"
            base_name = os.path.basename(path_to_file)
            new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_file)).strftime("%Y-%m-%d %H:%M:%S")
            with session() as sess:
                req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name == base_name)
                res = sess.execute(req).scalar()
                if res and str(res) >= new_update_time:
                    return
                cur_time = datetime.datetime.now()
                self.log.add(LOG_ID, f"Обновление {base_name} ...",
                             f"Обновление <span style='color:{colors.green_log_color};font-weight:bold;'>{base_name}</span> ...")

                table_name = 'file_settings'
                table_class = FileSettings
                cols = {"price_code": ["Прайс"], "email": ["Почта"], "file_name_cond": ["Условие имени файла"],
                        "file_name": ["Имя файла"], "pass_up": ["Пропуск сверху"], "pass_down": ["Пропуск снизу"],
                        "compare": ["Сопоставление по"], "rc_key_s": ["R/C КлючП"], "name_key_s": ["Название КлючП"],
                        "rc_article_s": ["R/C АртикулП"], "name_article_s": ["Название АртикулП"],
                        "rc_brand_s": ["R/C БрендП"], "name_brand_s": ["Название БрендП"], "replace_brand_s": ["Подставить Бренд"],
                        "rc_name_s": ["R/C НаименованиеП"],
                        "name_name_s": ["Название НаименованиеП"], "rc_count_s": ["R/C КоличествоП"],
                        "name_count_s": ["Название КоличествоП"], "rc_price_s": ["R/C ЦенаП"],
                        "name_price_s": ["Название ЦенаП"],
                        "rc_mult_s": ["R/C КратностьП"], "name_mult_s": ["Название КратностьП"],
                        "rc_notice_s": ["R/C ПримечаниеП"],
                        "name_notice_s": ["Название ПримечаниеП"], "rc_currency_s": ["R/C Валюта"],
                        "name_currency_s": ["Название Валюта"], "change_price_type": ["Вариант изменения цены"],
                        "change_price_val": ["Значение исправления цены"],
                        }
                sheet_name = "Настройка строк"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                rc_cols = [['r_key_s', 'c_key_s', 'rc_key_s'],
                           ['r_article_s', 'c_article_s', 'rc_article_s'],
                           ['r_brand_s', 'c_brand_s', 'rc_brand_s'],
                           ['r_name_s', 'c_name_s', 'rc_name_s'],
                           ['r_count_s', 'c_count_s', 'rc_count_s'],
                           ['r_price_s', 'c_price_s', 'rc_price_s'],
                           ['r_mult_s', 'c_mult_s', 'rc_mult_s'],
                           ['r_notice_s', 'c_notice_s', 'rc_notice_s'],
                           ['r_currency_s', 'c_currency_s', 'rc_currency_s'],
                           ]
                req = ''
                for r, c, rc in rc_cols:
                    # req += (f"update file_settings set {r} = (regexp_split_to_array({rc}, '[RC]'))[2]::INTEGER, "
                    #                 f"{c} = (regexp_split_to_array({rc}, '[RC]'))[3]::INTEGER where {rc} is not NULL;")
                    req += (f"update file_settings set {r} = (regexp_split_to_array({rc}, '[RC]'))[2]::INTEGER, "
                            f"{c} = (regexp_split_to_array({rc}, '[RC]'))[3]::INTEGER where {rc} SIMILAR TO 'R[0-9]{{1,}}C[0-9]{{1,}}';")
                sess.execute(text(req))
                sess.query(FileSettings).filter(FileSettings.price_code == None).delete()

                table_name = 'supplier_price_settings'
                table_class = SupplierPriceSettings
                cols = {"supplier_code": ["Код поставщика"], "price_code": ["Код прайса"], "save": ["Сохраняем"],
                        "standard": ["Стандартизируем"], "calculate": ["Обрабатываем"], "buy": ["Можем купить?"],
                        "works": ["Работаем"], "wholesale": ["Прайс оптовый"],
                        "buy_for_working_capital": ["Закупка для оборотных средств"],
                        "is_base_price": ["Цену считать базовой"], "costs": ["Издержки"], "update_time": ["Срок обновление не более"],
                        "in_price": ["В прайс"], "short_name": ["Краткое наименование"], "access_pp": ["Разрешения ПП"],
                        "supplier_lot": ["Лот поставщика"], "over_base_price": ["К.Превышения базовой цены"],
                        "convenient_lot": ["Лот удобный нам"], "min_markup": ["Наценка мин"],
                        "markup_wholesale": ["Наценка опт"],
                        "max_markup": ["Наценка макс"], "unload_percent": ["% Отгрузки"], "delay": ["Отсрочка"],
                        "markup_os": ["Наценка для ОС"],
                        "row_change_percent": ["Допустимый процент изменения количества строк"],
                        "price_change_percent": ["Допустимый процент изменения цены"],
                        "supplier_rating": ["Рейтинг поставщика"],
                        }
                sheet_name = "Настройка прайсов"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
                sess.query(SupplierPriceSettings).filter(SupplierPriceSettings.supplier_code == None).delete()

                table_name = 'article_fix'
                table_class = ArticleFix
                cols = {"price_code": ["Код прайса"], "change_type": ["Вариант исправления"], "find": ["Найти"],
                        "change": ["Установить"], }
                sheet_name = "Исправление Артикула"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
                # sess.query(ArticleFix).filter(ArticleFix.price_code == None).delete()

                table_name = 'brands'
                table_class = Brands
                cols = {"correct_brand": ["Правильный Бренд"], "brand": ["Поиск"], }
                sheet_name = "Справочник Бренды"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
                sess.execute(update(Brands).values(brand_low=func.lower(Brands.brand)))


                table_name = 'price_change'
                table_class = PriceChange
                cols = {"price_code": ["Код прайса"], "brand": ["Производитель поставщика"], "discount": ["Скидка, %"]}
                sheet_name = "Изменение цены по условиям"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                table_name = 'words_of_exception'
                table_class = WordsOfException
                cols = {"price_code": ["Код прайса"], "colunm_name": ["Столбец поиска"], "condition": ["Условие"],
                        "text": ["Текст"]}
                sheet_name = "Слова_исключения"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                table_name = 'supplier_goods_fix'
                table_class = SupplierGoodsFix
                cols = {"supplier": ["Поставщик"], "import_setting": ["Настройка импорта прайса"], "key1": ["Ключ1"],
                        "article_s": ["Артикул поставщика"], "brand_s": ["Производитель поставщика"],
                        "name": ["Наименование"],
                        "brand": ["Производитель"], "article": ["Артикул"], "price_s": ["Цена поставщика"],
                        "sales_ban": ["Запрет продажи"], "original": ["Оригинал"],
                        "marketable_appearance": ["Товарный вид"],
                        "put_away_percent": ["Убрать %"], "put_away_count": ["Убрать шт"], "nomenclature": ["Номенклатура"],
                        "mult_s": ["Кратность поставщика"],
                        }
                sheet_name = "Исправление товаров поставщиков"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == base_name).delete()
                sess.add(CatalogUpdateTime(catalog_name=base_name, updated_at=new_update_time))

                sess.commit()

            self.log.add(LOG_ID, f"{base_name} обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]",
                         f"<span style='color:{colors.green_log_color};font-weight:bold;'>{base_name}</span> обновлён "
                         f"[{str(datetime.datetime.now() - cur_time)[:7]}]")
        except (OperationalError, UnboundExecutionError) as db_ex:
            raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"update_price_settings_catalog Error", ex_text)


class SaveTime(QThread):
    def __init__(self, BasePriceTime, MassOffersTime, log=None, parent=None):
        self.param_dict = {PARAM_LIST[0]: BasePriceTime, PARAM_LIST[1]: MassOffersTime}
        self.log = log
        QThread.__init__(self, parent)

    def run(self):
        try:
            with session() as sess:
                sess.query(AppSettings).filter(AppSettings.param.in_(PARAM_LIST)).delete()
                for k in self.param_dict:
                    sess.add(AppSettings(param=k, var=f"{self.param_dict[k].hour()} {self.param_dict[k].minute()}"))
                sess.commit()
            self.log.add(LOG_ID, f"Время сохранено", f"<span style='color:{colors.green_log_color};font-weight:bold;'>Время сохранено</span>  ")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"SaveTime Error", ex_text)

def get_catalogs_time_update():
    try:
        with session() as sess:
            req = select(AppSettings).where(AppSettings.param.in_(PARAM_LIST))
            times = sess.execute(req).scalars().all()
            times = {t.param: t.var for t in times}
            return times
    except:
        return None


def update_catalog(ses, path_to_file, cols, table_name, table_class, sheet_name=0):
    '''for varchar(x), real, numeric, integer'''
    con = ses.connection()
    pk = []
    # берутся столбцы из таблицы: название столбца, максимальная длина его поля
    # with engine.connect() as sess:
    # print(table_name)
    req = delete(table_class)
    con.execute(req)
    con.execute(text(f"ALTER SEQUENCE {table_name}_id_seq restart 1"))
    # sess.commit()
    res = con.execute(text(
        f"SELECT column_name, character_maximum_length FROM information_schema.columns WHERE table_name = '{table_name}' "
        f"and column_name != 'id'")).all()
    for i in res:
        if cols.get(i[0], None):
            cols[i[0]].append(i[1])
    res = con.execute(text(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' and is_nullable = 'NO' "
        f"and column_name != 'id'")).all()
    pk = [i[0] for i in res]

    df = pd.read_excel(path_to_file, usecols=[cols[c][0] for c in cols], na_filter=False,sheet_name=sheet_name)
    df = df.rename(columns={cols[c][0]: c for c in cols})

    for c in cols:
        char_limit = cols[c][1]
        if char_limit:  # str
            df[c] = df[c].apply(lambda x: str(x)[:char_limit] or None)
        else:  # float/int
            df[c] = df[c].apply(to_float)
            # df[c] = df[c].replace('', 0)
            # df = df[df[c].apply(is_float)]
            # df[c] = np.float64(df[c])
        if c in pk:  # для PK
            df = df[df[c].notna()]
    # return (df)
    # print(df)
    df.to_sql(name=table_name, con=con, if_exists='append', index=False, index_label=False, chunksize=CHUNKSIZE)

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



        # def update_price_settings_catalog(self):
        #     path_to_file = fr"{data['catalogs_dir']}\4.0 Настройка прайсов поставщиков.xlsx"
        #     base_name = os.path.basename(path_to_file)
        #     new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_file))
        #     with session() as sess:
        #         req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name==base_name)
        #         res = sess.execute(req).scalar()
        #         if res and res >= new_update_time:
        #             return
        #         cur_time = datetime.datetime.now()
        #         self.log.add(LOG_ID, f"Обновление {base_name} ...",
        #                      f"Обновление <span style='color:{green_log_color};font-weight:bold;'>{base_name}</span> ...")
        #
        #         table_name = 'file_settings'
        #         table_class = FileSettings
        #         cols = {"price_code": ["Прайс"], "pass_up": ["Пропуск сверху"], "pass_down": ["Пропуск снизу"],
        #                 "compare": ["Сопоставление по"], "rc_key_s": ["R/C КлючП"], "name_key_s": ["Название КлючП"],
        #                 "rc_article_s": ["R/C АртикулП"], "name_article_s": ["Название АртикулП"], "rc_brand_s": ["R/C БрендП"],
        #                 "name_brand_s": ["Название БрендП"], "rc_name_s": ["R/C НаименованиеП"],
        #                 "name_name_s": ["Название НаименованиеП"], "rc_count_s": ["R/C КоличествоП"],
        #                 "name_count_s": ["Название КоличествоП"], "rc_price_s": ["R/C ЦенаП"], "name_price_s": ["Название ЦенаП"],
        #                 "rc_mult_s": ["R/C КратностьП"], "name_mult_s": ["Название КратностьП"], "rc_notice_s": ["R/C ПримечаниеП"],
        #                 "name_notice_s": ["Название ПримечаниеП"], "rc_currency_s": ["R/C Валюта"], "name_currency_s": ["Название Валюта"],
        #                 }
        #         sheet_name = "Настройка строк"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         rc_cols = [['r_key_s', 'c_key_s', 'rc_key_s'],
        #                    ['r_article_s', 'c_article_s', 'rc_article_s'],
        #                    ['r_brand_s', 'c_brand_s', 'rc_brand_s'],
        #                    ['r_name_s', 'c_name_s', 'rc_name_s'],
        #                    ['r_count_s', 'c_count_s', 'rc_count_s'],
        #                    ['r_price_s', 'c_price_s', 'rc_price_s'],
        #                    ['r_mult_s', 'c_mult_s', 'rc_mult_s'],
        #                    ['r_notice_s', 'c_notice_s', 'rc_notice_s'],
        #                    ['r_currency_s', 'c_currency_s', 'rc_currency_s'],
        #                    ]
        #         req = ''
        #         for r, c, rc in rc_cols:
        #             # req += (f"update file_settings set {r} = (regexp_split_to_array({rc}, '[RC]'))[2]::INTEGER, "
        #             #                 f"{c} = (regexp_split_to_array({rc}, '[RC]'))[3]::INTEGER where {rc} is not NULL;")
        #             req += (f"update file_settings set {r} = (regexp_split_to_array({rc}, '[RC]'))[2]::INTEGER, "
        #                     f"{c} = (regexp_split_to_array({rc}, '[RC]'))[3]::INTEGER where {rc} SIMILAR TO 'R[0-9]{{1,}}C[0-9]{{1,}}';")
        #         sess.execute(text(req))
        #         sess.query(FileSettings).filter(FileSettings.price_code == None).delete()
        #
        #
        #         table_name = 'supplier_price_settings'
        #         table_class = SupplierPriceSettings
        #         cols = {"supplier_code": ["Код поставщика"], "price_code": ["Код прайса"], "save": ["Сохраняем"],
        #                 "standard": ["Стандартизируем"], "calculate": ["Обрабатываем"], "buy": ["Можем купить?"],
        #                 "works": ["Работаем"], "wholesale": ["Прайс оптовый"],
        #                 "buy_for_working_capital": ["Закупка для оборотных средств"],
        #                 "is_base_price": ["Цену считать базовой"], "costs": ["Издержки"], "email": ["Почта"],
        #                 "file_name_cond": ["Условие имени файла"], "update_time": ["Срок обновление не более"],
        #                 "file_name": ["Имя файла"],
        #                 "in_price": ["В прайс"], "short_name": ["Краткое наименование"], "access_pp": ["Разрешения ПП"],
        #                 "supplier_lot": ["Лот поставщика"], "over_base_price": ["К.Превышения базовой цены"],
        #                 "convenient_lot": ["Лот удобный нам"], "min_markup": ["Наценка мин"],
        #                 "markup_wholesale": ["Наценка опт"],
        #                 "max_markup": ["Наценка макс"], "unload_percent": ["% Отгрузки"], "delay": ["Отсрочка"],
        #                 "markup_os": ["Наценка для ОС"],
        #                 "row_change_percent": ["Допустимый процент изменения количества строк"],
        #                 "price_change_percent": ["Допустимый процент изменения цены"],
        #                 "supplier_rating": ["Рейтинг поставщика"],
        #                 }
        #         sheet_name = "Настройка прайсов"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #         sess.query(SupplierPriceSettings).filter(SupplierPriceSettings.supplier_code == None).delete()
        #
        #         table_name = 'article_fix'
        #         table_class = ArticleFix
        #         cols = {"price_code": ["Код прайса"], "change_type": ["Вариант исправления"], "find": ["Найти"], "change": ["Установить"],}
        #         sheet_name = "Исправление Артикула"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #         # sess.query(ArticleFix).filter(ArticleFix.price_code == None).delete()
        #
        #         table_name = 'brands'
        #         table_class = Brands
        #         cols = {"correct_brand": ["Правильный Бренд"], "brand": ["Поиск"],}
        #         sheet_name = "Справочник Бренды"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         table_name = 'price_change'
        #         table_class = PriceChange
        #         cols = {"price_code": ["Код прайса"], "brand": ["Производитель поставщика"], "discount": ["Скидка, %"]}
        #         sheet_name = "Изменение цены по условиям"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         table_name = 'words_of_exception'
        #         table_class = WordsOfException
        #         cols = {"price_code": ["Код прайса"], "colunm_name": ["Столбец поиска"], "condition": ["Условие"], "text": ["Текст"]}
        #         sheet_name = "Слова_исключения"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         table_name = 'supplie_goods_fix'
        #         table_class = SupplierGoodsFix
        #         cols = {"supplier": ["Поставщик"], "import_setting": ["Настройка импорта прайса"], "key1": ["Ключ1"],
        #                 "article_s": ["Артикул поставщика"], "brand_s": ["Производитель поставщика"], "name": ["Наименование"],
        #                 "brand": ["Производитель"], "article": ["Артикул"], "price_s": ["Цена поставщика"],
        #                 "sales_ban": ["Запрет продажи"], "original": ["Оригинал"], "marketable_appearance": ["Товарный вид"],
        #                 "put_away_percent": ["Убрать %"], "put_away_count": ["Убрать шт"], "nomenclature": ["Номенклатура"],
        #                 "mult_s": ["Кратность поставщика"],
        #                 }
        #         sheet_name = "Исправление товаров поставщиков"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #
        #         sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name==base_name).delete()
        #         sess.add(CatalogUpdateTime(catalog_name=base_name, updated_at=new_update_time))
        #
        #         sess.commit()
        #
        #     self.log.add(LOG_ID, f"{base_name} обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]",
        #                  f"<span style='color:{green_log_color};font-weight:bold;'>{base_name}</span> обновлён "
        #                  f"[{str(datetime.datetime.now() - cur_time)[:7]}]")

        # def update_rc_price_settings(self):
        #     path_to_file = fr"{data['catalogs_dir']}\Справочник расположения столбцов и условий.xlsx"
        #     base_name = os.path.basename(path_to_file)
        #     new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_file))
        #     with session() as sess:
        #         req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name==base_name)
        #         res = sess.execute(req).scalar()
        #         if res and res >= new_update_time:
        #             return
        #         cur_time = datetime.datetime.now()
        #         self.log.add(LOG_ID, f"Обновление {base_name} ...",
        #                      f"Обновление <span style='color:{green_log_color};font-weight:bold;'>{base_name}</span> ...")
        #
        #         table_name = 'file_settings'
        #         table_class = FileSettings
        #         cols = {"price_code": ["Прайс"], "pass_up": ["Пропуск сверху"], "pass_down": ["Пропуск снизу"],
        #                 "compare": ["Сопоставление по"], "rc_key_s": ["R/C КлючП"], "name_key_s": ["Название КлючП"],
        #                 "rc_article_s": ["R/C АртикулП"], "name_article_s": ["Название АртикулП"], "rc_brand_s": ["R/C БрендП"],
        #                 "name_brand_s": ["Название БрендП"], "rc_name_s": ["R/C НаименованиеП"],
        #                 "name_name_s": ["Название НаименованиеП"], "rc_count_s": ["R/C КоличествоП"],
        #                 "name_count_s": ["Название КоличествоП"], "rc_price_s": ["R/C ЦенаП"], "name_price_s": ["Название ЦенаП"],
        #                 "rc_mult_s": ["R/C КратностьП"], "name_mult_s": ["Название КратностьП"], "rc_notice_s": ["R/C ПримечаниеП"],
        #                 "name_notice_s": ["Название ПримечаниеП"], "rc_currency_s": ["R/C Валюта"], "name_currency_s": ["Название Валюта"],
        #                 }
        #         update_catalog(sess, path_to_file, cols, table_name, table_class)
        #
        #         rc_cols = [['r_key_s', 'c_key_s', 'rc_key_s'],
        #                    ['r_article_s', 'c_article_s', 'rc_article_s'],
        #                    ['r_brand_s', 'c_brand_s', 'rc_brand_s'],
        #                    ['r_name_s', 'c_name_s', 'rc_name_s'],
        #                    ['r_count_s', 'c_count_s', 'rc_count_s'],
        #                    ['r_price_s', 'c_price_s', 'rc_price_s'],
        #                    ['r_mult_s', 'c_mult_s', 'rc_mult_s'],
        #                    ['r_notice_s', 'c_notice_s', 'rc_notice_s'],
        #                    ['r_currency_s', 'c_currency_s', 'rc_currency_s'],
        #                    ]
        #         req = ''
        #         for r, c, rc in rc_cols:
        #             req += (f"update file_settings set {r} = (regexp_split_to_array({rc}, '[RC]'))[2]::INTEGER, "
        #                             f"{c} = (regexp_split_to_array({rc}, '[RC]'))[3]::INTEGER where {rc} is not NULL;")
        #         sess.execute(text(req))
        #         sess.query(FileSettings).filter(FileSettings.price_code == None).delete()
        #
        #         sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name==base_name).delete()
        #         sess.add(CatalogUpdateTime(catalog_name=base_name, updated_at=new_update_time))
        #
        #         sess.commit()
        #
        #     self.log.add(LOG_ID, f"{base_name} обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]",
        #                  f"<span style='color:{green_log_color};font-weight:bold;'>{base_name}</span> обновлён "
        #                  f"[{str(datetime.datetime.now() - cur_time)[:7]}]")

        # def update_supplier_price_settings(self):
        #     path_to_file = fr"{data['catalogs_dir']}\Настройки прайса поставщика.xlsx"
        #     base_name = os.path.basename(path_to_file)
        #     new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_file))
        #     with session() as sess:
        #         req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name==base_name)
        #         res = sess.execute(req).scalar()
        #         if res and res >= new_update_time:
        #             return
        #         cur_time = datetime.datetime.now()
        #         self.log.add(LOG_ID, f"Обновление {base_name} ...",
        #                      f"Обновление <span style='color:{green_log_color};font-weight:bold;'>{base_name}</span> ...")
        #
        #         table_name = 'supplier_price_settings'
        #         table_class = SupplierPriceSettings
        #         cols = {"supplier_code": ["Код поставщика"], "price_code": ["Код прайса"], "save": ["Сохраняем"],
        #                 "standard": ["Стандартизируем"], "calculate": ["Обрабатываем"], "buy": ["Можем купить?"],
        #                 "works": ["Работаем"], "wholesale": ["Прайс оптовый"], "buy_for_working_capital": ["Закупка для оборотных средств"],
        #                 "is_base_price": ["Цену считать базовой"], "costs": ["Издержки"], "email": ["Почта"],
        #                 "file_name_cond": ["Условие имени файла"], "update_time": ["Срок обновление не более"], "file_name": ["Имя файла"],
        #                 "in_price": ["В прайс"], "short_name": ["Краткое наименование"], "access_pp": ["Разрешения ПП"],
        #                 "supplier_lot": ["Лот поставщика"], "over_base_price": ["К.Превышения базовой цены"],
        #                 "convenient_lot": ["Лот удобный нам"], "min_markup": ["Наценка мин"], "markup_wholesale": ["Наценка опт"],
        #                 "max_markup": ["Наценка макс"], "unload_percent": ["% Отгрузки"], "delay": ["Отсрочка"],
        #                 "markup_os": ["Наценка для ОС"], "row_change_percent": ["Допустимый процент изменения количества строк"],
        #                 "price_change_percent": ["Допустимый процент изменения цены"], "supplier_rating": ["Рейтинг поставщика"],
        #                 }
        #         sheet_name = "Справочник"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name==base_name).delete()
        #         sess.add(CatalogUpdateTime(catalog_name=base_name, updated_at=new_update_time))
        #
        #         sess.commit()
        #
        #     self.log.add(LOG_ID, f"{base_name} обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]",
        #                  f"<span style='color:{green_log_color};font-weight:bold;'>{base_name}</span> обновлён "
        #                  f"[{str(datetime.datetime.now() - cur_time)[:7]}]")

        # def update_3_0_condition(self):
        #     path_to_file = fr"{data['catalogs_dir']}\3.0 Условия.xlsx"
        #     base_name = os.path.basename(path_to_file)
        #     new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_file))
        #     with session() as sess:
        #         req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name==base_name)
        #         res = sess.execute(req).scalar()
        #         if res and res >= new_update_time:
        #             return
        #         cur_time = datetime.datetime.now()
        #         self.log.add(LOG_ID, f"Обновление {base_name} ...",
        #                      f"Обновление <span style='color:{green_log_color};font-weight:bold;'>{base_name}</span> ...")
        #
        #         table_name = 'data07_14'
        #         table_class = Data07_14
        #         cols = {"works": ["Работаем?"], "update_time": ["Период обновления не более"], "setting": ["Настройка"],
        #                 "max_decline": ["Макс снижение от базовой цены"], "correct": ["Правильное"],
        #                 "markup_pb": ["Наценка ПБ"], "code_pb_p": ["Код ПБ_П"]}
        #         sheet_name = "07&14Данные"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         table_name = 'data07'
        #         table_class = Data07
        #         cols = {"works": ["Работаем?"], "update_time": ["Период обновления не более"], "setting": ["Настройка"],
        #                 "delay": ["Отсрочка"], "sell_os": ["Продаём для ОС"], "markup_os": ["Наценка для ОС"],
        #                 "max_decline": ["Макс снижение от базовой цены"],
        #                 "markup_holidays": ["Наценка на праздники (1,02)"], "markup_R": ["Наценка Р"],
        #                 "min_markup": ["Мин наценка"], "markup_wholesale": ["Наценка на оптовые товары"],
        #                 "grad_step": ["Шаг градаци"],
        #                 "wholesale_step": ["Шаг опт"], "access_pp": ["Разрешения ПП"], "unload_percent": ["% Отгрузки"]}
        #         sheet_name = "07Данные"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         table_name = 'data15'
        #         table_class = Data15
        #         cols = {"code_15": ["15"], "offers_wholesale": ["Предложений опт"], "price_b": ["ЦенаБ"]}
        #         sheet_name = "15Данные"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         table_name = 'data09'
        #         table_class = Data09
        #         cols = {"put_away_zp": ["УбратьЗП"], "reserve_count": ["ШтР"], "code_09": ["09"]}
        #         sheet_name = "09Данные"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         table_name = 'buy_for_os'
        #         table_class = Buy_for_OS
        #         cols = {"buy_count": ["Количество закупок"], "article_producer": ["АртикулПроизводитель"]}
        #         sheet_name = "Закупки для ОС"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #         table_name = 'reserve'
        #         table_class = Reserve
        #         cols = {"code_09": ["09Код"], "reserve_count": ["ШтР"], "code_07": ["07Код"]}
        #         sheet_name = "Резерв_да"
        #         update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
        #
        #
        #         sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name==base_name).delete()
        #         sess.add(CatalogUpdateTime(catalog_name=base_name, updated_at=new_update_time))
        #
        #         sess.commit()
        #
        #     self.log.add(LOG_ID, f"{base_name} обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]",
        #                  f"<span style='color:{green_log_color};font-weight:bold;'>{base_name}</span> обновлён "
        #                  f"[{str(datetime.datetime.now() - cur_time)[:7]}]")

    # def update_catalog(ses, path_to_file, cols, table_name, table_class, sheet_name=0):
    #     '''for varchar(x), real, numeric, integer'''
    #     con = ses.connection()
    #     pk = []
    #     # берутся столбцы из таблицы: название столбца, максимальная длина его поля
    #     # with engine.connect() as sess:
    #     print(table_name)
    #     req = delete(table_class)
    #     con.execute(req)
    #     con.execute(text(f"ALTER SEQUENCE {table_name}_id_seq restart 1"))
    #     # sess.commit()
    #     res = con.execute(text(
    #         f"SELECT column_name, character_maximum_length FROM information_schema.columns WHERE table_name = '{table_name}' "
    #         f"and column_name != 'id'")).all()
    #     for i in res:
    #         if cols.get(i[0], None):
    #             cols[i[0]].append(i[1])
    #     res = con.execute(text(
    #         f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' and is_nullable = 'NO' "
    #         f"and column_name != 'id'")).all()
    #     pk = [i[0] for i in res]
    #
    #     df = pd.read_excel(path_to_file, usecols=[cols[c][0] for c in cols], na_filter=False,sheet_name=sheet_name)
    #     df = df.rename(columns={cols[c][0]: c for c in cols})
    #
    #     for c in cols:
    #         char_limit = cols[c][1]
    #         if char_limit:  # str
    #             df[c] = df[c].apply(lambda x: str(x)[:char_limit] or None)
    #         else:  # float/int
    #             df[c] = df[c].apply(to_float)
    #             # df[c] = df[c].replace('', 0)
    #             # df = df[df[c].apply(is_float)]
    #             # df[c] = np.float64(df[c])
    #         if c in pk:  # для PK
    #             df = df[df[c].notna()]
    #     # return (df)
    #     # print(df)
    #     df.to_sql(name=table_name, con=con, if_exists='append', index=False, index_label=False, chunksize=CHUNKSIZE)

    # def is_float(x):
    #     try:
    #         x = float(str(x).replace(',', '.'))
    #         if math.isnan(x) or math.isinf(x):
    #             return False
    #         if 1E+37 < x < 1E-37:  # real
    #             return False
    #         return True
    #     except:
    #         return False
    #
    # def to_float(x):
    #     try:
    #         x = float(str(x).replace(',', '.'))
    #         if math.isnan(x) or math.isinf(x):
    #             return 0
    #         if 1E+37 < x < 1E-37:  # real
    #             return 0
    #         return x
    #     except:
    #         return 0