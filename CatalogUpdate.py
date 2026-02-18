from PySide6.QtCore import QThread, Signal
import time
import math
import datetime
import traceback
import requests
import os
import shutil
import pandas as pd
import openpyxl
from sqlalchemy import text, select, delete, insert, update, Sequence, func, and_, or_, distinct, case, cast, REAL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
from models import (Base, BasePrice, MassOffers, MailReport, CatalogUpdateTime, SupplierPriceSettings, FileSettings,
                    ColsFix, Brands, SupplierGoodsFix, AppSettings, ExchangeRate, Data07, BuyersForm, PriceException,
                    SaleDK, Data07_14, Data15, Data09, Buy_for_OS, Reserve, TotalPrice_1, TotalPrice_2, PriceReport,
                    Brands_3, SuppliersForm, FinalPriceHistory, Orders, PriceSendTime, FinalPriceHistoryDel, PriceSendTimeHistory,
                    MailReportUnloaded, CrossBrandTypeMarkupPct)
from telebot import TeleBot
from tg_users_id import USERS, TG_TOKEN
import colors

import setting
engine = setting.get_engine()
session = sessionmaker(engine)
settings_data = setting.get_vars()

tg_bot = TeleBot(TG_TOKEN)

PARAM_LIST = ["base_price_update", "mass_offers_update"]
CHUNKSIZE = int(settings_data["chunk_size"])
LOG_ID = 2

class CatalogUpdate(QThread):
    SetButtonEnabledSignal = Signal(bool)
    StartTablesUpdateSignal = Signal(bool)
    CreateBasePriceSignal = Signal(bool)
    CreateMassOffersSignal = Signal(bool)
    # CreateTotalCsvSignal = Signal(bool)

    isPause = None
    del_history_day = (datetime.datetime.now() - datetime.timedelta(days=1)).day

    def __init__(self, log=None, parent=None):
        self.log = log
        self.CBP = CreateBasePrice(log=log)
        self.CMO = CreateMassOffers(log=log)
        # self.CTC = CreateTotalCsv(log=log)
        QThread.__init__(self, parent)

    def run(self):
        global session, engine
        wait_sec = 30
        last_update_h = None
        self.SetButtonEnabledSignal.emit(False)
        while not self.isPause:
            start_cycle_time = datetime.datetime.now()
            try:
                # self.update_DB_3()
                # self.update_price_settings_catalog_4_0()
                # self.update_price_settings_catalog_3_0()
                # self.update_price_settings_catalog_4_0_cond()
                # return
                # with session() as sess:
                #     # engine.autocommit = True
                #     cur_time = datetime.datetime.now()
                #     self.log.add(LOG_ID, f"vacuum ...",
                #                  f"<span style='color:{colors.green_log_color};font-weight:bold;'>vacuum</span> ... ")
                #     sess.execute(text('VACUUM FULL'))
                #     self.log.add(LOG_ID, f"vacuum завершен [{str(datetime.datetime.now() - cur_time)[:7]}]",
                #                  f"<span style='color:{colors.green_log_color};font-weight:bold;'>vacuum</span> завершен "
                #                  f"[{str(datetime.datetime.now() - cur_time)[:7]}]")
                    # sess.commit()
                #     working_prices = sess.execute(select(distinct(SupplierPriceSettings.price_code)).where(
                #         func.upper(SupplierPriceSettings.works) == 'ДА')).scalars().all()
                #     sess.query(PriceReport).where(PriceReport.price_code.not_in(working_prices)).delete()
                # return
                self.update_orders_table()
                self.send_tg_notification()
                self.update_currency()
                self.update_price_settings_catalog_4_0()
                self.update_price_settings_catalog_4_0_cond()
                if self.update_price_settings_catalog_3_0():
                    # self.CreateTotalCsvSignal.emit(True)
                    # self.CTC.start()
                    # self.CTC.wait()
                    pass

                # + удаление из final_price_history и удаление неактуальных прайсов
                if self.update_DB_3():
                    pass
                    # self.CTC.start()
                    # self.CTC.wait()

                self.update_mass_offers()
                self.CMO.wait()
                self.update_base_price()
                self.CBP.wait()

                if datetime.datetime.now().hour != last_update_h:
                    last_update_h = datetime.datetime.now().hour
                    self.StartTablesUpdateSignal.emit(1)
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
                now = datetime.datetime.now()  #.strftime("%Y-%m-%d %H:%M:%S")
                req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name == 'Курс валют')
                res = sess.execute(req).scalar()

                if res:
                    if now.strftime("%Y-%m-%d") == res.strftime("%Y-%m-%d"):
                        return

                self.log.add(LOG_ID, "Обновление курса валют", f"Обновление <span style='color:{colors.green_log_color};font-weight:bold;'>курса валют</span>...")
                sess.query(ExchangeRate).delete()
                valute_data = requests.get('https://www.cbr-xml-daily.ru/daily_json.js', timeout=100).json()
                valute_dict = dict()
                for c in valute_data['Valute']:
                    info = valute_data['Valute'][c]
                    valute_dict[info["CharCode"]] = info["Value"] / info["Nominal"]

                df = pd.DataFrame.from_dict(valute_dict, orient='index')
                df = df.reset_index()
                df = df.rename(columns={"index": "code", 0: "rate"})
                df.to_sql(name='exchange_rate', con=sess.connection(), if_exists='append', index=False)

                sess.query(CatalogUpdateTime).where(CatalogUpdateTime.catalog_name == 'Курс валют').delete()
                sess.add(CatalogUpdateTime(catalog_name='Курс валют', updated_at=now.strftime("%Y-%m-%d %H:%M:%S")))

                sess.execute(update(TotalPrice_1).where(and_(TotalPrice_1.currency_s != None,
                                                             ExchangeRate.code == func.upper(TotalPrice_1.currency_s)))
                             .values(_05price=TotalPrice_1.price_s * ExchangeRate.rate))
                sess.execute(update(TotalPrice_2).where(and_(TotalPrice_2.currency_s != None,
                                                             ExchangeRate.code == func.upper(TotalPrice_2.currency_s)))
                             .values(_05price=TotalPrice_2.price_s * ExchangeRate.rate))

                discounts = sess.execute(select(ColsFix).where(ColsFix.col_change.in_(['05Цена', 'Цена поставщика']))).scalars().all()

                for dscnt in discounts:
                    if not isinstance(dscnt.set, (float, int)):
                        continue
                    add = 1 + float(dscnt.set)
                    # print(dscnt.set, (1 + float(dscnt.set)), dscnt.col_change, dscnt.find)
                    sess.execute(update(TotalPrice_1).where(and_(TotalPrice_1._07supplier_code == dscnt.price_code,
                                                                 TotalPrice_1.currency_s != None,
                                                                 TotalPrice_1._14brand_filled_in == dscnt.find)).values(
                        {'_05price': TotalPrice_1._05price * add}))
                        # {price_cols[dscnt.col_change].__dict__['name']: price_cols[dscnt.col_change] * (1 + float(dscnt.set))})) # price_cols[dscnt.col_change]

                    sess.execute(update(TotalPrice_2).where(and_(TotalPrice_2._07supplier_code == dscnt.price_code,
                                                                 TotalPrice_2.currency_s != None,
                                                                 TotalPrice_2._14brand_filled_in == dscnt.find)).values(
                        {'_05price': TotalPrice_2._05price * add}))
                        # {price_cols2[dscnt.col_change].__dict__['name']: price_cols2[dscnt.col_change] * (1 + float(dscnt.set))})) # price_cols2[dscnt.col_change]

                sess.execute(update(TotalPrice_2).where(TotalPrice_2.currency_s != None).values(_05price_plus=None))
                sess.execute(update(TotalPrice_2).where(and_(TotalPrice_2._05price_plus == None, TotalPrice_2._04count > 0,
                                                             TotalPrice_2.markup_holidays > TotalPrice_2._05price * TotalPrice_2._04count)).
                             values(_05price_plus=TotalPrice_2.markup_holidays / TotalPrice_2._04count))
                sess.execute(update(TotalPrice_2).where(TotalPrice_2._05price_plus == None).values(_05price_plus=TotalPrice_2._05price))
                # для пересчёта прайсов, где указана валюта
                # prices_with_curr = sess.execute(select(distinct(TotalPrice_1._07supplier_code)).
                #                                 where(TotalPrice_1.currency_s != None)).scalars().all()
                # sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code.in_(prices_with_curr)).delete()
                # sess.execute(update(PriceReport).where(PriceReport.price_code.in_(prices_with_curr)).values(updated_at_2_step=None))

                sess.commit()

                self.log.add(LOG_ID, "Курс валют обновлён", f"<span style='color:{colors.green_log_color};font-weight:bold;'>Курс валют</span> обновлён")

        except (OperationalError, UnboundExecutionError) as db_ex:
            raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "update_currency Error", ex_text)


    def send_tg_notification(self):
        try:
            with session() as sess:
                req = select(AppSettings).where(AppSettings.param == 'last_tg_notification_time')
                last_tg_nt = sess.execute(req).scalar()
                req = select(AppSettings).where(AppSettings.param == 'tg_notification_time')
                tg_nt_time = sess.execute(req).scalar()
                h, m = tg_nt_time.var.split()

                now = datetime.datetime.now()
                last_tg_nt = datetime.datetime.strptime(f"{last_tg_nt.var[:10]} {h}:{m}:00",
                                                        "%Y-%m-%d %H:%M:%S")
                # print(last_tg_nt)
                diff = now - last_tg_nt
                if diff.days >= 1:
                    msg = ''

                    miss_brands_prices = sess.execute(select(distinct(PriceSendTime.price_code)).where(PriceSendTime.info_msg=='Не указаны бренды').
                                                          order_by(PriceSendTime.price_code)).scalars().all()
                    if miss_brands_prices:
                        miss_brands_prices = ', '.join(miss_brands_prices)
                        msg += f"📧 Не указаны бренды: {miss_brands_prices}\n\n"

                    zero_count = sess.execute(select(distinct(PriceSendTime.price_code)).where(PriceSendTime.info_msg=='Итоговое кол-во 0, не отправлен').
                                                          order_by(PriceSendTime.price_code)).scalars().all()
                    if zero_count:
                        zero_count = ', '.join(zero_count)
                        msg += f"📧 Итоговое кол-во 0: {zero_count}\n\n"

                    miss_email = sess.execute(select(distinct(PriceSendTime.price_code)).where(and_(PriceSendTime.info_msg=='Не отправлено, почта для отправки не указана',
                                                                                                    PriceSendTime.price_code == BuyersForm.buyer_price_code,
                                                                                                    func.upper(BuyersForm.for_send) == 'ДА')).
                                                          order_by(PriceSendTime.price_code)).scalars().all()
                    if miss_email:
                        miss_email = ', '.join(miss_email)
                        msg += f"📧 Почта для отправки не указана: {miss_email}\n\n"

                    miss_4_settings_prices = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message != None,
                                  PriceReport.info_message.contains('Нет в условиях'))).order_by(PriceReport.price_code)).scalars().all()
                    if miss_4_settings_prices:
                        miss_4_settings_prices = ', '.join(miss_4_settings_prices)
                        msg += f"Нет в условиях (4.0 - Настройка прайсов): {miss_4_settings_prices}\n\n"

                    not_standarted = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message != None,
                                  PriceReport.info_message=="Не указана стандартизация")).order_by(PriceReport.price_code)).scalars().all()
                    if not_standarted:
                        not_standarted = ', '.join(not_standarted)
                        msg += f"Не указана стандартизация: {not_standarted}\n\n"

                    not_save = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message != None,
                                  PriceReport.info_message=="Не указано сохранение")).order_by(PriceReport.price_code)).scalars().all()
                    if not_save:
                        not_save = ', '.join(not_save)
                        msg += f"Не указано сохранение: {not_save}\n\n"

                    update_times = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message != None,
                                  PriceReport.info_message=="Не подходит по сроку обновления")).order_by(PriceReport.price_code)).scalars().all()
                    if update_times:
                        update_times = ', '.join(update_times)
                        msg += f"Не подходит по сроку обновления: {update_times}\n\n"

                    miss_4_settings_cols = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message != None,
                                  PriceReport.info_message=="Нет настроек")).order_by(PriceReport.price_code)).scalars().all()
                    if miss_4_settings_cols:
                        miss_4_settings_cols = ', '.join(miss_4_settings_cols)
                        msg += f"Нет в условиях (4.0 - Настройка строк): {miss_4_settings_cols}\n\n"

                    cols_uncomp = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message != None,
                                  PriceReport.info_message.contains("Нет подходящих настроек столбцов"))).order_by(PriceReport.price_code)).scalars().all()
                    if cols_uncomp:
                        cols_uncomp = ', '.join(cols_uncomp)
                        msg += f"Нет подходящих настроек столбцов: {cols_uncomp}\n\n"

                    format_problem = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message != None,
                                  PriceReport.info_message=="Ошибка формата")).order_by(PriceReport.price_code)).scalars().all()
                    if format_problem:
                        format_problem = ', '.join(format_problem)
                        msg += f"Нестандартный формат входящих прайсов: {format_problem}\n\n"

                    cols_uncomp_2 = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message != None,
                                  PriceReport.info_message=="Настройки столбцов не подошли")).order_by(PriceReport.price_code)).scalars().all()
                    if cols_uncomp_2:
                        cols_uncomp_2 = ', '.join(cols_uncomp_2)
                        msg += f"Настройки столбцов не подошли: {cols_uncomp_2}\n\n"

                    miss_07data = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message2 != None,
                                  PriceReport.info_message2=="Нет настроек или срока обновления в 07Данные")).order_by(PriceReport.price_code)).scalars().all()
                    if miss_07data:
                        miss_07data = ', '.join(miss_07data)
                        msg += f"Нет настроек или срока обновления в 07Данные: {miss_07data}\n\n"

                    update_times_07data = sess.execute(select(distinct(PriceReport.price_code)).where(and_(PriceReport.info_message2 != None,
                                  PriceReport.info_message2=="Не подходит период обновления")).order_by(PriceReport.price_code)).scalars().all()
                    if update_times_07data:
                        update_times_07data = ', '.join(update_times_07data)
                        msg += f"Не подходит период обновления из 07Данные: {update_times_07data}\n\n"


                    total_cnt = sess.execute(select(func.count()).select_from(TotalPrice_2)).scalar()
                    total_cnt = '{0:,d}'.format(total_cnt)
                    msg += f'Всего позиций: {total_cnt}'

                    for u in USERS:
                        tg_bot.send_message(chat_id=u, text=msg, parse_mode='HTML')
                    # print(msg)
                    self.log.add(LOG_ID, "Уведомление отправлено", f"<span style='color:{colors.green_log_color};font-weight:bold;'>Уведомление отправлено</span>  ")

                    sess.query(AppSettings).where(AppSettings.param == 'last_tg_notification_time').delete()
                    now = now.strftime("%Y-%m-%d %H:%M:%S")
                    sess.add(AppSettings(param='last_tg_notification_time', var=f'{now}'))
                    sess.commit()

        except (OperationalError, UnboundExecutionError) as db_ex:
            raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "send_tg_notification Error", ex_text)


    def update_price_settings_catalog_4_0(self):
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
                cols = {"price_code": ["Прайс"], "parent_code": ["Прайс родитель"], "save": ["Сохраняем"], "email": ["Почта"], "file_name_cond": ["Условие имени файла"],
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
                sess.execute(update(FileSettings).values(email=FileSettings.email.regexp_replace(' ', '', 'g')))

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
                    req += (f"update file_settings set {r} = (regexp_split_to_array({rc}, '[RC]'))[2]::INTEGER, "
                            f"{c} = (regexp_split_to_array({rc}, '[RC]'))[3]::INTEGER where {rc} SIMILAR TO 'R[0-9]{{1,}}C[0-9]{{1,}}';")
                sess.execute(text(req))
                sess.query(FileSettings).filter(FileSettings.price_code == None).delete()

                table_name = 'supplier_price_settings'
                table_class = SupplierPriceSettings
                cols = {"supplier_code": ["Код поставщика"], "price_code": ["Код прайса"],
                        "standard": ["Стандартизируем"], "calculate": ["Обрабатываем"], "buy": ["Можем купить?"],
                        "works": ["Работаем"], "wholesale": ["Прайс оптовый"],
                        "buy_for_working_capital": ["Закупка для оборотных средств"],
                        "is_base_price": ["Цену считать базовой"], "costs": ["Издержки"], "update_time_str": ["Срок обновление не более"],
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
                sess.execute(update(SupplierPriceSettings).values(update_time=cast(func.regexp_substr(SupplierPriceSettings.update_time_str, r'\d+'), REAL)))

                table_name = 'cols_fix'
                table_class = ColsFix
                cols = {"price_code": ["Код прайса"], "col_find": ["Столбец поиска"], "find": ["Найти"],
                        "change_type": ["Вариант исправления"], "col_change": ["Столбец исправления"], "set": ["Установить"]}
                sheet_name = "ИсправНомПоУсл"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
                sess.execute(update(ColsFix).values(find=func.upper(ColsFix.find)))

                table_name = 'brands'
                table_class = Brands
                cols = {"correct_brand": ["Правильный Бренд"], "brand": ["Поиск"],
                        "mass_offers": ["Для подсчёта предложений в опте"], "base_price": ["Для базовой цены"], }
                sheet_name = "Справочник Бренды"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
                sess.execute(update(Brands).values(brand_low=func.lower(Brands.brand.regexp_replace(r'\W', '', 'g'))))

                table_name = 'price_exception'
                table_class = PriceException
                cols = {"price_code": ["Код прайса"], "condition": ["Условие"], "find": ["Столбец поиска"],
                        "text": ["Текст"], "deny": ["Куда запрещено"], "extra": ["Примечание"], }
                sheet_name = "Исключения из прайсов"
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
                        "mult_s": ["Кратность поставщика"], "name_s": ["Наименование поставщика"]
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
            self.log.error(LOG_ID, f"update_price_settings_catalog_4_0 Error", ex_text)


    def update_price_settings_catalog_3_0(self):
        try:
            path_to_file = fr"{settings_data['3_cond_dir']}\3.0 Условия.xlsx"
            base_name = os.path.basename(path_to_file)
            new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_file)).strftime("%Y-%m-%d %H:%M:%S")
            with session() as sess:
                req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name == base_name)
                last_time_update = sess.execute(req).scalar()

                if last_time_update and str(last_time_update) >= new_update_time:
                    return
                cur_time = datetime.datetime.now()
                self.log.add(LOG_ID, f"Обновление {base_name} ...",
                             f"Обновление <span style='color:{colors.green_log_color};font-weight:bold;'>{base_name}</span> ...")

                table_name = 'data07_14'
                table_class = Data07_14
                cols = {"works": ["Работаем?"], "update_time": ["Период обновления не более"], "setting": ["Настройка"],
                        "max_decline": ["Макс снижение от базовой цены"], "correct": ["Правильное"],
                        "markup_pb": ["Наценка ПБ"], "code_pb_p": ["Код ПБ_П"]}
                sheet_name = "07&14Данные"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
                sess.execute(update(Data07_14).values(correct=func.upper(Data07_14.correct)))

                table_name = 'data07'
                table_class = Data07
                cols = {"works": ["Работаем?"], "update_time": ["Период обновления не более"], "setting": ["Настройка"],
                        "to_price": ["В прайс"], "delay": ["Отсрочка"], "sell_os": ["Продаём для ОС"], "markup_os": ["Наценка для ОС"],
                        "max_decline": ["Макс снижение от базовой цены"],
                        "markup_holidays": ["Наценка на праздники (1,02)"], "markup_R": ["Наценка Р"],
                        "min_markup": ["Мин наценка"], "min_wholesale_markup": ["Мин опт наценка"],
                        "markup_wholesale": ["Наценка на оптовые товары"], "grad_step": ["Шаг градации"],
                        "wholesale_step": ["Шаг опт"], "access_pp": ["Разрешения ПП"], "unload_percent": ["% Отгрузки"]}
                sheet_name = "07Данные"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                # table_name = 'data15'
                # table_class = Data15
                # cols = {"code_15": ["15"], "offers_wholesale": ["Предложений опт"], "price_b": ["ЦенаБ"]}
                # sheet_name = "15Данные"
                # update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                table_name = 'data09'
                table_class = Data09
                cols = {"put_away_zp": ["УбратьЗП"], "reserve_count": ["ШтР"], "code_09": ["09"]}
                sheet_name = "09Данные"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                table_name = 'buy_for_os'
                table_class = Buy_for_OS
                cols = {"buy_count": ["Количество закупок"], "article_producer": ["АртикулПроизводитель"]}
                sheet_name = "Закупки для ОС"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                table_name = 'reserve'
                table_class = Reserve
                cols = {"code_09": ["09Код"], "reserve_count": ["ШтР"], "code_07": ["07Код"]}
                sheet_name = "Резерв_да"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                table_name = 'sale_dk'
                table_class = SaleDK
                cols = {"price_code": ["Код покупателя / прайса поставщика"], "agr": ["Атрибут"], "val": ["Значение"]}
                sheet_name = "СкидкиДК"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                table_name = 'brands_3'
                table_class = Brands_3
                cols = {"correct": ["Сюда правильное"], "zp_brands_setting": ["Настройка ЗП и Брендов"], "brand": ["Бренд"],
                        "short_name": ["Короткое наименование бренда"], }
                sheet_name = "Справочник_Бренд3"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
                # unique rows
                uniques_rows_id = select(func.max(Brands_3.id)).group_by(Brands_3.correct, Brands_3.zp_brands_setting)
                sess.query(Brands_3).where(Brands_3.id.not_in(uniques_rows_id)).delete()

                table_name = 'suppliers_form'
                table_class = SuppliersForm
                cols = {"rating": ["Рейтинг поставщика"], "setting": ["Настройка"], "days": ["Дни трансляции"], }
                sheet_name = "Анкета поставщика"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                table_name = 'buyers_form'
                table_class = BuyersForm
                # "us_set": ["Установить УС"],
                cols = {"name": ["Наименование"], "name2": ["Наименование2"], "buyer_code": ["Код покупателя"],
                        "price_name": ["Имя прайса"], "file_name": ["Имя файла"], "file_extension": ["Расширение файла"],
                        "buyer_price_code": ["Код прайса покупателя"], "main_price": ["Основной прайс"],
                        "zp_brands_setting": ["Настройка ЗП и Брендов"], "included": ["Включен?"],
                        "period": ["Срок"], "us_buyer_req": ["УС по требованиям покупателя"], "us_current": ["УС текущий"],
                        "us_was": ["УС была"], "us_change": ["УС Изменения"], "us_above": ["Уровень сервиса не ниже"],
                        "vp_dynamic": ["Динамика ВП"], "val_dynamic": ["Динамика Вал"],
                        "d_val_was": ["Д Вал была"], "d_change": ["Д изменения"], "rise_markup": ["Доп наценка рост"],
                        "costs": ["Издержки"], "final_markup": ["Итоговая наценка"],
                        "markup_buyer_wh": ["Наценка покупателя опт"], "name_check": ["Прохождение наименования"],
                        "short_name": ["Короткое наименование"], "delay": ["Отсрочка дней"],
                        "kb_price": ["КБ цены"], "percent": ["Проценты за период"], "max_rows": ["Максимум строк"],
                        "max_rise": ["Максимальный рост"], "max_fall": ["Максимальное снижение"],
                        "quality_markup": ["Наценка качество приёма товара"], "sell_for_kos": ["Продаём для К.ОС"],
                        "kos_markup": ["Наценка для К.ОС"], "emails": ["Адрес для прайсов"], "send_days": ["Дни отправки"],
                        "time1": ["Время 1"], "time2": ["Время 2"], "time3": ["Время 3"], "time4": ["Время 4"],
                        "time5": ["Время 5"], "time6": ["Время 6"], "for_send": ["Рассылка"], "col_1": ["1 Столбец в прайсе"],
                        "col_2": ["2 Столбец в прайсе"], "col_3": ["3 Столбец в прайсе"], "col_4": ["4 Столбец в прайсе"],
                        "col_5": ["5 Столбец в прайсе"], "col_6": ["6 Столбец в прайсе"], "col_7": ["7 Столбец в прайсе"],
                        }
                sheet_name = "Анкета покупателя"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

                # sess.execute(update(TotalPrice_2).values(count=TotalPrice_2._04count))
                # sess.execute(update(TotalPrice_2).where(TotalPrice_2.reserve_count > 0).values(count=TotalPrice_2._04count - TotalPrice_2.reserve_count))
                # sess.execute(update(TotalPrice_2).values(mult_less=None))
                # sess.execute(update(TotalPrice_2).where(TotalPrice_2.count < TotalPrice_2._06mult_new).values(mult_less='-'))
                sess.commit()

                sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == base_name).delete()
                sess.add(CatalogUpdateTime(catalog_name=base_name, updated_at=new_update_time))

                sess.commit()
                self.log.add(LOG_ID, f"{base_name} обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]",
                             f"<span style='color:{colors.green_log_color};font-weight:bold;'>{base_name}</span> обновлён "
                             f"[{str(datetime.datetime.now() - cur_time)[:7]}]")
                return 1

        except (OperationalError, UnboundExecutionError) as db_ex:
            raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"update_price_settings_catalog_3_0 Error", ex_text)
        return None

    def update_price_settings_catalog_4_0_cond(self):
        try:
            path_to_file = fr"{settings_data['3_cond_dir']}\4.0 Условия.xlsx"
            base_name = os.path.basename(path_to_file)
            new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_file)).strftime("%Y-%m-%d %H:%M:%S")
            with session() as sess:
                req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name == base_name)
                last_time_update = sess.execute(req).scalar()

                if last_time_update and str(last_time_update) >= new_update_time:
                    return
                cur_time = datetime.datetime.now()
                self.log.add(LOG_ID, f"Обновление {base_name} ...",
                             f"Обновление <span style='color:{colors.green_log_color};font-weight:bold;'>{base_name}</span> ...")

                table_name = 'cross_brand_type_markup_pct'
                table_class = CrossBrandTypeMarkupPct
                cols = {"supplier_price_code": ["supplier_price_code"], "normalized_brand": ["normalized_brand"],
                        "customer_price_code": ["customer_price_code"], "short_name": ["short_name"],
                        "customer_brand": ["customer_brand"], "floor_markup_pct": ["floor_markup_pct"],
                        "starting_markup_pct": ["starting_markup_pct"], "grad_step_pct": ["grad_step_pct"],
                        "unique_starting_markup_pct": ["unique_starting_markup_pct"],
                        "opt_starting_markup_pct": ["opt_starting_markup_pct"], "unique_grad_step_pct": ["unique_grad_step_pct"],
                        "opt_grad_step_pct": ["opt_grad_step_pct"], }
                sheet_name = "Разрешения и наценки"
                update_catalog(sess, path_to_file, cols, table_name, table_class, sheet_name=sheet_name)
                sess.execute(update(CrossBrandTypeMarkupPct).values(short_name=func.upper(CrossBrandTypeMarkupPct.short_name),
                                                                    normalized_brand=func.upper(CrossBrandTypeMarkupPct.normalized_brand)))


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
            self.log.error(LOG_ID, f"update_price_settings_catalog_4_0_cond Error", ex_text)

    def update_DB_3(self):
        with session() as sess:
            cur_time = datetime.datetime.now()
            if cur_time.hour > 8:
                return

            last_3_condition_update = sess.execute(select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name == '3.0 Условия.xlsx')).scalar()
            last_DB_3_update = sess.execute(select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name == 'Обновление данных в БД по 3.0')).scalar()


            last_DB_3_update_HM = sess.execute(select(AppSettings.var).where(AppSettings.param == "last_DB_3_update")).scalar()
            h, m = last_DB_3_update_HM.split()

            compare_time = datetime.datetime.strptime(f"{str(last_DB_3_update)[:10]} {h}:{m}:00", "%Y-%m-%d %H:%M:%S")
            if (cur_time - compare_time).days < 1:
                return

            # удалить позиции старше 14 дней + неактуальные прайсы
            if self.del_history_day != cur_time.day:
                self.del_history_day = cur_time.day
                cur_time = datetime.datetime.now()
                dels = sess.query(FinalPriceHistory).where(or_(FinalPriceHistory.send_time < cur_time - datetime.timedelta(days=5),
                                                    FinalPriceHistory.send_time == None)).delete()
                if dels:
                    total_fph_rows = sess.execute(func.count(FinalPriceHistory.id)).scalar()
                    self.log.add(LOG_ID, f"Удалено строк из истории: {dels} [{str(datetime.datetime.now() - cur_time)[:7]}]. Всего строк: {total_fph_rows}",
                                     f"Удалено строк из <span style='color:{colors.green_log_color};font-weight:bold;'>истории</span>: "
                                     f"{dels} [{str(datetime.datetime.now() - cur_time)[:7]}]. Всего строк: {total_fph_rows}")

                cur_time = datetime.datetime.now()
                delsD = sess.query(FinalPriceHistoryDel).where(or_(FinalPriceHistoryDel.send_time < cur_time - datetime.timedelta(days=3),
                                                    FinalPriceHistoryDel.send_time == None)).delete()
                if delsD:
                    total_fphd_rows = sess.execute(func.count(FinalPriceHistoryDel.id)).scalar()
                    self.log.add(LOG_ID, f"Удалено строк из истории удалённых строк: {delsD} [{str(datetime.datetime.now() - cur_time)[:7]}]. Всего строк: {total_fphd_rows}",
                                     f"Удалено строк из <span style='color:{colors.green_log_color};font-weight:bold;'>истории удалённых строк</span>: "
                                     f"{delsD} [{str(datetime.datetime.now() - cur_time)[:7]}]. Всего строк: {total_fphd_rows}")

                cur_time = datetime.datetime.now()
                delsPST = sess.query(PriceSendTimeHistory).where(PriceSendTimeHistory.update_time < cur_time - datetime.timedelta(days=62)).delete()
                if delsPST:
                    self.log.add(LOG_ID, f"Удалено строк из отчёта по отправленым прайсам: {delsPST} [{str(datetime.datetime.now() - cur_time)[:7]}]",
                                     f"Удалено строк из <span style='color:{colors.green_log_color};font-weight:bold;'>отчёта по отправленым прайсам</span>: "
                                     f"<span style='color:{colors.orange_log_color};font-weight:bold;'>{delsPST}</span> [{str(datetime.datetime.now() - cur_time)[:7]}]")

                sess.query(MailReportUnloaded).where(MailReportUnloaded.date < cur_time - datetime.timedelta(days=62))

            # проверка неактуальных прайсов
            loaded_prices = set(sess.execute(select(distinct(TotalPrice_2._07supplier_code))).scalars().all())
            actual_prices = set(sess.execute(select(SupplierPriceSettings.price_code).where(and_(SupplierPriceSettings.calculate == 'ДА',
                                                                                                 SupplierPriceSettings.works == 'ДА'))).scalars().all())
            useless_prices = (loaded_prices - actual_prices)
            # print(useless_prices)
            if useless_prices:
                self.log.add(LOG_ID, f"Не обрабатываем или не работаем: {useless_prices}")
                dels = sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code.in_(useless_prices)).delete()
                sess.query(TotalPrice_1).where(TotalPrice_1._07supplier_code.in_(useless_prices)).delete()
                sess.execute(update(PriceReport).where(PriceReport.price_code.in_(useless_prices)).values(
                    info_message="Не обрабатываем или не работаем", info_message2=None))
                self.log.add(LOG_ID, f"Удалено строк (Обрабаытваем, Работаем): {dels}",
                             f"Удалено строк (Обрабаытваем, Работаем): <span style='color:{colors.orange_log_color};font-weight:bold;'>{dels}</span> ")

            expired_prices = set(sess.execute(select(PriceReport.price_code).where(
                and_(SupplierPriceSettings.price_code == PriceReport.price_code, PriceReport.updated_at_2_step is not None,
                     SupplierPriceSettings.update_time > 0,
                     PriceReport.updated_at_2_step < func.now() - SupplierPriceSettings.update_time * text("interval '1 day'")))).scalars().all())
            if expired_prices:
                self.log.add(LOG_ID, f"Не подходят по сроку обновления: {expired_prices}")
                dels = sess.query(TotalPrice_2).where(TotalPrice_2._07supplier_code.in_(expired_prices)).delete()
                sess.query(TotalPrice_1).where(TotalPrice_1._07supplier_code.in_(expired_prices)).delete()
                sess.execute(update(PriceReport).where(PriceReport.price_code.in_(expired_prices)).values(info_message="Не подходит по сроку обновления", info_message2=None))
                self.log.add(LOG_ID, f"Удалено строк (Срок обновления не более): {dels}",
                             f"Удалено строк (Срок обновления не более): <span style='color:{colors.orange_log_color};font-weight:bold;'>{dels}</span> ")

            working_prices = sess.execute(select(distinct(SupplierPriceSettings.price_code)).where(func.upper(SupplierPriceSettings.works)=='ДА')).scalars().all()
            sess.query(PriceReport).where(PriceReport.price_code.not_in(working_prices)).delete()
            sess.execute(update(PriceReport).where(or_(PriceReport.info_message != 'Ок', PriceReport.info_message2 != 'Ок',
                                                       PriceReport.price_code.in_(useless_prices.union(expired_prices)))).values(updated_at=None))
            sess.commit()



            if last_3_condition_update and last_3_condition_update <= last_DB_3_update:
                sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == 'Обновление данных в БД по 3.0').delete()
                sess.add(CatalogUpdateTime(catalog_name='Обновление данных в БД по 3.0', updated_at=cur_time.strftime("%Y-%m-%d %H:%M:%S")))

                sess.commit()
                return

            # Обновление данных в total по новым 3.0 Условия
            self.log.add(LOG_ID, f"Обновление данных в Итоговом прайсе...",
                         f"Обновление данных в <span style='color:{colors.green_log_color};font-weight:bold;'>Итоговом прайсе</span> ...")
            cur_time = datetime.datetime.now()
            sess.execute(update(TotalPrice_2).values(delay=Data07.delay, to_price=Data07.to_price, sell_for_OS=Data07.sell_os,
                                                markup_os=Data07.markup_os, max_decline=Data07.max_decline,
                                                markup_holidays=Data07.markup_holidays,
                                                markup_R=Data07.markup_R, min_markup=Data07.min_markup,
                                                min_wholesale_markup=Data07.min_wholesale_markup,
                                                markup_wh_goods=Data07.markup_wholesale,
                                                grad_step=Data07.grad_step, wh_step=Data07.wholesale_step,
                                                access_pp=Data07.access_pp,
                                                unload_percent=Data07.unload_percent).where(TotalPrice_2._07supplier_code == Data07.setting))

            sess.execute(update(TotalPrice_2).where(TotalPrice_2._09code_supl_goods == Data09.code_09).
                         values(put_away_zp=Data09.put_away_zp, reserve_count=Data09.reserve_count))
            # вычет ШтР
            sess.execute(update(TotalPrice_2).values(count=TotalPrice_2._04count))
            sess.execute(update(TotalPrice_2).where(TotalPrice_2.reserve_count > 0).values(count=TotalPrice_2._04count - TotalPrice_2.reserve_count))
            sess.execute(update(TotalPrice_2).values(mult_less=None))
            sess.execute(update(TotalPrice_2).where(TotalPrice_2.count < TotalPrice_2._06mult_new).values(mult_less='-'))

            sess.execute(update(TotalPrice_2).where(and_(TotalPrice_2._07supplier_code == Data07_14.setting,
                                                    TotalPrice_2._14brand_filled_in == Data07_14.correct))
                         .values(markup_pb=Data07_14.markup_pb)) # code_pb_p=Data07_14.code_pb_p

            sess.execute(update(TotalPrice_2).where(TotalPrice_2._15code_optt == Buy_for_OS.article_producer).values(
                buy_count=Buy_for_OS.buy_count))

            self.log.add(LOG_ID, f"Данные в Итоговом прайсе обновлены [{str(datetime.datetime.now() - cur_time)[:7]}]",
                      f"Данные в <span style='color:{colors.green_log_color};font-weight:bold;'>Итоговом прайсе</span> обновлены "
                      f"[{str(datetime.datetime.now() - cur_time)[:7]}]")

            sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == 'Обновление данных в БД по 3.0').delete()
            sess.add(CatalogUpdateTime(catalog_name='Обновление данных в БД по 3.0', updated_at=cur_time.strftime("%Y-%m-%d %H:%M:%S")))

            sess.commit()
            return True


    def update_orders_table(self):
        try:
            with session() as sess:
                last_update = sess.execute(select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name == 'Заказы')).scalar()
                now = datetime.datetime.now()
                if now.date() == last_update.date() or now.hour < 12:
                    return

                self.log.add(LOG_ID, f"Загзузка заказов в БД ...",
                             f"Загзузка <span style='color:{colors.green_log_color};font-weight:bold;'>заказов</span> в БД ...")
                start_time = datetime.datetime.now()
                workbook = openpyxl.load_workbook(filename=settings_data["orders"])
                lists = workbook.sheetnames
                sheet_names = []
                for list_name in lists:
                    for table_name in workbook[list_name]._tables: #workbook['AvtoTO']._tables:
                        if str(table_name).startswith('таб'):
                            # print(list_name, table_name)
                            sheet_names.append(list_name)

                table_name = 'orders'
                table_class = Orders
                cols = {"order_time": ["Заказ"], "client": ["Клиент"], "auto": ["Автомат"], "manually": ["В ручную"],
                        "for_sort": ["Для сортировки"], "key_1_ord": ["Ключ1 в заказ"],
                        "article_ord": ["Артикул в заказ"],
                        "brand_ord": ["Производитель в заказ"], "count_ord": ["Заказ шт"],
                        "price_ord": ["Цена в заказ"],
                        "code_1c": ["В 1С Код наш"], "article_1c": ["В 1С Артикул наш"], "article": ["Тех. Артикул"],
                        "brand": ["Тех. Производитель"], "count": ["Тех. Кол-во"], "price": ["Тех. Цена"],
                        "name": ["Тех. Наименование"], "code_optt": ["Код ТутОптТорг"],
                        "our_brand": ["Наш производитель"],
                        "code_09": ["09Код"], }

                for sheet_name in sheet_names:
                    # self.log.add(LOG_ID, sheet_name)
                    update_catalog(sess, settings_data["orders"], cols, table_name, table_class, sheet_name=sheet_name,
                                   del_table=False, skiprows=3, orders_table=True)

                # Предложений опт
                sess.execute(update(Orders).where(and_(Orders.updated_at == None, Orders.code_optt == func.concat(MassOffers.article, MassOffers.brand)))
                             .values(offers_wh=MassOffers.offers_count))
                # Отказано шт
                conditions = [(Orders.auto == Orders.manually, func.greatest(0, Orders.count - Orders.count_ord))]
                sess.execute(update(Orders).where(Orders.updated_at == None).values(refuse=case(*conditions, else_=0)))
                # Заявка сумма
                sess.execute(update(Orders).where(Orders.updated_at == None).values(ord_sum=Orders.count * Orders.price))
                # Сумма в закупке
                conditions = [(Orders.count_ord == 0, 0),
                              (Orders.price_ord == 0, Orders.count_ord * Orders.price)]
                sess.execute(update(Orders).where(Orders.updated_at == None).values(buy_sum=case(*conditions, else_=Orders.count_ord * Orders.price_ord)))
                # ВП по подтверждённому
                sess.execute(update(Orders).where(Orders.updated_at == None).values(vp_accept=(Orders.price - Orders.price_ord) * Orders.count_ord))
                # Сумма подтверждённого
                sess.execute(update(Orders).where(Orders.updated_at == None).values(sum_accept=Orders.price_ord * Orders.count_ord))
                # Тип товара
                conditions = [(Orders.offers_wh >= 2, 'Опт'),]
                sess.execute(update(Orders).where(Orders.updated_at == None).values(product_type=case(*conditions, else_='УТ')))

                now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                sess.execute(update(Orders).where(Orders.updated_at == None).values(updated_at=now))

                sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == 'Заказы').delete()
                sess.add(CatalogUpdateTime(catalog_name='Заказы', updated_at=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                sess.commit()
                self.log.add(LOG_ID,
                             f"Заказы загружены в БД [{str(datetime.datetime.now() - start_time)[:7]}]",
                             f"<span style='color:{colors.green_log_color};font-weight:bold;'>Заказы</span> загружены в БД "
                             f"[{str(datetime.datetime.now() - start_time)[:7]}]")
        except (OperationalError, UnboundExecutionError) as db_ex:
            raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"update_orders_table Error", ex_text)

    def update_base_price(self, force_update=False):
        '''Формирование справочника Базовая цена'''
        try:
            if not self.CBP.isRunning():  # and not self.CMO.isRunning():
                # self.CreateBasePriceSignal.emit(False)
                self.CBP = CreateBasePrice(log=self.log, force_update=force_update)
                self.CBP.start()
        except (OperationalError, UnboundExecutionError) as db_ex:
            raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"update_base_price Error", ex_text)

    def update_mass_offers(self, force_update=False):
        '''Формирование справочника Базовая цена'''
        try:
            if not self.CMO.isRunning():# and not self.CMO.isRunning():
                # self.CreateBasePriceSignal.emit(False)
                self.CMO = CreateMassOffers(log=self.log, force_update=force_update)
                self.CMO.start()
        except (OperationalError, UnboundExecutionError) as db_ex:
            raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"update_mass_offers Error", ex_text)

class CreateBasePrice(QThread):
    CreateBasePriceSignal = Signal(bool)

    def __init__(self, log=None, force_update=False, parent=None):
        self.log = log
        self.force_update = force_update
        QThread.__init__(self, parent)

    def run(self):
        self.CreateBasePriceSignal.emit(False)
        # limit = 1_048_500
        try:
            catalog_name = 'Базовая цена'
            with session() as sess:
                # report_parts_count = sess.execute(select(func.count()).select_from(TotalPrice_1)).scalar()
                # report_parts_count = math.ceil(report_parts_count / 1_040_500)
                # print(f"{report_parts_count=}")
                # report_parts_count = 4
                hm = sess.execute(select(AppSettings.var).where(AppSettings.param == "base_price_update")).scalar()
                h, m = hm.split()

                cur_time = datetime.datetime.now()

                if not self.force_update:
                    last_update = sess.execute(select(CatalogUpdateTime.updated_at).where(
                        CatalogUpdateTime.catalog_name == catalog_name)).scalar()
                    compare_time = datetime.datetime.strptime(f"{str(last_update)[:10]} {h}:{m}:00","%Y-%m-%d %H:%M:%S")
                    # print(f"{compare_time=}")
                    if cur_time.hour > 8 or (cur_time - compare_time).days < 1:
                        return

                self.log.add(LOG_ID, f"Обновление {catalog_name} ...",
                             f"Обновление <span style='color:{colors.green_log_color};font-weight:bold;'>{catalog_name}</span> ...")

                sess.query(BasePrice).delete()
                sess.execute(text(f"ALTER SEQUENCE {BasePrice.__tablename__}_id_seq restart 1"))

                actual_prices = select(distinct(MailReport.price_code))\
                    .where(datetime.datetime.now() - datetime.timedelta(days=1) < MailReport.date)
                subq = select(TotalPrice_1._01article_comp, TotalPrice_1._14brand_filled_in, TotalPrice_1._05price,
                              TotalPrice_1._07supplier_code).where(
                    and_(TotalPrice_1._07supplier_code.in_(actual_prices), TotalPrice_1._07supplier_code == SupplierPriceSettings.price_code,
                         SupplierPriceSettings.is_base_price == 'ДА',
                         TotalPrice_1._20exclude == None, TotalPrice_1._05price > 0,
                         TotalPrice_1._14brand_filled_in != None, TotalPrice_1._01article_comp != None))
                sess.execute(insert(BasePrice).from_select(['article', 'brand', 'price_b', 'min_supplier'], subq))
                sess.query(BasePrice).where(BasePrice.brand.in_(select(distinct(Brands.correct_brand)).where(Brands.base_price != 'ДА'))).delete()
                sess.query(BasePrice).where(and_(BasePrice.article == MassOffers.article, BasePrice.brand == MassOffers.brand, MassOffers.offers_count <= 1)).delete()
                sess.commit()

                min_price_T = select(BasePrice.article, BasePrice.brand,
                                     func.min(BasePrice.price_b).label('min_price')) \
                    .group_by(BasePrice.article, BasePrice.brand).having(func.count(BasePrice.id) > 1)
                min_supl_T = select(BasePrice.article.label('min_art'), BasePrice.brand.label('min_brand'),
                                    BasePrice.min_supplier.label('min_supl')) \
                    .where(and_(BasePrice.article == min_price_T.c.article, BasePrice.brand == min_price_T.c.brand,
                                BasePrice.price_b == min_price_T.c.min_price))
                sess.execute(update(BasePrice).where(
                    and_(BasePrice.article == min_supl_T.c.min_art, BasePrice.brand == min_supl_T.c.min_brand)).
                             values(min_supplier=min_supl_T.c.min_supl))

                avg_price = select(BasePrice.article, BasePrice.brand,
                                   func.avg(BasePrice.price_b).label('avg_price_b'),
                                   func.min(BasePrice.price_b).label('min_price_b')).group_by(BasePrice.article,
                                                                                              BasePrice.brand)
                sess.execute(update(BasePrice).where(
                    and_(BasePrice.article == avg_price.c.article, BasePrice.brand == avg_price.c.brand)).
                             values(price_b=avg_price.c.avg_price_b, min_price=avg_price.c.min_price_b))

                max_id_dupl = select(func.max(BasePrice.id).label('max_id')).group_by(BasePrice.article,
                                                                                      BasePrice.brand)
                sess.execute(update(BasePrice).where(BasePrice.id.in_(max_id_dupl)).values(duple=False))
                sess.query(BasePrice).where(BasePrice.duple == True).delete()
                sess.commit()

                # Удаление старых данных
                # for file in os.listdir(fr"{settings_data['catalogs_dir']}/pre Справочник Базовая цена"):
                #     if file.startswith('Справочник Базовая цена - страница'):
                #         os.remove(fr"{settings_data['catalogs_dir']}/pre Справочник Базовая цена/{file}")
                #
                # loaded = 0
                # for i in range(1, report_parts_count + 1):
                #     df = pd.DataFrame(columns=['Артикул', 'Бренд', 'ЦенаБ', 'Мин. Цена', 'Мин. Поставщик'])
                #     df.to_csv(
                #         fr"{settings_data['catalogs_dir']}/pre Справочник Базовая цена/Справочник Базовая цена - страница {i}.csv",
                #         sep=';', decimal=',',
                #         encoding="windows-1251", index=False, errors='ignore')
                #     req = select(BasePrice.article, BasePrice.brand, BasePrice.price_b, BasePrice.min_price,
                #                  BasePrice.min_supplier). \
                #         order_by(BasePrice.id).offset(loaded).limit(limit)
                #     df = pd.read_sql_query(req, sess.connection(), index_col=None)
                #
                #     df.to_csv(
                #         fr"{settings_data['catalogs_dir']}/pre Справочник Базовая цена/Справочник Базовая цена - страница {i}.csv",
                #         mode='a',
                #         sep=';', decimal=',', encoding="windows-1251", index=False, header=False, errors='ignore')
                #
                #     df_len = len(df)
                #     loaded += df_len
                #
                # # create_csv_catalog(path_to_catalogs + "/pre Справочник Базовая цена/Базовая цена - страница {}.csv",
                # #                    """SELECT base_price.Артикул as "Артикул", base_price.Бренд as "Бренд",
                # #                         base_price.ЦенаБ as "ЦенаБ", base_price.ЦенаМин as "Мин. Цена", ЦенаМинПоставщик
                # #                         as "Мин. Поставщик" FROM base_price ORDER BY Бренд OFFSET {} LIMIT {}""",
                # #                    host, user, password, db_name, report_parts_count)
                # #
                # for file in os.listdir(fr"{settings_data['catalogs_dir']}/Справочник Базовая цена"):
                #     if file.startswith('Справочник Базовая цена - страница'):
                #         os.remove(fr"{settings_data['catalogs_dir']}/Справочник Базовая цена/{file}")
                #
                # for i in range(1, report_parts_count + 1):
                #     shutil.copy(
                #         fr"{settings_data['catalogs_dir']}/pre Справочник Базовая цена/Справочник Базовая цена - страница {i}.csv",
                #         fr"{settings_data['catalogs_dir']}/Справочник Базовая цена/Справочник Базовая цена - страница {i}.csv")

                # Обновление итога
                sess.execute(update(TotalPrice_2).where(and_(TotalPrice_2._01article_comp == BasePrice.article,
                                                             TotalPrice_2._14brand_filled_in == BasePrice.brand))
                             .values(price_b=BasePrice.price_b, min_price=BasePrice.min_price,
                                     min_supplier=BasePrice.min_supplier))

                sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == catalog_name).delete()
                sess.add(
                    CatalogUpdateTime(catalog_name=catalog_name, updated_at=cur_time.strftime("%Y-%m-%d %H:%M:%S")))
                sess.commit()


            self.log.add(LOG_ID, f"{catalog_name} обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]",
                         f"Обновление <span style='color:{colors.green_log_color};font-weight:bold;'>{catalog_name}</span> "
                         f"обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]")
        # except (OperationalError, UnboundExecutionError) as db_ex:
        #     raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"CreateBasePrice Error", ex_text)
        finally:
            self.CreateBasePriceSignal.emit(True)


class CreateMassOffers(QThread):
    CreateMassOffersSignal = Signal(bool)

    def __init__(self, log=None, force_update=False, parent=None):
        self.log = log
        self.force_update = force_update
        QThread.__init__(self, parent)

    def run(self):
        self.CreateMassOffersSignal.emit(False)
        # limit = 1_048_500
        try:
            catalog_name = 'Предложений в опте'
            with session() as sess:
                # report_parts_count = sess.execute(select(func.count()).select_from(TotalPrice_1)).scalar()
                # report_parts_count = math.ceil(report_parts_count / 1_040_500)
                # print(f"{report_parts_count=}")
                # report_parts_count = 4
                hm = sess.execute(select(AppSettings.var).where(AppSettings.param == "mass_offers_update")).scalar()
                h, m = hm.split()
                # hm_time = datetime.time(int(h), int(m))

                cur_time = datetime.datetime.now()

                if not self.force_update:
                    last_update = sess.execute(select(CatalogUpdateTime.updated_at).where(
                        CatalogUpdateTime.catalog_name == catalog_name)).scalar()
                    compare_time = datetime.datetime.strptime(f"{str(last_update)[:10]} {h}:{m}:00", "%Y-%m-%d %H:%M:%S")
                    # print(f"{compare_time=}")
                    if cur_time.hour > 8 or (cur_time - compare_time).days < 1:
                        return

                # report_parts_count = math.ceil(sess.execute(select(func.count()).select_from(TotalPrice_1)).scalar() / limit)
                # if report_parts_count < 1:
                #     report_parts_count = 1


                self.log.add(LOG_ID, f"Обновление {catalog_name} ...",
                             f"Обновление <span style='color:{colors.green_log_color};font-weight:bold;'>{catalog_name}</span> ...")

                sess.query(MassOffers).delete()
                sess.execute(text(f"ALTER SEQUENCE {MassOffers.__tablename__}_id_seq restart 1"))

                actual_prices = select(distinct(MailReport.price_code))\
                    .where(datetime.datetime.now() - datetime.timedelta(days=1) < MailReport.date)
                subq = select(TotalPrice_1._01article_comp, TotalPrice_1._14brand_filled_in, TotalPrice_1._07supplier_code).where(
                    and_(TotalPrice_1._07supplier_code.in_(actual_prices), TotalPrice_1._07supplier_code == SupplierPriceSettings.price_code,
                         SupplierPriceSettings.wholesale == 'ДА', TotalPrice_1._20exclude == None, TotalPrice_1._05price > 0,
                         TotalPrice_1._14brand_filled_in != None, TotalPrice_1._01article_comp != None))
                sess.execute(insert(MassOffers).from_select(['article', 'brand', 'price_code'], subq))
                sess.query(MassOffers).where(
                    MassOffers.brand.in_(select(distinct(Brands.correct_brand)).where(Brands.mass_offers != 'ДА'))).delete()


                # # Замена 1MIK, 2MIK на MIK

                sess.execute(update(MassOffers).where(MassOffers.price_code == SupplierPriceSettings.price_code).
                             values(price_code=SupplierPriceSettings.supplier_code))
                # # Удаление дублей в разрезе MIK
                duple_pos = select(MassOffers.article, MassOffers.brand, MassOffers.price_code).\
                    group_by(MassOffers.article, MassOffers.brand, MassOffers.price_code).having(func.count(MassOffers.id) > 1)
                sess.execute(update(MassOffers).where(and_(MassOffers.article == duple_pos.c.article, MassOffers.brand == duple_pos.c.brand,
                                                           MassOffers.price_code == duple_pos.c.price_code)).values(duple=True))

                max_id_in_duple = select(func.max(MassOffers.id)).where(MassOffers.duple == True).\
                    group_by(MassOffers.article, MassOffers.brand, MassOffers.price_code)
                sess.execute(update(MassOffers).where(and_(MassOffers.duple == True, MassOffers.id.in_(max_id_in_duple))).
                             values(duple=False))
                sess.query(MassOffers).where(MassOffers.duple == True).delete()

                # # Предложений_в_опте
                cnt_price = select(MassOffers.article, MassOffers.brand, func.count(MassOffers.id).label('cnt')).\
                    group_by(MassOffers.article, MassOffers.brand).having(func.count(MassOffers.id) > 1)
                sess.execute(update(MassOffers).where(and_(MassOffers.article == cnt_price.c.article, MassOffers.brand == cnt_price.c.brand)).
                             values(offers_count=cnt_price.c.cnt))

                # # Удаление дублей (Артикул, Бренд)
                max_id_in_duple = select(MassOffers.article, MassOffers.brand).group_by(MassOffers.article, MassOffers.brand).\
                    having(func.count(MassOffers.id) > 1)
                sess.execute(update(MassOffers).where(and_(MassOffers.article == max_id_in_duple.c.article,
                                                           MassOffers.brand == max_id_in_duple.c.brand)).values(duple=True))

                max_id_in_duple = select(func.max(MassOffers.id)).where(MassOffers.duple == True).group_by(MassOffers.article, MassOffers.brand). \
                    having(func.count(MassOffers.id) > 1)
                sess.execute(update(MassOffers).where(MassOffers.id.in_(max_id_in_duple)).values(duple=False))
                sess.query(MassOffers).where(MassOffers.duple == True).delete()  # MassOffers.offers_count <= 1

                sess.commit()

                # for file in os.listdir(fr"{settings_data['catalogs_dir']}/pre Справочник Предложений в опте"):
                #     if file.startswith('Справочник Предложений в опте - страница'):
                #         os.remove(fr"{settings_data['catalogs_dir']}/pre Справочник Предложений в опте/{file}")
                #
                # loaded = 0
                # for i in range(1, report_parts_count + 1):
                #     df = pd.DataFrame(columns=['Артикул', 'Бренд', 'Предложений в опте'])
                #     df.to_csv(
                #         fr"{settings_data['catalogs_dir']}/pre Справочник Предложений в опте/Справочник Предложений в опте - страница {i}.csv",
                #         sep=';', decimal=',',
                #         encoding="windows-1251", index=False, errors='ignore')
                #     req = select(MassOffers.article, MassOffers.brand, MassOffers.offers_count).order_by(MassOffers.id).offset(loaded).limit(limit)
                #     df = pd.read_sql_query(req, sess.connection(), index_col=None)
                #
                #     df.to_csv(
                #         fr"{settings_data['catalogs_dir']}/pre Справочник Предложений в опте/Справочник Предложений в опте - страница {i}.csv",
                #         mode='a',
                #         sep=';', decimal=',', encoding="windows-1251", index=False, header=False, errors='ignore')
                #
                #     df_len = len(df)
                #     loaded += df_len
                #
                # for file in os.listdir(fr"{settings_data['catalogs_dir']}/Справочник Предложений в опте"):
                #     if file.startswith('Справочник Предложений в опте - страница'):
                #         os.remove(fr"{settings_data['catalogs_dir']}/Справочник Предложений в опте/{file}")
                #
                # for i in range(1, report_parts_count + 1):
                #     shutil.copy(
                #         fr"{settings_data['catalogs_dir']}/pre Справочник Предложений в опте/Справочник Предложений в опте - страница {i}.csv",
                #         fr"{settings_data['catalogs_dir']}/Справочник Предложений в опте/Справочник Предложений в опте - страница {i}.csv")

                # Обновление итога
                sess.execute(update(TotalPrice_2).where(and_(TotalPrice_2._01article_comp == MassOffers.article,
                                                        TotalPrice_2._14brand_filled_in == MassOffers.brand))
                             .values(offers_wh=MassOffers.offers_count))

                sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == catalog_name).delete()
                sess.add(
                    CatalogUpdateTime(catalog_name=catalog_name, updated_at=cur_time.strftime("%Y-%m-%d %H:%M:%S")))
                sess.commit()

            self.log.add(LOG_ID, f"{catalog_name} обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]",
                         f"Обновление <span style='color:{colors.green_log_color};font-weight:bold;'>{catalog_name}</span> "
                         f"обновлён [{str(datetime.datetime.now() - cur_time)[:7]}]")
        except (OperationalError, UnboundExecutionError) as db_ex:
            raise db_ex
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"CreateMassOffers Error", ex_text)
        finally:
            self.CreateMassOffersSignal.emit(True)


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


class SaveTgTime(QThread):
    def __init__(self, tg_time, log=None, parent=None):
        self.log = log
        self.tg_time = tg_time
        QThread.__init__(self, parent)

    def run(self):
        try:
            with session() as sess:
                sess.query(AppSettings).filter(AppSettings.param=='tg_notification_time').delete()
                sess.add(AppSettings(param='tg_notification_time', var=f"{self.tg_time.hour()} {self.tg_time.minute()}"))
                sess.commit()
            self.log.add(LOG_ID, f"Время сохранено", f"<span style='color:{colors.green_log_color};font-weight:bold;'>Время сохранено</span>  ")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"SaveTgTime Error", ex_text)

class SaveCond3Time(QThread):
    def __init__(self, tg_time, log=None, parent=None):
        self.log = log
        self.tg_time = tg_time
        QThread.__init__(self, parent)

    def run(self):
        try:
            with session() as sess:
                sess.query(AppSettings).filter(AppSettings.param == 'last_DB_3_update').delete()
                sess.add(AppSettings(param='last_DB_3_update',
                                     var=f"{self.tg_time.hour()} {self.tg_time.minute()}"))
                sess.commit()
            self.log.add(LOG_ID, f"Время сохранено",
                         f"<span style='color:{colors.green_log_color};font-weight:bold;'>Время сохранено</span>  ")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"SaveCond3Time Error", ex_text)

class CatalogsUpdateTable(QThread):
    CatalogsInfoSignal = Signal(CatalogUpdateTime)
    TimeUpdateSetSignal = Signal(str)
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)

    def run(self):
        try:
            with session() as sess:
                catalogs_info = sess.execute(select(CatalogUpdateTime)).scalars().all()
                # print(f"{catalogs_info=}")
                self.CatalogsInfoSignal.emit(catalogs_info)
            # self.log.add(LOG_ID, f"Таблица 'Последние обновления - справочники' обновлена", f"Таблица <span style='color:{colors.green_log_color};'>"
            #                                                                                 f"'Последние обновления - справочники'</span> обновлена  ")
            self.TimeUpdateSetSignal.emit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"CatalogsUpdateTable Error", ex_text)


class CurrencyUpdateTable(QThread):
    CurrencyInfoSignal = Signal(ExchangeRate)
    TimeUpdateSetSignal = Signal(str)
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)

    def run(self):
        try:
            with session() as sess:
                catalogs_info = sess.execute(select(ExchangeRate).where(ExchangeRate.code.in_(['USD', 'EUR', 'CNY']))).scalars().all()
                self.CurrencyInfoSignal.emit(catalogs_info)
                catalogs_info = sess.execute(select(ExchangeRate).where(ExchangeRate.code.notin_(['USD', 'EUR', 'CNY']))).scalars().all()
                self.CurrencyInfoSignal.emit(catalogs_info)
            # self.log.add(LOG_ID, f"Таблица 'Курс валют' обновлена", f"Таблица <span style='color:{colors.green_log_color};'>"
            #                                                         f"'Курс валют'</span> обновлена  ")
            self.TimeUpdateSetSignal.emit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"CurrencyUpdateTable Error", ex_text)

def get_catalogs_time_update():
    try:
        with session() as sess:
            req = select(AppSettings).where(AppSettings.param.in_(PARAM_LIST))
            times = sess.execute(req).scalars().all()
            times = {t.param: t.var for t in times}

            return times
    except:
        return None


def update_catalog(ses, path_to_file, cols, table_name, table_class, sheet_name=0, del_table=True, skiprows=0, orders_table=False):
    '''for varchar(x), real, numeric, integer'''
    con = ses.connection()
    pk = []
    # берутся столбцы из таблицы: название столбца, максимальная длина его поля
    if del_table:
        req = delete(table_class)
        con.execute(req)
        con.execute(text(f"ALTER SEQUENCE {table_name}_id_seq restart 1"))

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

    df = pd.read_excel(path_to_file, usecols=[cols[c][0] for c in cols], na_filter=False, sheet_name=sheet_name, skiprows=skiprows)
    df = df.rename(columns={cols[c][0]: c for c in cols})

    for c in cols:
        char_limit = cols[c][1]
        if char_limit:  # str
            df[c] = df[c].apply(lambda x: str(x).replace(' ', ' '))
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
    if orders_table:
        df = df[df['count'] > 0]  # Тех. Кол-во
        # now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # df['updated_at'] = now
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
