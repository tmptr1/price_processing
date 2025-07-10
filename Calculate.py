from PySide6.QtCore import QThread, Signal
import random
import multiprocessing as mp
import time
import traceback
import datetime
import math
import os
from sqlalchemy import text, select, delete
from sqlalchemy.orm import sessionmaker
import numpy as np
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)
import openpyxl

from models import Base, Data07, Data07_14, Data15, CatalogUpdateTime
from Logs import add_log_cf
import setting_vars
engine = setting_vars.get_engine()
session = sessionmaker(engine)

CHUNKSIZE = 50000


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
        self.SetButtonEnabledSignal.emit(False)
        while not self.isPause:
            # time.sleep(1)
            # print('C')
            try:
                Base.metadata.drop_all(engine)
                Base.metadata.create_all(engine)
                # return
                # with engine.connect() as sess:
                #     req = delete(Data07)
                #     sess.execute(req)
                #     sess.commit()
                # print('sql ok')
                # return
                # self.catalogs_update()
                print('ok')
                return


                file_count = ['1FAL', '0MI1', '1IMP', '1PRD', '1VAL', '1АТХ', '1ГУД', '2AVX', '4MI0']
                if file_count:
                    self.log.add(0, "Начало обработки")
                cur_time = datetime.datetime.now()
                if len(file_count) < self.threads_count:
                    self.threads_count = len(file_count)

                self.StartTotalTimeSignal.emit(True)
                self.sender.send(["new", len(file_count)])

                with mp.Pool(processes=self.threads_count) as pool:
                    args = [[code, self.sender] for code in file_count]
                    pool.map(multi_calculate, args)

                self.log.add(0, f"Обработка закончена [{str(datetime.datetime.now() - cur_time)[:7]}]")

                self.StartTotalTimeSignal.emit(False)
            except Exception as ex:
                ex_text = traceback.format_exc()
                self.sender.send(["error", 0, ex, "M ERROR", ex_text])

            time.sleep(1)
        else:
            self.SetButtonEnabledSignal.emit(True)

    def catalogs_update(self):
        path_to_file = r"3.0 Условия.xlsx"
        new_update_time = datetime.datetime.fromtimestamp(os.path.getmtime(path_to_file))
        with session() as sess:
            req = select(CatalogUpdateTime.updated_at).where(CatalogUpdateTime.catalog_name==path_to_file)
            # sess.sele(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name==path_to_file)
            res = sess.execute(req).scalar()
        if res and res >= new_update_time:
            return
        self.log.add(0, f"Обновление {path_to_file} ...")

        # print(datetime.datetime.fromtimestamp(os.path.getmtime(path_to_file)))
        # return

        table_name = 'data07'
        table_class = Data07
        cols = {"works": ["Работаем?"], "update_time": ["Период обновления не более"], "setting": ["Настройка"],
                "delay": ["Отсрочка"], "sell_os": ["Продаём для ОС"], "markup_os": ["Наценка для ОС"],
                "max_decline": ["Макс снижение от базовой цены"],
                "markup_holidays": ["Наценка на праздники (1,02)"], "markup_R": ["Наценка Р"],
                "min_markup": ["Мин наценка"], "markup_wholesale": ["Наценка на оптовые товары"],
                "grad_step": ["Шаг градаци"],
                "wholesale_step": ["Шаг опт"], "access_pp": ["Разрешения ПП"], "unload_percent": ["% Отгрузки"]}
        sheet_name = "07Данные"
        update_catalog(path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

        table_name = 'data07_14'
        table_class = Data07_14
        cols = {"works": ["Работаем?"], "update_time": ["Период обновления не более"], "setting": ["Настройка"],
                "max_decline": ["Макс снижение от базовой цены"], "correct": ["Правильное"], "markup_pb": ["Наценка ПБ"],
                "code_pb_p": ["Код ПБ_П"]}
        sheet_name = "07&14Данные"
        update_catalog(path_to_file, cols, table_name, table_class, sheet_name=sheet_name)

        table_name = 'data15'
        table_class = Data15
        cols = {"code_15": ["15"], "offers_wholesale": ["Предложений опт"], "price_b": ["ЦенаБ"]}
        sheet_name = "15Данные"
        update_catalog(path_to_file, cols, table_name, table_class, sheet_name=sheet_name)


        with session() as sess:
            sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name==path_to_file).delete()
            sess.add(CatalogUpdateTime(catalog_name=path_to_file,
                                       updated_at=new_update_time))
            sess.commit()

        self.log.add(0, f"{path_to_file} обновлён")


def update_catalog(path_to_file, cols, table_name, table_class, sheet_name=None):
    '''for varchar(x), real, numeric, integer'''
    pk = []
    # берутся столбцы из таблицы: название столбца, максимальная длина его поля
    with engine.connect() as sess:
        req = delete(table_class)
        sess.execute(req)
        sess.commit()
        res = sess.execute(text(
            f"SELECT column_name, character_maximum_length FROM information_schema.columns WHERE table_name = '{table_name}' "
            f"and column_name != 'id'")).all()
        for i in res:
            cols[i[0]].append(i[1])
        res = sess.execute(text(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' and is_nullable = 'NO' "
            f"and column_name != 'id'")).all()
        pk = [i[0] for i in res]

    df = pd.read_excel(path_to_file, usecols=[cols[c][0] for c in cols], na_filter=False,
                       sheet_name=0 or sheet_name)
    df = df.rename(columns={cols[c][0]: c for c in cols})

    for c in cols:
        char_limit = cols[c][1]
        if char_limit:  # str
            df[c] = df[c].apply(lambda x: str(x)[:char_limit] or None)
        else:  # float/int
            df[c] = df[c].replace('', 0)
            df = df[df[c].apply(is_float)]
            df[c] = np.float64(df[c])
        if c in pk:  # для PK
            df = df[df[c].notna()]
    # return (df)
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False, index_label=False, chunksize=CHUNKSIZE)

def multi_calculate(args):
    price_code, sender = args
    color = [random.randrange(0, 360), random.randrange(55, 100), 90]
    try:
        cur_time = datetime.datetime.now()
        sender.send(["add", mp.current_process().name, price_code, 1, f"Загрузка сырых дынных...", True])
        time.sleep(random.randrange(7))
        add_log_cf(0, "Загрузка сырых дынных завершена", sender, price_code, cur_time, color)

        cur_time = datetime.datetime.now()
        sender.send(["add", mp.current_process().name, price_code, 1, f"Удаление дублей..."])
        time.sleep(random.randrange(7))
        add_log_cf(0,"Дубли удалены", sender, price_code, cur_time, color)

    except Exception as ex:
        ex_text = traceback.format_exc()
        sender.send(["error", 0, ex, "M ERROR", ex_text])
    finally:
        sender.send(["end", mp.current_process().name])

def is_float(x):
    try:
        x = float(str(x).replace(',', '.'))
        if math.isnan(x) or math.isinf(x):
            return False
        if 1E+37 < x < 1E-37:  # real
            return False
        return True
    except:
        return False
