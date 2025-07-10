import sys
from PySide6.QtCore import QThread, Signal, Qt, QObject
from PySide6.QtWidgets import QApplication, QMainWindow, QTableView, QHeaderView
from PySide6.QtGui import QStandardItemModel, QStandardItem
from price_processing_2_ui import Ui_MainWindow
import time
import datetime
import os
from multiprocessing import Pipe
from sqlalchemy import text
from models import Base

import setting_vars
from Logs import LogClass
from Calculate import MainWorker#, get_correct_df
from Timer import MyTimer
from PipeListener import PipeListener


engine = setting_vars.get_engine()
Log = LogClass()



# def test_mp():
#     pass
#
# class LoopThread(QThread):
#     SetButtonEnabledSignal = Signal(bool)
#     UpdateinfoTableSignal = Signal(int, str, int, str, bool)
#     isPause = None
#
#     def __init__(self, parent=None):
#         QThread.__init__(self, parent)
#
#     def run(self):
#         # while not self.isPause:
#         #     print('c')
#         #     time.sleep(1)
#         self.SetButtonEnabledSignal.emit(False)
#         try:
#             while not self.isPause:
#                 print('T start')
#                 # time.sleep(1)
#                 mng = mp.Manager()
#                 obj = mng.Value('cnt', 0)
#                 # f = [1, 2]
#                 f = [[1, obj], [2, obj]]
#                 with mp.Pool(processes=2) as p:
#                     p.map(test_mp, f)
#                 # print(1/0)
#                 # time.sleep(2)
#                 print('T stop')
#         except Exception as ex:
#              print(ex)
#         self.SetButtonEnabledSignal.emit(True)


class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self):
        QMainWindow.__init__(self)
        self.setupUi(self)

        self.sender, self.listener = Pipe()
        self.PipeL = PipeListener(self.listener, Log)
        self.PipeL.UpdateinfoTableSignal.connect(self.update_table)
        self.PipeL.SetNewRowSignal.connect(self.add_new_row)
        self.PipeL.ResetTableSignal.connect(self.reset_table)
        self.PipeL.StopTimerSignal.connect(self.stop_timer)
        self.PipeL.start()

        self.MW = None

        Log.AddLogToTableSignal.connect(self.add_log_to_text_browser)

        self.StartButton_1.clicked.connect(self.start_mult)
        self.pushButton_2.clicked.connect(self.start_mult)  # start mult

        # взять паузу
        # self.LThread = LoopThread()
        # self.LThread.SetButtonEnabledSignal.connect(lambda _: self.set_enabled_start_buttons(_, self.StartButton_1, self.PauseCheckBox_1))
        # self.LThread.isPause = self.PauseCheckBox_1.isChecked()
        # self.LThread.UpdateinfoTableSignal.connect(self.update_table)
        self.PauseCheckBox_1.checkStateChanged.connect(self.setPause)

        self.model = QStandardItemModel()
        self.model.setHorizontalHeaderLabels(['Code', 'Status', 'Time', 'Total Time'])
        self.PriceStatusTableView_1.setModel(self.model)
        self.PriceStatusTableView_1.verticalHeader().hide()
        # self.PriceStatusTableView_1.horizontalHeader().setStretchLastSection(True)
        self.PriceStatusTableView_1.setEditTriggers(QTableView.NoEditTriggers)
        self.PriceStatusTableView_1.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        # self.PriceStatusTableView_1.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        # self.PriceStatusTableView_1.setColumnWidth(0, 40)
        # self.PriceStatusTableView_1.setSizeAdjustPolicy(QAbstractScrollArea.AdjustToContents)

        self.ConsoleTextBrowser_1.document().setMaximumBlockCount(25)

        self.timers = dict()
        self.main_timers = dict()
        self.totalFiles = 0
        self.doneFiles = 0

        # self.statusBar.showMessage("sb")
        self.progressBar_1.setStyleSheet('background-color: lightblue;')

        self.consoles = {0: self.ConsoleTextBrowser_1}

        self.x = 0
        self.dbWorker = None

    def add_log_to_text_browser(self, id_console_log, text):
        self.consoles[id_console_log].append(text)
    def stop_timer(self, r):
        self.doneFiles += 1
        self.ProgressLabel_1.setText(f"{self.doneFiles}/{self.totalFiles}")
        self.progressBar_1.setValue(self.doneFiles/self.totalFiles*100)
        self.timers[r] = None
        self.main_timers[r] = None
        for c in range(4):
            self.model.setData(self.model.index(r, c), '')

    def start_mult(self):
        if not self.MW:
            self.create_worker()
            return
        if not self.MW.isRunning():
            self.create_worker()
    def create_worker(self):
        self.MW = MainWorker(log=Log, sender=self.sender, threads_count=self.ThreadSpinBox.value())
        self.MW.SetButtonEnabledSignal.connect(
            lambda _: self.set_enabled_start_buttons(_, self.StartButton_1, self.PauseCheckBox_1))
        self.MW.start()
        self.MW.StartTotalTimeSignal.connect(self.set_total_time)

    def set_total_time(self, start):
        if start:
            self.total_timer = MyTimer()
            self.total_timer.SetTimeSignal.connect(self.set_total_time_on_label)
        else:
            self.total_timer.SetTimeSignal.disconnect(self.set_total_time_on_label)

    def set_total_time_on_label(self, text):
        self.TotalTimeLabel.setText(text)

    def update_table(self, r, price_code, c, text, isNewPrice):
        self.model.setData(self.model.index(r, c), text)
        self.model.setData(self.model.index(r, 0), price_code)
        if isNewPrice:
            self.main_timers[r] = MyTimer(r, 3)
            self.model.setData(self.model.index(r, 3), self.main_timers[r])
            self.main_timers[r].SetTimeInTableSignal.connect(self.set_time)
        self.timers[r] = MyTimer(r, 2)
        self.model.setData(self.model.index(r, 2), self.timers[r])
        self.timers[r].SetTimeInTableSignal.connect(self.set_time)

    def set_time(self, r, c, str_time):
        # print("set_time", r)
        self.model.setData(self.model.index(r, c), str_time)

    def add_new_row(self):
        # d = [None, None, None]
        data = [QStandardItem('') for i in range(4)]
        self.model.appendRow(data)
    def reset_table(self, file_count):
        self.progressBar_1.setValue(0)
        self.ProgressLabel_1.setText(f"0/{self.totalFiles}")
        self.totalFiles = file_count
        self.doneFiles = 0

        while self.model.rowCount() > 0:
            self.model.removeRow(self.model.rowCount() - 1)

    def add_text_to_list(self):
        self.x += 1
        self.ConsoleTextBrowser_1.append(f"Лог номер <span style='color:red; font-weight:bold;'>{self.x}</span>. ок")

    def set_row_count(self):
        self.x += 1
        d = [f"{self.x}", 'Aaaaaaaaaaaaaaaaasssssssssssssssssssssssssss', '0:00:15']
        data = [QStandardItem(i) for i in d]
        self.model.appendRow(data)

    def add_row(self):
        # d = ['1', 'Aaaaaaaaaaaaaaaaasssssssssssssssssssssssssss', '0:00:15']
        # self.data = [QStandardItem(i) for i in d]
        # self.model.appendRow(self.data)

        self.model.setData(self.model.index(0,1), 'ffffffffffffffg')

    def setPause(self, state):
        self.MW.isPause = (state == Qt.CheckState.Checked)

    def set_enabled_start_buttons(self, enabled, btn, chb):
        btn.setEnabled(enabled)
        chb.setEnabled(not enabled)
        if enabled:
            chb.setChecked(False)


    def start_thread(self):
        if self.MW.isRunning():
            return
        self.MW.start()

    # def db_test(self):
    #     table_name = 'data07'
    #     cols = {"works": ["Работаем?"], "update_time": ["Период обновления не более"], "setting": ["Настройка"],
    #             "delay": ["Отсрочка"], "sell_os": ["Продаём для ОС"], "markup_os": ["Наценка для ОС"],
    #             "max_decline": ["Макс снижение от базовой цены"],
    #             "markup_holidays": ["Наценка на праздники (1,02)"], "markup_R": ["Наценка Р"],
    #             "min_markup": ["Мин наценка"], "markup_wholesale": ["Наценка на оптовые товары"],
    #             "grad_step": ["Шаг градаци"],
    #             "wholesale_step": ["Шаг опт"], "access_pp": ["Разрешения ПП"], "unload_percent": ["% Отгрузки"]}
    #     if not self.dbWorker:
    #         self.dbWorker = DBWorker(path_to_file="3.0 Условия1.xlsx", cols=cols, table_name=table_name, sheet_name="07Данные")
    #     if self.dbWorker.isRunning():
    #         print('уже запущено')
    #         return
    #     # Base.metadata.drop_all(engine)
    #     # Base.metadata.create_all(engine)
    #     self.dbWorker.start()
        # return

        # df = self.dbWorker.get_correct_df("3.0 Условия1.xlsx", cols, table_name, sheet_name="07Данные")
        # print(df)
        # df = df[df['code'] != None]
        # print(df['code'])
        # df['markup_opt'] = np.float64(df['markup_opt'])
        # df.to_sql(name=table_name, con=engine, if_exists='append', index=False, index_label=False, chunksize=1000)
        # print(df)


def main():
    # with engine.connect() as con:
    #     res = con.execute(text("select count(*) from data07")).scalar()
    #     print(f"ff {res=}")

    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == '__main__':
    main()