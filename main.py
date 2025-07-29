import sys
from PySide6.QtCore import QThread, Signal, Qt, QObject, QTime
from PySide6.QtWidgets import QApplication, QMainWindow, QTableView, QHeaderView, QMessageBox
from PySide6.QtGui import QStandardItemModel, QStandardItem, QIcon
from price_processing_2_ui import Ui_MainWindow
import time
import datetime
import os
from multiprocessing import Pipe
import multiprocessing as mp
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

import setting
setting.create_dirs()
# print(os.getpid())

setting.check_settings_file()
engine = setting.get_engine()
if not engine:
    print('\nНе удалось подулючиться к БД. Проверьте данные в Settings.txt')
    _ = input()
    sys.exit()

# mp.freeze_support()

from models import Base, AppSettings
# Base.metadata.create_all(engine)

settings_data = setting.get_vars()

import Logs
from PriceReader import MainWorker, PriceReportUpdate
import MailParser
from Timer import MyTimer
from PipeListener import PipeListener
import CatalogUpdate # import CatalogUpdate, SaveTime, get_catalogs_time_update, CatalogsUpdateTable

Log = Logs.LogClass()
Log.start()

MAX_LOG_ROWS_IN_TEXT_BROWSER = 200
DEFAULT_THREAD_COUNT = settings_data["thread_count"]

class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self):
        QMainWindow.__init__(self)
        self.setupUi(self)

        self.ThreadSpinBox.setProperty("value", DEFAULT_THREAD_COUNT)

        self.sender, self.listener = Pipe()
        self.PipeL = PipeListener(self.listener, Log)
        self.PipeL.UpdateinfoTableSignal.connect(self.update_table)
        self.PipeL.SetNewRowSignal.connect(self.add_new_row)
        self.PipeL.ResetTableSignal.connect(self.reset_table)
        self.PipeL.StopTimerSignal.connect(self.stop_timer)
        self.PipeL.start()

        self.MW = None
        self.MP = None
        self.ST = None
        self.MailReportReset = MailParser.MailReportDelete(log=Log)
        self.MailReportUpdate = MailParser.MailReportUpdate(log=Log)

        self.CatalogsUpdateTable = CatalogUpdate.CatalogsUpdateTable(log=Log)
        self.CatalogsUpdateTable.CatalogsInfoSignal.connect(lambda x:self.add_item_to_catalogs_update_time_table(x))
        self.CatalogsUpdateTable.TimeUpdateSetSignal.connect(lambda x:self.CatalogUpdateTime_TimeLabel2.setText(x))

        self.CurrencyUpdateTable = CatalogUpdate.CurrencyUpdateTable(log=Log)
        self.CurrencyUpdateTable.CurrencyInfoSignal.connect(lambda x:self.add_item_to_currency_table(x))
        self.CurrencyUpdateTable.TimeUpdateSetSignal.connect(lambda x:self.CurrencyTable_TimeLabel2.setText(x))

        self.PriceReportUpdate = PriceReportUpdate(log=Log)
        self.PriceReportUpdate.UpdateInfoTableSignal.connect(self.add_item_to_price_1_report_table)
        self.PriceReportUpdate.UpdatePriceReportTime.connect(lambda x:self.TimeOfLastReportUpdatelabel_1.setText(x))
        self.CU = CatalogUpdate.CatalogUpdate(log=Log)
        self.CU.StartTablesUpdateSignal.connect(self.update_catalogs_update_time_table)
        self.CU.StartTablesUpdateSignal.connect(self.update_currency_table)

        Log.AddLogToTableSignal.connect(self.add_log_to_text_browser)

        self.StartButton_0.clicked.connect(self.start_mail_parser)
        self.ToMailReportDirButton_0.clicked.connect(lambda _:self.open_dir(settings_data['catalogs_dir']))
        self.ToMailFilesDirButton_0.clicked.connect(lambda _:self.open_dir(settings_data['mail_files_dir']))
        self.OpenReportButton_0.clicked.connect(lambda _:self.open_dir(fr".{settings_data['catalogs_dir']}/mail_repotr.csv"))
        self.ResetMailReportButton_0.clicked.connect(
            lambda _:self.confirmed_message_box('Обнуление отчёта','Обнулить отчёт?', self.reset_mail_report_confirmed))
        self.UpdateReportButton_0.clicked.connect(self.start_update_mail_report_table)
        self.LogButton_0.clicked.connect(lambda _: self.open_dir(fr"logs\{Logs.log_file_names[0]}"))
        self.LogButton_0.setToolTip("Открыть файл с логами")

        self.MailReportUpdate.UpdateInfoTableSignal.connect(self.add_item_to_mail_report_table)
        self.MailReportUpdate.UpdateMailReportTime.connect(lambda x:self.TimeOfLastReportUpdatelabel_0.setText(x))

        self.model_0 = QStandardItemModel()
        self.model_0.setHorizontalHeaderLabels(['Sender', 'File name', 'Date'])
        self.MailStatusTable_0.setModel(self.model_0)
        self.MailStatusTable_0.verticalHeader().hide()
        self.PriceStatusTableView_1.horizontalHeader().setStretchLastSection(True)
        self.MailStatusTable_0.setEditTriggers(QTableView.NoEditTriggers)
        self.MailStatusTable_0.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        # self.MailStatusTable_0.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)



        self.StartButton_1.clicked.connect(self.start_mult)
        self.LogButton_1.clicked.connect(lambda _: self.open_dir(fr"logs\{Logs.log_file_names[1]}"))
        self.LogButton_1.setToolTip("Открыть файл с логами")
        self.ToFilesDirButton_1.clicked.connect(lambda _: self.open_dir(settings_data['exit_1_dir']))
        self.UpdateReportButton_1.clicked.connect(self.update_price_1_report_table)
        # self.pushButton_2.clicked.connect(self.start_mult)  # start mult

        # взять паузу
        # self.LThread = LoopThread()
        # self.LThread.SetButtonEnabledSignal.connect(lambda _: self.set_enabled_start_buttons(_, self.StartButton_1, self.PauseCheckBox_1))
        # self.LThread.isPause = self.PauseCheckBox_1.isChecked()
        # self.LThread.UpdateinfoTableSignal.connect(self.update_table)
        self.PauseCheckBox_0.checkStateChanged.connect(lambda b:self.setPause(b, self.MP))
        self.PauseCheckBox_1.checkStateChanged.connect(lambda b:self.setPause(b, self.MW))
        self.PauseCheckBox_2.checkStateChanged.connect(lambda b:self.setPause(b, self.CU))

        self.model_1 = QStandardItemModel()
        self.model_1.setHorizontalHeaderLabels(['Code', 'Status', 'Time', 'Total Time'])
        self.PriceStatusTableView_1.setModel(self.model_1)
        self.PriceStatusTableView_1.verticalHeader().hide()
        self.PriceStatusTableView_1.setEditTriggers(QTableView.NoEditTriggers)
        self.PriceStatusTableView_1.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        # self.PriceStatusTableView_1.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        # self.PriceStatusTableView_1.horizontalHeader().stretchLastSection()
        # for i in range(self.PriceStatusTableView_1.horizontalHeader().count()):
        #     self.PriceStatusTableView_1.horizontalHeader().resizeSection(i, 200)



        self.model_1_1 = QStandardItemModel()
        self.model_1_1.setHorizontalHeaderLabels(['Code', 'Info', 'Time'])
        self.NotMatchedTableView_1.setModel(self.model_1_1)
        self.NotMatchedTableView_1.verticalHeader().hide()
        self.NotMatchedTableView_1.setEditTriggers(QTableView.NoEditTriggers)
        self.NotMatchedTableView_1.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        # self.NotMatchedTableView_1.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        # self.NotMatchedTableView_1.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)


        self.StartButton_2.clicked.connect(self.start_catalog_update)
        self.CU.SetButtonEnabledSignal.connect(lambda _: self.set_enabled_start_buttons(_, self.StartButton_2, self.PauseCheckBox_2))

        self.model_2_1 = QStandardItemModel()
        self.model_2_1.setHorizontalHeaderLabels(['Catalog', 'Time'])
        self.CatalogUpdateTimeTableView_2.setModel(self.model_2_1)
        self.CatalogUpdateTimeTableView_2.verticalHeader().hide()
        self.CatalogUpdateTimeTableView_2.setEditTriggers(QTableView.NoEditTriggers)
        # self.CatalogUpdateTimeTableView_2.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.CatalogUpdateTimeTableView_2.horizontalHeader().resizeSection(0, 170)
        self.CatalogUpdateTimeTableView_2.horizontalHeader().setStretchLastSection(True)

        self.model_2_2 = QStandardItemModel()
        self.model_2_2.setHorizontalHeaderLabels(['Code', 'Value'])
        self.CurrencyTableView_2.setModel(self.model_2_2)
        self.CurrencyTableView_2.verticalHeader().hide()
        self.CurrencyTableView_2.setEditTriggers(QTableView.NoEditTriggers)
        # self.CurrencyTableView_2.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.CurrencyTableView_2.horizontalHeader().resizeSection(0, 170)
        self.CurrencyTableView_2.horizontalHeader().setStretchLastSection(True)

        self.LogButton_2.clicked.connect(lambda _: self.open_dir(fr"logs\{Logs.log_file_names[2]}"))
        self.LogButton_2.setToolTip("Открыть файл с логами")
        self.ToReportDirButton_2.clicked.connect(lambda _: self.open_dir(settings_data['catalogs_dir']))
        self.ResetDBButton_2.clicked.connect(
            lambda _: self.confirmed_message_box('Обнуление БД', 'Обнулить данные в БД?', self.reset_db))
        self.CatalogUpdateTimeTableUpdateButton_2.clicked.connect(self.update_catalogs_update_time_table)
        self.CurrencyTableUpdateButton_2.clicked.connect(self.update_currency_table)


        times = CatalogUpdate.get_catalogs_time_update()
        time_edits = {"base_price_update": self.BasePriceTimeEdit_2, "mass_offers_update": self.MassOffersTimeEdit_2}
        if times:
            for t in times:
                h, m = str(times[t]).split(" ")
                time_edits[t].setTime(QTime(int(h), int(m)))

        self.TimeSaveButton_2.clicked.connect(self.save_catalogs_time_update)


        self.timers = dict()
        self.main_timers = dict()
        self.totalFiles = 0
        self.doneFiles = 0

        # self.statusBar.showMessage("sb")
        self.progressBar_1.setStyleSheet('background-color: lightblue;')

        self.ConsoleTextBrowser_0.document().setMaximumBlockCount(MAX_LOG_ROWS_IN_TEXT_BROWSER)
        self.ConsoleTextBrowser_1.document().setMaximumBlockCount(MAX_LOG_ROWS_IN_TEXT_BROWSER)
        self.ConsoleTextBrowser_2.document().setMaximumBlockCount(MAX_LOG_ROWS_IN_TEXT_BROWSER)
        self.consoles = {0: self.ConsoleTextBrowser_0, 1: self.ConsoleTextBrowser_1, 2: self.ConsoleTextBrowser_2}
        self.set_old_logs()

        self.x = 0
        self.dbWorker = None

        # AUTO START
        if os.path.exists('autostart.txt'):
            self.start_mail_parser()
            self.start_mult()
            self.start_catalog_update()


    def reset_db(self, btn):
        if btn.text() == 'OK':
            try:
                Base.metadata.drop_all(engine)
                Base.metadata.create_all(engine)
                with sessionmaker(engine)() as sess:
                    sess.add_all([AppSettings(param="base_price_update", var="0 15"), AppSettings(param="mass_offers_update", var="0 15")])
                    sess.commit()
                print('БД обновлена')
            except Exception as ex:
                print(ex)

    def start_catalog_update(self):
        if not self.CU.isRunning():
            self.CU.start()

    def save_catalogs_time_update(self):
        if not self.ST:
            self.ST = CatalogUpdate.SaveTime(self.BasePriceTimeEdit_2.time(), self.MassOffersTimeEdit_2.time(), log=Log)
            self.ST.start()
        elif not self.ST.isRunning():
            self.ST = CatalogUpdate.SaveTime(self.BasePriceTimeEdit_2.time(), self.MassOffersTimeEdit_2.time(), log=Log)
            self.ST.start()

    def update_price_1_report_table(self):
        if not self.PriceReportUpdate.isRunning():
            while self.model_1_1.rowCount() > 0:
                self.model_1_1.removeRow(self.model_1_1.rowCount() - 1)
            self.PriceReportUpdate.start()

    def add_item_to_price_1_report_table(self, report):
        if report:
            items = [QStandardItem(f"{r}") for r in report]
            self.model_1_1.appendRow(items)

    def start_update_mail_report_table(self):
        if not self.MailReportUpdate.isRunning():
            while self.model_0.rowCount() > 0:
                self.model_0.removeRow(self.model_0.rowCount() - 1)
            self.MailReportUpdate.start()

    def add_item_to_mail_report_table(self, *args):
        d = args
        items = [QStandardItem(i) for i in d]
        self.model_0.appendRow(items)

    def reset_mail_report_confirmed(self, btn):
        if btn.text() == 'OK':    # именно капсом
            if not self.MailReportReset.isRunning():
                self.MailReportReset.start()

    def confirmed_message_box(self, title, msg, function, icon_type=QMessageBox.Question): # reset_mail_report
        ConfirmMsgBox = QMessageBox()
        ConfirmMsgBox.setWindowTitle(title)#'Обнуление отчёта')
        ConfirmMsgBox.setText(msg)#'Обнулить отчёт?')
        ConfirmMsgBox.setIcon(icon_type)
        ConfirmMsgBox.setStandardButtons(QMessageBox.Ok|QMessageBox.Cancel)
        ConfirmMsgBox.setDefaultButton(QMessageBox.Ok)
        ConfirmMsgBox.buttonClicked.connect(function)#self.reset_mail_report_confirmed)

        ConfirmMsgBox.exec()

    def open_dir(self, dir):
        os.startfile(dir) # + except

    def set_old_logs(self):
        for i in self.consoles.keys():
            with open(Logs.log_files_local[i], 'r') as log_file:
                last_log_rows = log_file.readlines()[-MAX_LOG_ROWS_IN_TEXT_BROWSER:]
                if last_log_rows:
                    self.consoles[i].append('\n'.join(l.replace('\n', '') for l in last_log_rows))

            self.consoles[i].append("<hr><br>")

    def add_log_to_text_browser(self, id_console_log, text):
        self.consoles[id_console_log].append(text)

    def stop_timer(self, r, new_children_price):
        self.doneFiles += 1
        if new_children_price:
            self.totalFiles += 1
        self.ProgressLabel_1.setText(f"{self.doneFiles}/{self.totalFiles}")
        self.progressBar_1.setValue(self.doneFiles/self.totalFiles*100)
        self.timers[r] = None
        self.main_timers[r] = None
        for c in range(4):
            self.model_1.setData(self.model_1.index(r, c), '')

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
        self.MW.UpdateReportSignal.connect(self.update_price_1_report_table)

    def start_mail_parser(self):
        if not self.MP:
            self.create_mail_parser_class()
            return
        if not self.MP.isRunning():
            self.create_mail_parser_class()
    def create_mail_parser_class(self):
        self.MP = MailParser.MailParserClass(log=Log)
        self.MP.SetButtonEnabledSignal.connect(lambda _: self.set_enabled_start_buttons(_, self.StartButton_0, self.PauseCheckBox_0))
        self.MP.start()

    def set_total_time(self, start):
        if start:
            self.total_timer = MyTimer()
            self.total_timer.SetTimeSignal.connect(self.set_total_time_on_label)
        else:
            self.total_timer.SetTimeSignal.disconnect(self.set_total_time_on_label)

    def set_total_time_on_label(self, text):
        self.TotalTimeLabel.setText(text)

    def update_table(self, r, price_code, c, text, isNewPrice):
        self.model_1.setData(self.model_1.index(r, c), text)
        self.model_1.setData(self.model_1.index(r, 0), price_code)
        if isNewPrice:
            self.main_timers[r] = MyTimer(r, 3)
            self.model_1.setData(self.model_1.index(r, 3), self.main_timers[r])
            self.main_timers[r].SetTimeInTableSignal.connect(self.set_time)
        self.timers[r] = MyTimer(r, 2)
        self.model_1.setData(self.model_1.index(r, 2), self.timers[r])
        self.timers[r].SetTimeInTableSignal.connect(self.set_time)

    def set_time(self, r, c, str_time):
        # print("set_time", r)
        self.model_1.setData(self.model_1.index(r, c), str_time)

    def update_catalogs_update_time_table(self):
        if not self.CatalogsUpdateTable.isRunning():
            while self.model_2_1.rowCount() > 0:
                self.model_2_1.removeRow(self.model_2_1.rowCount() - 1)
            self.CatalogsUpdateTable.start()

    def add_item_to_catalogs_update_time_table(self, info_items):
        for info in info_items:
            items = [QStandardItem(f"{i}") for i in [info.catalog_name, info.updated_at]]
            self.model_2_1.appendRow(items)

    def update_currency_table(self):
        if not self.CurrencyUpdateTable.isRunning():
            while self.model_2_2.rowCount() > 0:
                self.model_2_2.removeRow(self.model_2_2.rowCount() - 1)
            self.CurrencyUpdateTable.start()
    def add_item_to_currency_table(self, info_items):
        for info in info_items:
            items = [QStandardItem(f"{i}") for i in [info.code, info.rate]]
            self.model_2_2.appendRow(items)

    def add_new_row(self):
        # d = [None, None, None]
        items = [QStandardItem('') for i in range(4)]
        self.model_1.appendRow(items)
    def reset_table(self, file_count):
        self.progressBar_1.setValue(0)
        self.ProgressLabel_1.setText(f"0/{self.totalFiles}")
        self.totalFiles = file_count
        self.doneFiles = 0

        while self.model_1.rowCount() > 0:
            self.model_1.removeRow(self.model_1.rowCount() - 1)

    # def add_text_to_list(self):
    #     self.x += 1
    #     self.ConsoleTextBrowser_1.append(f"Лог номер <span style='color:red; font-weight:bold;'>{self.x}</span>. ок")
    #
    # def set_row_count(self):
    #     self.x += 1
    #     d = [f"{self.x}", 'Aaaaaaaaaaaaaaaaasssssssssssssssssssssssssss', '0:00:15']
    #     items = [QStandardItem(i) for i in d]
    #     self.model_1.appendRow(items)
    #
    # def add_row(self):
    #     # d = ['1', 'Aaaaaaaaaaaaaaaaasssssssssssssssssssssssssss', '0:00:15']
    #     # self.items = [QStandardItem(i) for i in d]
    #     # self.model.appendRow(self.items)
    #
    #     self.model_1.setData(self.model_1.index(0,1), 'ffffffffffffffg')

    def setPause(self, state, some_class):
        if some_class:
            some_class.isPause = (state == Qt.CheckState.Checked)

    def set_enabled_start_buttons(self, enabled, btn, chb):
        btn.setEnabled(enabled)
        if not enabled:
            btn.setIcon(QIcon(QIcon.fromTheme(QIcon.ThemeIcon.MediaPlaybackPause)))
        else:
            btn.setIcon(QIcon(QIcon.fromTheme(QIcon.ThemeIcon.MediaPlaybackStart)))
        chb.setEnabled(not enabled)
        if enabled:
            chb.setChecked(False)


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