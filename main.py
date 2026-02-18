import sys
from PySide6.QtCore import QThread, Signal, Qt, QObject, QTime
from PySide6.QtWidgets import QApplication, QMainWindow, QTableView, QHeaderView, QMessageBox
from PySide6.QtGui import QStandardItemModel, QStandardItem, QIcon
from price_processing_2_ui import Ui_MainWindow
import time
import datetime
import os
from sqlalchemy import text, select
from sqlalchemy.orm import sessionmaker

import setting
# print(os.getpid())
with open('pid.txt', 'w') as file:
    file.write(f"{os.getpid()}")

setting.check_settings_file()
engine = setting.get_engine()
if not engine:
    print('\nНе удалось подулючиться к БД. Проверьте данные в Settings.txt')
    _ = input()
    sys.exit()

session = sessionmaker(engine)
# mp.freeze_support()

from models import Base, AppSettings, CatalogUpdateTime
# Base.metadata.create_all(engine)

settings_data = setting.get_vars()
setting.create_dirs(settings_data)

import Logs
from PriceReader import MainWorker, PriceReportUpdate, PriceReportReset, SaveMBVAlue
import MailParser
from Timer import MyTimer
import CatalogUpdate
from Calculate import CalculateClass, PriceReportUpdate_2
from PriceSender import Sender, FinalPriceReportReset, FinalPriceReportUpdate

Log = Logs.LogClass()
Log.start()

MAX_LOG_ROWS_IN_TEXT_BROWSER = 200

class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self, autostart):
        QMainWindow.__init__(self)
        self.setupUi(self)
        self.autostart = autostart

        with session() as sess:
            try:
                req = select(AppSettings).where(AppSettings.param == 'tg_notification_time')
                tg_time = sess.execute(req).scalar()
                h, m = map(int, str(tg_time.var).split())
                self.Tg_timeEdit_2.setTime(QTime(h, m))

                req = select(AppSettings).where(AppSettings.param == 'last_DB_3_update')
                upd_3_time = sess.execute(req).scalar()
                h, m = map(int, str(upd_3_time.var).split())
                self.Update3_0_Condition_timeEdit_2.setTime(QTime(h, m))

                mb_limit_1 = sess.execute(select(AppSettings.var).where(AppSettings.param == 'mb_limit_1')).scalar()
                self.FileSizeLimit_spinBox_1.setValue(int(mb_limit_1))

                mb_limit_2 = sess.execute(select(AppSettings.var).where(AppSettings.param == 'mb_limit_2')).scalar()
                self.FileSizeLimit_spinBox_2.setValue(int(mb_limit_2))
            except Exception as get_tg_n_time_ex:
                pass

        self.MW = MainWorker(file_size_limit=f">{self.FileSizeLimit_spinBox_1.value()}", log=Log)  #, sender=self.sender)
        self.MW2 = MainWorker(file_size_limit=f"<{self.FileSizeLimit_spinBox_1.value()}", log=Log)


        self.MP = None
        self.ST = None
        self.STT = None
        self.STU = None
        self.SMBV = None
        self.MailReportReset = MailParser.MailReportDelete(log=Log)
        self.MailReportResetUnloaded = MailParser.MailReportUnloadedDelete(log=Log)
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
        self.PriceReportReset = PriceReportReset(log=Log)
        self.CU = CatalogUpdate.CatalogUpdate(log=Log)
        self.CU.StartTablesUpdateSignal.connect(self.update_catalogs_update_time_table)
        self.CU.StartTablesUpdateSignal.connect(self.update_currency_table)
        # self.CU.CreateTotalCsvSignal.connect(self.start_create_total_csv)
        # self.CU.CreateBasePriceSignal.connect(lambda b: self.CreateBasePriceButton_2.setEnabled(b))
        # self.CU.CreateMassOffersSignal.connect(lambda b: self.CreateMassOffersButton_2.setEnabled(b))

        self.Calculate = CalculateClass(file_size_limit=f">{self.FileSizeLimit_spinBox_2.value()}", log=Log)
        self.Calculate2 = CalculateClass(file_size_limit=f"<{self.FileSizeLimit_spinBox_2.value()}", log=Log)
        # self.CreateTotalCsv = CatalogUpdate.CreateTotalCsv(log=Log)
        self.PriceReportUpdate_2 = PriceReportUpdate_2(log=Log)
        self.PriceReportUpdate_2.UpdateInfoTableSignal.connect(self.add_item_to_price_2_report_table)
        self.PriceReportUpdate_2.UpdatePriceReportTime.connect(lambda x: self.TimeOfLastReportUpdatelabel_3.setText(x))
        self.PriceReportUpdate_2.ResetPriceReportTime.connect(lambda _: self.update_price_2_report_table)

        self.PriceSender = Sender(log=Log)

        Log.AddLogToTableSignal.connect(self.add_log_to_text_browser)

        self.StartButton_0.clicked.connect(self.start_mail_parser)
        self.ToMailReportDirButton_0.clicked.connect(lambda _:self.open_dir(settings_data['catalogs_dir']))
        self.ToMailFilesDirButton_0.clicked.connect(lambda _:self.open_dir(settings_data['mail_files_dir']))
        self.OpenReportButton_0.clicked.connect(lambda _:self.open_dir(fr"{settings_data['catalogs_dir']}/mail_report_unloaded.csv"))
        self.ResetMailReportButton_0.clicked.connect(
            lambda _: self.confirmed_message_box('Обнуление отчёта', 'Обнулить отчёт?', self.reset_mail_report_confirmed))
        self.ResetMailReportUnloadedButton_0.clicked.connect(
            lambda _: self.confirmed_message_box('Обнуление отчёта', 'Обнулить отчёт?', self.reset_mail_report_unloaded_confirmed))
        self.UpdateReportButton_0.clicked.connect(self.start_update_mail_report_table)
        self.LogButton_0.clicked.connect(lambda _: self.open_dir(fr"logs\{Logs.log_file_names[0]}"))
        self.LogButton_0.setToolTip("Открыть файл с логами")

        self.MailReportUpdate.UpdateInfoTableSignal.connect(self.add_item_to_mail_report_table)
        self.MailReportUpdate.UpdateMailReportTime.connect(lambda x:self.TimeOfLastReportUpdatelabel_0.setText(x))

        self.model_0 = QStandardItemModel()
        # self.model_0.setHorizontalHeaderLabels(['Price code', 'Sender', 'File name', 'Date'])
        self.model_0.setHorizontalHeaderLabels(['Sender', 'File name', 'Date'])
        self.MailStatusTable_0.setModel(self.model_0)
        self.MailStatusTable_0.verticalHeader().hide()
        self.MailStatusTable_0.setEditTriggers(QTableView.NoEditTriggers)
        self.MailStatusTable_0.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        # self.MailStatusTable_0.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)



        self.StartButton_1.clicked.connect(self.start_mult)
        self.LogButton_1.clicked.connect(lambda _: self.open_dir(fr"logs\{Logs.log_file_names[1]}"))
        self.LogButton_1.setToolTip("Открыть файл с логами")
        self.ToFilesDirButton_1.clicked.connect(lambda _: self.open_dir(settings_data['exit_1_dir']))
        self.ToReportDirButton_1.clicked.connect(lambda _: self.open_dir(settings_data['catalogs_dir']))
        self.UpdateReportButton_1.clicked.connect(self.update_price_1_report_table)
        self.ResetPriceReportButton_1.clicked.connect(self.reset_price_1_report)
        self.OpenReportButton_1.clicked.connect(lambda _: self.open_dir(fr"{settings_data['catalogs_dir']}/price_report.csv"))
        self.MB_SaveButton_1.clicked.connect(lambda _: self.save_MB_limit(0))

        # пауза
        self.PauseCheckBox_0.checkStateChanged.connect(lambda b: self.setPause(b, self.MP))
        self.PauseCheckBox_1.checkStateChanged.connect(lambda b: self.setPause(b, self.MW))
        self.PauseCheckBox_1.checkStateChanged.connect(lambda b: self.setPause(b, self.MW2))
        self.PauseCheckBox_2.checkStateChanged.connect(lambda b: self.setPause(b, self.CU))
        self.PauseCheckBox_3.checkStateChanged.connect(lambda b: self.setPause(b, self.Calculate))
        self.PauseCheckBox_3.checkStateChanged.connect(lambda b: self.setPause(b, self.Calculate2))
        self.PauseCheckBox_4.checkStateChanged.connect(lambda b: self.setPause(b, self.PriceSender))


        self.MW.UpdateReportSignal.connect(self.update_price_1_report_table)
        self.MW.UpdatePriceStatusTableSignal.connect(lambda r_id, p, s, t: self.update_status_table_1(r_id, p, s, t))
        self.MW2.UpdatePriceStatusTableSignal.connect(lambda r_id, p, s, t: self.update_status_table_1(r_id, p, s, t))
        self.MW.ResetPriceStatusTableSignal.connect(self.reset_model_1)
        self.MW2.ResetPriceStatusTableSignal.connect(self.reset_model_1)
        self.MW.SetProgressBarValue.connect(lambda cur, total: self.set_value_in_prigress_bar(cur, total, self.ProgressLabel_1,
                                                                                                       self.progressBar_1))
        self.MW2.SetProgressBarValue.connect(lambda cur, total: self.set_value_in_prigress_bar(cur, total, self.ProgressLabel_1_1,
                                                                                                       self.progressBar_1_1))
        self.MW.SetTotalTome.connect(lambda x: self.set_total_time_1(x, 0))
        self.MW2.SetTotalTome.connect(lambda x: self.set_total_time_1(x, 1))

        self.model_1 = QStandardItemModel()
        self.model_1.setHorizontalHeaderLabels(['Code', 'Status', 'Time', 'Total Time'])
        self.PriceStatusTableView_1.setModel(self.model_1)
        self.PriceStatusTableView_1.verticalHeader().hide()
        self.PriceStatusTableView_1.setEditTriggers(QTableView.NoEditTriggers)
        self.PriceStatusTableView_1.horizontalHeader().setStretchLastSection(True)
        self.PriceStatusTableView_1.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.timer_1 = [None, None]
        self.total__table_timer_1 = [None, None]
        self.total_timers_1 = [None, None]


        self.model_1_1 = QStandardItemModel()
        self.model_1_1.setHorizontalHeaderLabels(['Code', 'Info', 'Time'])
        items = [QStandardItem('') for _ in range(self.model_1.columnCount())]
        self.model_1.appendRow(items)
        self.model_1.appendRow(items)
        self.NotMatchedTableView_1.setModel(self.model_1_1)
        self.NotMatchedTableView_1.verticalHeader().hide()
        self.NotMatchedTableView_1.setEditTriggers(QTableView.NoEditTriggers)
        self.NotMatchedTableView_1.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)


        self.StartButton_2.clicked.connect(self.start_catalog_update)
        self.CU.SetButtonEnabledSignal.connect(lambda _: self.set_enabled_start_buttons(_, self.StartButton_2, self.PauseCheckBox_2))

        self.model_2_1 = QStandardItemModel()
        self.model_2_1.setHorizontalHeaderLabels(['Catalog', 'Time'])
        self.CatalogUpdateTimeTableView_2.setModel(self.model_2_1)
        self.CatalogUpdateTimeTableView_2.verticalHeader().hide()
        self.CatalogUpdateTimeTableView_2.setEditTriggers(QTableView.NoEditTriggers)
        self.CatalogUpdateTimeTableView_2.horizontalHeader().resizeSection(0, 170)
        self.CatalogUpdateTimeTableView_2.horizontalHeader().setStretchLastSection(True)

        self.model_2_2 = QStandardItemModel()
        self.model_2_2.setHorizontalHeaderLabels(['Code', 'Value'])
        self.CurrencyTableView_2.setModel(self.model_2_2)
        self.CurrencyTableView_2.verticalHeader().hide()
        self.CurrencyTableView_2.setEditTriggers(QTableView.NoEditTriggers)
        self.CurrencyTableView_2.horizontalHeader().resizeSection(0, 170)
        self.CurrencyTableView_2.horizontalHeader().setStretchLastSection(True)

        self.LogButton_2.clicked.connect(lambda _: self.open_dir(fr"logs\{Logs.log_file_names[2]}"))
        self.LogButton_2.setToolTip("Открыть файл с логами")
        self.ToReportDirButton_2.clicked.connect(lambda _: self.open_dir(settings_data['catalogs_dir']))
        self.ResetDBButton_2.clicked.connect(
            lambda _: self.confirmed_message_box('Обнуление БД', 'Обнулить данные в БД?', self.reset_db))
        self.AddDBButton_2.clicked.connect(self.add_db)
        self.CatalogUpdateTimeTableUpdateButton_2.clicked.connect(self.update_catalogs_update_time_table)
        self.CurrencyTableUpdateButton_2.clicked.connect(self.update_currency_table)
        self.CreateBasePriceButton_2.clicked.connect(self.update_base_price)
        self.CreateMassOffersButton_2.clicked.connect(self.update_mass_offers)
        self.OpenPriceStatusButton_2.clicked.connect(lambda _: self.open_dir(fr"{settings_data['catalogs_dir']}/price_report.csv"))
        self.ResetPriceStatusButton_2.clicked.connect(self.reset_price_1_report)
        self.CreatePriceStatusButton_2.clicked.connect(self.update_price_1_report_table)
        self.OpenMailReporButton_2.clicked.connect(lambda _: self.open_dir(fr"{settings_data['catalogs_dir']}/mail_report.csv"))
        self.ResetMailReporButton_2.clicked.connect(lambda _: self.confirmed_message_box('Обнуление отчёта', 'Обнулить отчёт?', self.reset_mail_report_confirmed))
        self.CreateMailReporButton_2.clicked.connect(self.start_update_mail_report_table)
        self.MB_SaveButton_2.clicked.connect(lambda _: self.save_MB_limit(1))



        self.model_3_1 = QStandardItemModel()
        self.model_3_1.setHorizontalHeaderLabels(['Code', 'Info', 'Time'])
        self.NotMatchedTableView_3.setModel(self.model_3_1)
        self.NotMatchedTableView_3.verticalHeader().hide()
        self.NotMatchedTableView_3.setEditTriggers(QTableView.NoEditTriggers)
        self.NotMatchedTableView_3.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)

        self.model_3 = QStandardItemModel()
        self.model_3.setHorizontalHeaderLabels(['Code', 'Status', 'Time', 'Total Time'])
        items = [QStandardItem('') for _ in range(self.model_3.columnCount())]
        self.model_3.appendRow(items)
        self.model_3.appendRow(items)
        self.PriceStatusTableView_3.setModel(self.model_3)
        self.PriceStatusTableView_3.verticalHeader().hide()
        self.PriceStatusTableView_3.setEditTriggers(QTableView.NoEditTriggers)
        self.PriceStatusTableView_3.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        self.LogButton_3.clicked.connect(lambda _: self.open_dir(fr"logs\{Logs.log_file_names[3]}"))
        self.ToFilesDirButton_3.clicked.connect(lambda _: self.open_dir(settings_data['exit_2_dir']))
        self.StartButton_3.clicked.connect(self.start_calculate)
        self.UpdateReportButton_3.clicked.connect(self.update_price_2_report_table)
        self.ToReportDirButton_3.clicked.connect(lambda _: self.open_dir(settings_data['catalogs_dir']))
        self.OpenReportButton_3.clicked.connect(lambda _: self.open_dir(fr"{settings_data['catalogs_dir']}/price_report.csv"))
        self.ResetPriceReportButton_3.clicked.connect(self.reset_price_1_report)
        self.Calculate.UpdatePriceStatusTableSignal.connect(lambda r_id, p, s, t: self.update_status_table_3(r_id, p, s, t))
        self.Calculate2.UpdatePriceStatusTableSignal.connect(lambda r_id, p, s, t: self.update_status_table_3(r_id, p, s, t))
        self.Calculate.UpdatePriceReportTableSignal.connect(self.update_price_2_report_table)
        self.Calculate.ResetPriceStatusTableSignal.connect(self.reset_model_3)
        self.Calculate2.ResetPriceStatusTableSignal.connect(self.reset_model_3)
        self.Calculate.TotalCountSignal.connect(lambda c: self.TotalCountLabel_2.setText("Всего позиций: {:,d}".format(c)))
        self.Calculate2.TotalCountSignal.connect(lambda c: self.TotalCountLabel_2.setText("Всего позиций: {:,d}".format(c)))
        self.Calculate.SetTotalTome.connect(lambda x: self.set_total_time_3(x, 0))
        self.Calculate2.SetTotalTome.connect(lambda x: self.set_total_time_3(x, 1))
        self.Calculate.SetProgressBarValue.connect(lambda cur, total: self.set_value_in_prigress_bar(cur, total, self.ProgressLabel_3,
                                                                                                       self.progressBar_3))
        self.Calculate2.SetProgressBarValue.connect(lambda cur, total: self.set_value_in_prigress_bar(cur, total, self.ProgressLabel_3_1,
                                                                                                       self.progressBar_3_1))
        self.timer_3 = [None, None]
        self.total__table_timer_3 = [None, None]
        self.total_timers_3 = [None, None]


        self.model_4 = QStandardItemModel()
        self.model_4.setHorizontalHeaderLabels(['Code', 'Status', 'Send time'])
        self.FinalPriceSendTableView_4.setModel(self.model_4)
        self.FinalPriceSendTableView_4.verticalHeader().hide()
        self.FinalPriceSendTableView_4.setEditTriggers(QTableView.NoEditTriggers)
        self.FinalPriceSendTableView_4.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)

        self.PriceSender.SetButtonEnabledSignal.connect(lambda _: self.set_enabled_start_buttons(_, self.StartButton_4, self.PauseCheckBox_4))
        self.PriceSender.SetProgressBarValue.connect(lambda cur, total: self.set_value_in_prigress_bar(cur, total, self.ProgressLabel_4,
                                                              self.progressBar_4))
        self.StartButton_4.clicked.connect(self.start_send)
        self.LogButton_4.clicked.connect(lambda _: self.open_dir(fr"logs\{Logs.log_file_names[4]}"))
        self.LogButton_4.setToolTip("Открыть файл с логами")
        self.SendCheckBox_4.checkStateChanged.connect(lambda status: self.setSendStatus(status))
        self.PriceSender.StartCreationSignal.connect(lambda b: self.set_total_time_4(b))
        self.PriceSender.UpdateReportSignal.connect(self.update_price_4_report_table)
        self.ToFilesDirButton_4.clicked.connect(lambda _: self.open_dir(settings_data['send_dir']))
        self.FinalPriceReportReset = FinalPriceReportReset(log=Log)
        self.ResetPriceReportButton_4.clicked.connect(self.reset_final_price_report)
        self.ToReportDirButton_4.clicked.connect(lambda _: self.open_dir(settings_data['catalogs_dir']))
        self.OpenReportButton_4.clicked.connect(
            lambda _: self.open_dir(fr"{settings_data['catalogs_dir']}/final_price_report.csv"))
        self.FinalPriceReportUpdate = FinalPriceReportUpdate(log=Log)
        self.FinalPriceReportUpdate.UpdateInfoTableSignal.connect(self.add_item_to_price_4_report_table)
        self.FinalPriceReportUpdate.UpdatePriceReportTime.connect(lambda x: self.TimeOfLastReportUpdatelabel_4.setText(x))
        self.UpdateReportButton_4.clicked.connect(self.update_price_4_report_table)


        times = CatalogUpdate.get_catalogs_time_update()
        time_edits = {"base_price_update": self.BasePriceTimeEdit_2, "mass_offers_update": self.MassOffersTimeEdit_2}
        if times:
            for t in times:
                h, m = str(times[t]).split(" ")
                time_edits[t].setTime(QTime(int(h), int(m)))


        self.TimeSaveButton_2.clicked.connect(self.save_catalogs_time_update)
        self.TgNTimeSaveButton_2.clicked.connect(self.save_tg_time)
        self.Update3_0_ConditionSaveButton_2.clicked.connect(self.save_upd_cond_3_time)


        self.timers = dict()
        self.main_timers = dict()
        self.totalFiles = 0
        self.doneFiles = 0

        self.ConsoleTextBrowser_0.document().setMaximumBlockCount(MAX_LOG_ROWS_IN_TEXT_BROWSER)
        self.ConsoleTextBrowser_1.document().setMaximumBlockCount(MAX_LOG_ROWS_IN_TEXT_BROWSER)
        self.ConsoleTextBrowser_2.document().setMaximumBlockCount(MAX_LOG_ROWS_IN_TEXT_BROWSER)
        self.ConsoleTextBrowser_3.document().setMaximumBlockCount(MAX_LOG_ROWS_IN_TEXT_BROWSER)
        self.ConsoleTextBrowser_4.document().setMaximumBlockCount(MAX_LOG_ROWS_IN_TEXT_BROWSER)
        self.consoles = {0: self.ConsoleTextBrowser_0, 1: self.ConsoleTextBrowser_1, 2: self.ConsoleTextBrowser_2,
                         3: self.ConsoleTextBrowser_3, 4: self.ConsoleTextBrowser_4}
        self.set_old_logs()

        self.x = 0
        self.dbWorker = None

        # AUTO START
        if self.autostart:
            self.start_mail_parser()
            self.start_mult()
            self.start_catalog_update()
            self.start_calculate()
            self.start_send()


    def reset_db(self, btn):
        if btn.text() == 'OK':
            try:
                with session() as sess:
                    # sess.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                    # sess.commit()
                    Base.metadata.drop_all(engine)
                    Base.metadata.create_all(engine)
                    sess.add_all([AppSettings(param="base_price_update", var="3 0"), AppSettings(param="mass_offers_update", var="3 0"),
                                  AppSettings(param="tg_notification_time", var="19 0"),
                                  AppSettings(param="last_tg_notification_time", var="2025-01-01 01:01:01"),
                                  AppSettings(param="last_DB_4_update", var="2 0"),
                                  AppSettings(param="last_tg_price_send", var="2025-01-01 01:01:01"),
                                  AppSettings(param="mb_limit_1", var="10"),
                                  AppSettings(param="mb_limit_2", var="25"),
                                  CatalogUpdateTime(catalog_name='Обновление данных в БД по 4.0', updated_at='2025-01-01 01:01:01'),
                                  CatalogUpdateTime(catalog_name='Базовая цена', updated_at='2025-01-01 01:01:01'),
                                  CatalogUpdateTime(catalog_name='Предложений в опте', updated_at='2025-01-01 01:01:01'),
                                  CatalogUpdateTime(catalog_name='Заказы', updated_at='2025-01-01 01:01:01'),
                                  ])
                    sess.commit()
                print('БД обновлена')
            except Exception as ex:
                print(ex)

    def add_db(self):
        try:
            with session() as sess:
                Base.metadata.create_all(engine)
                sess.commit()
            print('Таблицы в БД добавлены')
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

    def save_tg_time(self):
        if not self.STT:
            self.STT = CatalogUpdate.SaveTgTime(self.Tg_timeEdit_2.time(), log=Log)
            self.STT.start()
        elif not self.STT.isRunning():
            self.STT = CatalogUpdate.SaveTgTime(self.Tg_timeEdit_2.time(), log=Log)
            self.STT.start()

    def save_upd_cond_3_time(self):
        if not self.STU:
            self.STU = CatalogUpdate.SaveCond3Time(self.Update3_0_Condition_timeEdit_2.time(), log=Log)
            self.STU.start()
        elif not self.STU.isRunning():
            self.STU = CatalogUpdate.SaveCond3Time(self.Update3_0_Condition_timeEdit_2.time(), log=Log)
            self.STU.start()

    def save_MB_limit(self, module_id):
        SpinBoxes = [self.FileSizeLimit_spinBox_1, self.FileSizeLimit_spinBox_2]
        if not self.SMBV:
            self.SMBV = SaveMBVAlue(module_id, SpinBoxes[module_id].value(), log=Log)
            self.SMBV.start()
        elif not self.SMBV.isRunning():
            self.SMBV = SaveMBVAlue(module_id, SpinBoxes[module_id].value(), log=Log)
            self.SMBV.start()

    def update_price_1_report_table(self):
        if not self.PriceReportUpdate.isRunning():
            while self.model_1_1.rowCount() > 0:
                self.model_1_1.removeRow(self.model_1_1.rowCount() - 1)
            self.PriceReportUpdate.start()

    def update_price_2_report_table(self):
        if not self.PriceReportUpdate_2.isRunning():
            while self.model_3_1.rowCount() > 0:
                self.model_3_1.removeRow(self.model_3_1.rowCount() - 1)
            self.PriceReportUpdate_2.start()

    def update_price_4_report_table(self):
        if not self.FinalPriceReportUpdate.isRunning():
            while self.model_4.rowCount() > 0:
                self.model_4.removeRow(self.model_4.rowCount() - 1)
            self.FinalPriceReportUpdate.start()

    def reset_price_1_report(self):
        if not self.PriceReportReset.isRunning():
            self.PriceReportReset.start()

    def reset_final_price_report(self):
        if not self.FinalPriceReportReset.isRunning():
            self.FinalPriceReportReset.start()

    def add_item_to_price_1_report_table(self, report):
        if report:
            items = [QStandardItem(f"{r}") for r in report]
            self.model_1_1.appendRow(items)

    def add_item_to_price_4_report_table(self, report):
        if report:
            items = [QStandardItem(f"{r}") for r in report]
            self.model_4.appendRow(items)

    def add_item_to_price_2_report_table(self, report):
        if report:
            items = [QStandardItem(f"{r}") for r in report]
            self.model_3_1.appendRow(items)

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

    def reset_mail_report_unloaded_confirmed(self, btn):
        if btn.text() == 'OK':    # именно капсом
            if not self.MailReportResetUnloaded.isRunning():
                self.MailReportResetUnloaded.start()

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
        if not self.MW.isRunning():
            self.MW.start()
        if not self.MW2.isRunning():
            self.MW2.start()


    def start_mail_parser(self):
        if not self.MP:
            self.create_mail_parser_class()
            return
        if not self.MP.isRunning():
            self.create_mail_parser_class()
    def create_mail_parser_class(self):
        self.MP = MailParser.MailParserClass(log=Log)
        self.MP.SetButtonEnabledSignal.connect(lambda _: self.set_enabled_start_buttons(_, self.StartButton_0, self.PauseCheckBox_0))
        self.MP.UpdateReportSignal.connect(self.start_update_mail_report_table)
        self.MP.start()

    def set_total_time_4(self, start):
        if start:
            self.total_timer_4 = MyTimer()
            self.total_timer_4.SetTimeSignal.connect(lambda t: self.set_text_to_label(self.TotalTimeLabel_4, t))
            # self.total_timer.SetTimeSignal.connect(self.set_total_time_on_label)
        else:
            self.total_timer_4 = None
            # self.TotalTimeLabel_4.setText('[0:00:00]')

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

    def update_base_price(self):
        if not self.CU.isRunning():
            self.CU.update_base_price(force_update=True)

    def update_mass_offers(self):
        if not self.CU.isRunning():
            self.CU.update_mass_offers(force_update=True)

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

    def start_calculate(self):
        if not self.Calculate.isRunning():
            self.Calculate.start()
        if not self.Calculate2.isRunning():
            self.Calculate2.start()

    # def start_create_total_csv(self):
    #     if not self.CreateTotalCsv.isRunning():
    #         self.CreateTotalCsv.start()

    def update_status_table_1(self, row_id, price_code, status, total_timer):
        self.model_1.setData(self.model_1.index(row_id, 0), f"{price_code}")
        self.model_1.setData(self.model_1.index(row_id, 1), f"{status}")
        self.timer_1[row_id] = MyTimer(row_id, 2)
        self.model_1.setData(self.model_1.index(row_id, 2), self.timer_1[row_id])
        self.timer_1[row_id].SetTimeInTableSignal.connect(self.set_time_1)
        if total_timer:
            self.total__table_timer_1[row_id] = MyTimer(row_id, 3)
            self.model_1.setData(self.model_1.index(row_id, 3), self.total__table_timer_1[row_id])
            self.total__table_timer_1[row_id].SetTimeInTableSignal.connect(self.set_time_1)

    def reset_model_1(self, row_id):
        self.timer_1[row_id] = None
        self.total__table_timer_1[row_id] = None
        for c in range(self.model_1.columnCount()):
            self.model_1.setItem(row_id, c, QStandardItem(''))

    def set_time_1(self, r, c, str_time):
        self.model_1.setData(self.model_1.index(r, c), str_time)

    def set_total_time_1(self, start, type_id):
        TotalTimeLabels_1 = [self.TotalTimeLabel, self.TotalTimeLabel_1_1]
        if start:
            self.total_timers_1[type_id] = MyTimer()
            self.total_timers_1[type_id].SetTimeSignal.connect(lambda t: self.set_text_to_label(TotalTimeLabels_1[type_id], t))
        else:
            self.total_timers_1[type_id] = None
            TotalTimeLabels_1[type_id].setText(TotalTimeLabels_1[type_id].text())

    def update_status_table_3(self, row_id, price_code, status, total_timer):
        self.model_3.setData(self.model_3.index(row_id, 0), f"{price_code}")
        self.model_3.setData(self.model_3.index(row_id, 1), f"{status}")
        self.timer_3[row_id] = MyTimer(row_id, 2)
        self.model_3.setData(self.model_3.index(row_id, 2), self.timer_3[row_id])
        self.timer_3[row_id].SetTimeInTableSignal.connect(self.set_time_3)
        if total_timer:
            self.total__table_timer_3[row_id] = MyTimer(row_id, 3)
            self.model_3.setData(self.model_3.index(row_id, 3), self.total__table_timer_3[row_id])
            self.total__table_timer_3[row_id].SetTimeInTableSignal.connect(self.set_time_3)

    def set_time_3(self, r, c, str_time):
        self.model_3.setData(self.model_3.index(r, c), str_time)

    def reset_model_3(self, row_id):
        self.timer_3[row_id] = None
        self.total__table_timer_3[row_id] = None
        for c in range(self.model_3.columnCount()):
            self.model_3.setItem(row_id, c, QStandardItem(''))

    def set_value_in_prigress_bar(self, cur, total, ProgressLabel, progressBar):
        ProgressLabel.setText(f"{cur}/{total}")
        progressBar.setValue(cur/total*100)

    def set_total_time_3(self, start, type_id):
        TotalTimeLabels_3 = [self.TotalTimeLabel_3, self.TotalTimeLabel_3_1]
        if start:
            self.total_timers_3[type_id] = MyTimer()
            self.total_timers_3[type_id].SetTimeSignal.connect(lambda t: self.set_text_to_label(TotalTimeLabels_3[type_id], t))
        else:
            self.total_timers_3[type_id] = None
            TotalTimeLabels_3[type_id].setText(TotalTimeLabels_3[type_id].text())

    def start_send(self):
        if not self.PriceSender.isRunning():
            self.PriceSender.start()

    def set_text_to_label(self, label, text):
        label.setText(text)

    def setPause(self, state, some_class):
        if some_class:
            some_class.isPause = (state == Qt.CheckState.Checked)

    def setSendStatus(self, state):
        self.PriceSender.need_to_send = (state != Qt.CheckState.Checked)

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
    autostart = False
    if len(sys.argv) > 1:
        autostart = bool(sys.argv[1])

    app = QApplication(sys.argv)
    window = MainWindow(autostart)
    window.show()
    app.exec()


if __name__ == '__main__':
    main()