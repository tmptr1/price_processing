import logging
import time
from logging.handlers import RotatingFileHandler
from PySide6.QtCore import Signal, QThread
import datetime
import shutil

import setting
settings_data = setting.get_vars()

log_file_names = ["logs_mail_parser_molule.log", "logs_price_reader_molule.log", "logs_catalog_update_molule.log",
                  "logs_calcutate_molule.log"]
log_files_local = []
log_files_server = []
for l in log_file_names:
    log_files_local.append(fr"logs/{l}")
    log_files_server.append(fr"{settings_data['server_logs_dir']}/{l}")

loggers = []

for log_file in log_files_local:
    # logger = mp.get_logger()
    logger = logging.getLogger(log_file)
    logger.setLevel(21)

    formater = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    f_handler = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=2, errors='ignore',)
    f_handler.setFormatter(formater)
    # s_handler = logging.StreamHandler()
    # s_handler.setFormatter(formater)

    logger.addHandler(f_handler)
    # logger.addHandler(s_handler)
    loggers.append(logger)


class LogClass(QThread):
    AddLogToTableSignal = Signal(int, str)
    def __init__(self, parent=None):
        self.loggers = loggers
        QThread.__init__(self, parent)

    def run(self):
        '''Перенос логов с локальной директории на серверную папку, где расположено приложение'''
        while True:
            time.sleep(120)
            try:
                for i, l in enumerate(log_files_local):
                    shutil.copy(l, log_files_server[i])
                # print('logs ok')
            except Exception as ex:
                print(ex)

    def add(self, id_console_log, text:str, color_text=None):
        try:
            self.loggers[id_console_log].log(21, f"{text}")
        except Exception as log_ex:
            print("log add error:", log_ex)
        self.AddLogToTableSignal.emit(id_console_log, f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {color_text or text}  ")

    def error(self, id_console_log, er_custom_message, ex_text):
        try:
            self.loggers[id_console_log].error(f"{er_custom_message}: {ex_text}")
        except Exception as log_ex:
            print("log er error:", log_ex)
        self.AddLogToTableSignal.emit(id_console_log, f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] <span style='color:red;font-weight:bold;'>"
                                      f"{er_custom_message}</span>:<br> <span style='color:#cc2f2f'>{ex_text}</span><br>")



def add_log_cf(id_console_log, log_main_text, sender, price_code, color, cur_time=None):
    '''Custom Format'''
    if cur_time:
        log_text = "{}{price}{} {log_main_text} [{time_spent}]"
        sender.send(["log", id_console_log, log_text.format('', '', price=price_code, log_main_text=log_main_text,
                                            time_spent=str(datetime.datetime.now() - cur_time)[:7]),
                     log_text.format(f"<span style='background-color:hsl({color[0]}, {color[1]}%, {color[2]}%);'>", '</span>',
                                     price=price_code, log_main_text=log_main_text,
                                     time_spent=str(datetime.datetime.now() - cur_time)[:7])])
    else:
        log_text = "{}{price}{} {log_main_text}"
        sender.send(["log", id_console_log, log_text.format('', '', price=price_code, log_main_text=log_main_text),
                     log_text.format(f"<span style='background-color:hsl({color[0]}, {color[1]}%, {color[2]}%);'>",
                                     '</span>', price=price_code, log_main_text=log_main_text)])