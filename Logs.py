import multiprocessing as mp
import logging
from logging.handlers import RotatingFileHandler
from PySide6.QtCore import Signal, QObject
import datetime

logger = mp.get_logger()
logger.setLevel(21)

formater = logging.Formatter("[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

f_handler = RotatingFileHandler('logs_calculate_molule.log', maxBytes=10 * 1024 * 1024, backupCount=2, errors='ignore',)
f_handler.setFormatter(formater)
s_handler = logging.StreamHandler()
s_handler.setFormatter(formater)

logger.addHandler(f_handler)
logger.addHandler(s_handler)

class LogClass(QObject):
    AddLogToTableSignal = Signal(int, str)
    def __init__(self, parent=None):
        QObject.__init__(self)
        self.loggers = [logger]

    def add(self, id_console_log, text:str, color_text=None):
        self.loggers[id_console_log].log(21, f"{text}")
        self.AddLogToTableSignal.emit(id_console_log, f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {color_text or text}")

    def error(self, id_console_log, exc, er_text, ex_text):
        self.loggers[id_console_log].error(f"{er_text}", exc_info=exc)
        self.AddLogToTableSignal.emit(id_console_log, f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] <span style='color:red;font-weight:bold;'>"
                                      f"{er_text}</span>:<br> <span style='color:#cc2f2f'>{ex_text}</span>")



def add_log_cf(id_console_log, log_main_text, sender, price_code, cur_time, color):
    '''Custom Format'''
    log_text = "{}{price}{} {log_main_text} [{time_spent}]"
    sender.send(["log", id_console_log, log_text.format('', '', price=price_code, log_main_text=log_main_text,
                                        time_spent=str(datetime.datetime.now() - cur_time)[:7]),
                 log_text.format(f"<span style='background-color:hsl({color[0]}, {color[1]}%, {color[2]}%);'>", '</span>',
                                 price=price_code, log_main_text=log_main_text,
                                 time_spent=str(datetime.datetime.now() - cur_time)[:7])])