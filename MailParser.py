from PySide6.QtCore import QThread, Signal
import time
import traceback
import os
import datetime
import imaplib
import email
from email.header import decode_header
import shutil
import chardet
from zipfile import ZipFile
import aspose.zip as az
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
from models import FileSettings, MailReport, MailReportUnloaded, CatalogUpdateTime
from sqlalchemy import select, delete, insert, and_
import pandas as pd
import colors

import setting
engine = setting.get_engine()
session = sessionmaker(engine)
settings_data = setting.get_vars()

LOG_ID = 0
REPORT_FILE = r"mail_report_unloaded.csv"

class MailParserClass(QThread):
    SetButtonEnabledSignal = Signal(bool)
    UpdateReportSignal = Signal(bool)
    isPause = None
    letters_b = set()

    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)


    def run(self):
        global session, engine
        self.SetButtonEnabledSignal.emit(False)
        self.log.add(LOG_ID, "Старт", f"<span style='color:{colors.green_log_color};'>Старт</span>  ")
        wait_sec = 180

        while not self.isPause:
            # self.delete_irrelevant_prices()
            # self.UpdateReportSignal.emit(True)
            start_cycle_time = datetime.datetime.now()
            self.check_since = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%d-%b-%Y")
            self.now_time = datetime.datetime.now().strftime("%d-%b-%Y")
            try:
                mail = imaplib.IMAP4_SSL(host='imap.yandex.ru', port=993)
                mail.login(settings_data['mail_login'], settings_data['mail_imap_password'])
                mail.select("inbox")
                # self.get_mail("86693", mail)
                # self.get_mail("86422", mail)
                # self.get_mail("86854", mail)
                # self.get_mail("94946", mail)
                # self.get_mail("97738", mail)
                # self.get_mail("97739", mail)
                # self.get_mail("97390", mail)
                # return
                _, res = mail.uid('search', '(SINCE "' + self.check_since + '")', "ALL")
                letters_id = res[0].split()[:]

                if letters_id:
                    if letters_id[0] == b'[UNAVAILABLE]':
                        self.log.add(LOG_ID, "UNAVAILABLE")
                        time.sleep(wait_sec)
                        continue

                    for i in letters_id:
                        if self.isPause:
                            self.log.add(LOG_ID, "Пауза", f"<span style='color:{colors.orange_log_color};'>Пауза</span>  ")
                            self.SetButtonEnabledSignal.emit(True)
                            return

                        if i not in self.letters_b:
                            self.letters_b.add(i)
                            # logger.info(str(i))
                            self.log.add(LOG_ID, f"{i}")
                            self.get_mail(i, mail)  # получение необходимых файлов из письма

                    if self.now_time < (datetime.datetime.now()).strftime("%d-%b-%Y"):
                        self.now_time = (datetime.datetime.now()).strftime("%d-%b-%Y")
                        self.check_since = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%d-%b-%Y")

                        _, yesterday_id_list = mail.uid('search', '(SINCE "' + (
                                datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
                            "%d-%b-%Y") + '" BEFORE "' + datetime.datetime.now().strftime(
                            "%d-%b-%Y") + '")', "ALL")
                        self.letters_b = set(yesterday_id_list[0].split()[:])

                        # обновление времени при наутуплении следующего дня
                        self.log.add(LOG_ID, f"{'=' * 35}\nОбновление времени\n{'=' * 35}")
            except (OperationalError, UnboundExecutionError) as db_ex:
                self.log.add(LOG_ID, f"Повторное подключение к БД ...",  f"<span style='color:{colors.orange_log_color};"
                                                                         f"font-weight:bold;'>Повторное подключение к БД ...</span>  ")
                try:
                    engine = setting.get_engine()
                    session = sessionmaker(engine)
                except:
                    pass
            except Exception as ex:
                ex_text = traceback.format_exc()
                self.log.error(LOG_ID, "ERROR", ex_text)

            self.UpdateReportSignal.emit(True)

            # проверка на паузу
            finish_cycle_time = datetime.datetime.now()
            if wait_sec > (finish_cycle_time - start_cycle_time).seconds:
                for _ in range(wait_sec - (finish_cycle_time - start_cycle_time).seconds):
                    if self.isPause:
                        break
                    time.sleep(1)
        else:
            self.SetButtonEnabledSignal.emit(True)

    # def delete_irrelevant_prices(self):
    #     try:
    #         with session() as sess:
    #             price_list = set(sess.execute(select(FileSettings.price_code).where(func.upper(FileSettings.save) == 'ДА')).scalars().all())
    #             # print(price_list)
    #         for price in os.listdir(f"{settings_data['mail_files_dir']}"):
    #             price_code = price[:4]
    #             if price_code not in price_list:
    #                 os.remove(fr"{settings_data['mail_files_dir']}\{price}")
    #
    #     except Exception as del_ip_ex:
    #         ex_text = traceback.format_exc()
    #         self.log.error(LOG_ID, "ERROR", ex_text)

    def get_mail(self, id, mail):
        try:
            _, res = mail.uid('fetch', id, "(RFC822)")
            raw_email = res[0][1]
            msg = email.message_from_string(raw_email.decode("utf-8"))
            # msg = email.message_from_bytes(raw_email)
            # print(msg.__dict__)
            sender = None
            try:
                msg_from = msg['From'].split(' ')
                if len(msg_from) > 1:
                    sender = msg_from[-1][1:-1]
                else:
                    sender = msg['From']
                if len(sender) > 1 and sender[0] == '<':
                    sender = sender[1:]
                if len(sender) > 1 and sender[-1] == '>':
                    sender = sender[:-1]
                # print(sender)
            except:
                pass

            if sender:
                pass
            elif msg['X-Envelope-From']:
                sender = msg['X-Envelope-From']
            else:
                sender = msg['Return-path']

            # удаление папки для временных файлов из архивов
            tmp_dir = r"tmp"
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

            # удаление старых архивов
            tmp_archive_dir = r'Archives'
            for f in os.listdir(tmp_archive_dir):
                os.remove(f"{tmp_archive_dir}/{f}")

            # logger.info(f"Sender: {sender}")
            self.log.add(LOG_ID, f"Sender: {sender}")
            try:
                header = decode_header(msg['Subject'])[0][0].decode()
            except:
                header = decode_header(msg['Subject'])[0][0]
            # logger.info(f"Header: {header}")
            self.log.add(LOG_ID, f"Header: {header}")

            rcv = msg.get('Received')
            received_time = rcv.split(',')[-1].strip().replace(':', '-').split()[:-1]
            received_time = ' '.join(received_time)
            # logger.info(f"Date: {received_time}")
            self.log.add(LOG_ID, f"Date: {received_time}")
            received_time = datetime.datetime.strptime(str(received_time), "%d %b %Y %H-%M-%S")

            with session() as sess:
                # sender = sender.replace('\'', '\'\'')
                # req = select(SupplierPriceSettings.file_name, SupplierPriceSettings.file_name_cond,
                #              SupplierPriceSettings.price_code
                #              ).where(
                #     and_(func.lower(SupplierPriceSettings.email) == str.lower(sender),
                #          SupplierPriceSettings.save == "ДА"))
                # db_data = sess.execute(req).all()

                req = select(FileSettings.file_name, FileSettings.file_name_cond, FileSettings.price_code
                             ).where(
                    and_(func.lower(FileSettings.email) == str.lower(sender), FileSettings.save == "ДА"))
                # FileSettings.price_code == FileSettings.price_code,
                db_data = sess.execute(req).all()

            # connection = psycopg2.connect(host=host, user=user, password=password, database=db_name)
            # with connection.cursor() as cur:
            #     sender = sender.replace('\'', '\'\'')
            #     cur.execute(
            #         f"SELECT Имя_файла, Условие_имени_файла, Код_прайса FROM Настройки_прайса_поставщика WHERE LOWER(Почта) = LOWER('{sender}') and Сохраняем = 'ДА'")
            #     data = cur.fetchall()
            # connection.close()
                if not db_data:
                    # logger.info('Не подходит')
                    # logger.info('=' * 20)
                    self.log.add(LOG_ID, f"Не подходит\n{'*'*35}", f"<span style='color:{colors.orange_log_color};'>Не подходит</span><br>"
                                                                   f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]{'*'*35}")
                    sess.query(MailReportUnloaded).where(and_(MailReportUnloaded.sender == sender,
                                                              MailReportUnloaded.file_name == '')).delete()
                    sess.add(MailReportUnloaded(sender=sender, file_name='', date=received_time))
                    sess.commit()
                    # create csv report + delete report button
                    return

                # print(data)
                self.load_content(msg, tmp_archive_dir, tmp_dir, db_data, sender, received_time)

            # logger.info('=' * 20)
            self.log.add(LOG_ID, "*" * 35)
        except Exception as ex:
            # logger.error("get_mail error:", exc_info=ex)
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, "get_mail Error", ex_text)

    def load_content(self, message, tmp_archive_dir, tmp_dir, db_data, sender, received_time):
        '''Скачивает необходимые файлы, в случае с архивами, скачивает полный архив в папку Archives, далее распаковывает
        его в папку tmp, нужные файлы переносит в папку для сырых прайсов'''
        # received_time = datetime.datetime.strptime(str(received_time), "%d %b %Y %H-%M-%S")

        for part in message.walk():
            if part.get_content_disposition() == 'attachment':

                if part.get_content_maintype() != 'multipart' and part.get('Content-Disposition') is not None:
                    try:
                        enc = chardet.detect(decode_header(part.get_filename())[0][0])['encoding']
                        # print(enc)
                        name = decode_header(part.get_filename())[0][0].decode(enc)
                        # print(name)
                    except:
                        name = part.get_filename()

                    self.log.add(LOG_ID, f"{name}")

                    loaded = False
                    file_format = name.split('.')[-1]

                    with session() as sess:
                        # обработка архивов
                        if file_format in ('zip', 'rar'):
                            path_to_archieve = f'{tmp_archive_dir}/{name}'
                            open(path_to_archieve, 'wb').write(part.get_payload(decode=True))
                            if not os.path.exists(tmp_dir):
                                os.mkdir(tmp_dir)
                            if file_format == 'zip':
                                with ZipFile(path_to_archieve, 'r') as archive:
                                    archive.extractall(path=tmp_dir)
                            elif file_format == 'rar':
                                with az.rar.RarArchive(path_to_archieve) as archive:
                                    archive.extract_to_directory(tmp_dir)
                            os.remove(path_to_archieve)

                            for f in os.listdir(tmp_dir):
                                for d_name in db_data:
                                    if self.check_file_name(f, d_name[0], d_name[1]):
                                        price_code = str(d_name[2])
                                        addition = f"{d_name[0]}.{f.split('.')[-1]}"
                                        if d_name[1] == 'Равно + расширение':
                                            addition = ".".join(addition.split('.')[:-1])

                                        price_from_db = sess.execute(select(MailReport).where(
                                            and_(MailReport.sender == sender,
                                                 MailReport.file_name == addition))).scalar()

                                        if price_from_db:
                                            # print(f"{price_from_db.date=}")
                                            if received_time <= price_from_db.date:
                                                self.log.add(LOG_ID, f"- ({price_code}) - {addition}",
                                                             f"- ({price_code}) - {addition}")
                                                loaded = True
                                                continue

                                        self.del_duplicates(price_code, id)

                                        shutil.move(fr"{tmp_dir}/{f}", fr"{settings_data['mail_files_dir']}\{price_code} {addition}")
                                        shutil.copy(fr"{settings_data['mail_files_dir']}\{price_code} {addition}",
                                                    fr"{settings_data['mail_files_dir_copy']}\{price_code} {addition}")
                                        # logger.info(f"+ ({price_code}) - {f}")
                                        self.log.add(LOG_ID, f"+ ({price_code}) - {f}", f"✔ (<span style='color:{colors.green_log_color};"
                                                                                        f"font-weight:bold;'>{price_code}</span>) - {f}")

                                        sess.query(MailReport).where(
                                            and_(MailReport.sender == sender, MailReport.file_name == addition)).delete()
                                        sess.add(
                                            MailReport(price_code=price_code, sender=sender, file_name=addition, date=received_time))
                                        sess.commit()
                                        loaded = True
                                        break
                                    # cur.execute(f"delete from mail_report_tab where sender = '{sender}' and file_name = '{f}'")
                                    # cur.execute(f"insert into mail_report_tab values('{sender}', '{f}', '{received_time}')")
                            if not loaded:
                                sess.query(MailReportUnloaded).where(and_(MailReportUnloaded.sender == sender,
                                                                          MailReportUnloaded.file_name == f)).delete()
                                sess.add(MailReportUnloaded(sender=sender, file_name=f, date=received_time))
                                sess.commit()

                            shutil.rmtree(tmp_dir)
                            continue

                        # обработка других файлов
                        for d_name in db_data:
                            if self.check_file_name(name, d_name[0], d_name[1]):
                                price_code = str(d_name[2])
                                addition = f"{d_name[0]}.{name.split('.')[-1]}"
                                if d_name[1] == 'Равно + расширение':
                                    addition = ".".join(addition.split('.')[:-1])

                                price_from_db = sess.execute(select(MailReport).where(
                                    and_(MailReport.sender == sender, MailReport.file_name == addition))).scalar()

                                if price_from_db:
                                    # print(f"{price_from_db.date=}")
                                    if received_time <= price_from_db.date:
                                        self.log.add(LOG_ID, f"- ({price_code}) - {name}",
                                                     f"- ({price_code}) - {name}")
                                        loaded = True
                                        continue

                                self.del_duplicates(price_code, id)

                                open(fr"{settings_data['mail_files_dir']}\{price_code} {addition}", 'wb').write(part.get_payload(decode=True))
                                shutil.copy(fr"{settings_data['mail_files_dir']}\{price_code} {addition}",
                                            fr"{settings_data['mail_files_dir_copy']}\{price_code} {addition}")

                                self.log.add(LOG_ID, f"+ ({price_code}) - {name}", f"✔ (<span style='color:{colors.green_log_color};"
                                                                                        f"font-weight:bold;'>{price_code}</span>) - {name}")

                                sess.query(MailReport).where(
                                    and_(MailReport.sender == sender, MailReport.file_name == addition)).delete()
                                sess.add(
                                    MailReport(price_code=price_code, sender=sender, file_name=addition, date=received_time))
                                sess.commit()
                                loaded = True
                                break

                        if not loaded:
                            sess.query(MailReportUnloaded).where(and_(MailReportUnloaded.sender == sender,
                                                                      MailReportUnloaded.file_name == name)).delete()
                            sess.add(MailReportUnloaded(sender=sender, file_name=name, date=received_time))
                            sess.commit()
                            #     sess.query(MailReportUnloaded).where(MailReportUnloaded.sender == sender).delete()
                            #     sess.add(MailReportUnloaded(sender=sender, file_name=name, date=received_time))
                            #     sess.commit()

                        # if not is_loaded:
                            # cur.execute(f"delete from mail_report_tab where sender = '{sender}' and file_name = '{name}'")
                            # cur.execute(f"insert into mail_report_tab values('{sender}', '{name}', '{received_time}')")

    def del_duplicates(self, file_code, id):
        '''Удаляет файл при совпадении первых 4 символов (код прайса)'''
        try:
            for i in os.listdir(settings_data['mail_files_dir']):
                try:
                    if ".".join(i.split('.')[:-1])[:4] == file_code:
                        os.remove(f"{settings_data['mail_files_dir']}/{i}")

                except Exception as e:
                    if id in self.letters_b:
                        self.letters_b.remove(id)
                    # logger.error(f"del error ({file_code}):", exc_info=e)
                    ex_text = traceback.format_exc()
                    self.log.error(LOG_ID, f"del Error ({file_code})", ex_text)

        except Exception as ex:
            # logger.error("del_duplicates error:", exc_info=ex)
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"del_duplicates Error", ex_text)

    def check_file_name(self, file_name, file_db_name, type):
        try:
            if not file_db_name:
                return 0

            if type == 'Равно + расширение':
                if file_name == file_db_name:
                    return 1
            elif type == 'Равно':
                return 1 if ".".join(file_name.split('.')[:-1]) == file_db_name else 0
            elif type == 'Содержит':
                return 1 if file_db_name in file_name else 0
            elif type == 'Начинается':
                if len(file_name) < len(file_db_name):
                    return 0
                return 1 if file_name[:len(file_db_name)] == file_db_name else 0
            elif type == 'Заканчивается':
                if len(file_name) < len(file_db_name):
                    return 0
                return 1 if file_name[-len(file_db_name):] == file_db_name else 0
            else:
                return 0

        except Exception as ex:
            # logger.error("Error:", exc_info=ex)
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"check_file_name Error", ex_text)
            return 0

class MailReportDelete(QThread):
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)

    def run(self):
        try:
            with session() as sess:
                sess.query(MailReport).delete()
                sess.commit()
            self.log.add(LOG_ID, f"Отчёт обнулён", f"<span style='color:{colors.green_log_color};'>Отчёт обнулён</span>  ")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"MailReportDelete Error", ex_text)


class MailReportUnloadedDelete(QThread):
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)

    def run(self):
        try:
            with session() as sess:
                sess.query(MailReportUnloaded).delete()
                sess.commit()
            self.log.add(LOG_ID, f"Отчёт обнулён", f"<span style='color:{colors.green_log_color};'>Отчёт обнулён</span>  ")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"MailReportDelete Error", ex_text)

class MailReportUpdate(QThread):
    # UpdateInfoTableSignal = Signal(str, str, str, str)
    UpdateInfoTableSignal = Signal(str, str, str)
    UpdateMailReportTime = Signal(str)
    # first_start = True
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)

    def run(self):
        try:
            # with session() as sess:
            #     req = select(MailReport.price_code.label("Price code"), MailReport.sender.label("Sender"),
            #                  MailReport.file_name.label("File name"), MailReport.date.label("Date"))
            #     db_data = sess.execute(req).all()
            #     for i in db_data:
            #         self.UpdateInfoTableSignal.emit(i[0], i[1], i[2], str(i[3]))
            #
            #     sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == 'Отчёт почта').delete()
            #     sess.add(CatalogUpdateTime(catalog_name='Отчёт почта', updated_at=str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))
            #     sess.commit()
            #
            # self.UpdateMailReportTime.emit(str(datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))
            #
            # # if not self.first_start:
            # df = pd.read_sql(req, engine)
            # df.to_csv(fr"{settings_data['catalogs_dir']}/{REPORT_FILE}", sep=';', encoding="windows-1251",
            #           index=False, header=True, errors='ignore') # ['Sender', 'File name', 'Date']

            with session() as sess:
                req = select(MailReportUnloaded.sender.label("Sender"),
                             MailReportUnloaded.file_name.label("File name"), MailReportUnloaded.date.label("Date"))
                db_data = sess.execute(req).all()
                for i in db_data:
                    self.UpdateInfoTableSignal.emit(i[0], i[1], str(i[2]))

                sess.query(CatalogUpdateTime).filter(CatalogUpdateTime.catalog_name == 'Отчёт несохранённые файлы').delete()
                sess.add(CatalogUpdateTime(catalog_name='Отчёт несохранённые файлы', updated_at=str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))
                sess.commit()

            self.UpdateMailReportTime.emit(str(datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S")))

            # if not self.first_start:
            df = pd.read_sql(req, engine)
            df.to_csv(fr"{settings_data['catalogs_dir']}/{REPORT_FILE}", sep=';', encoding="windows-1251",
                      index=False, header=True, errors='ignore') # ['Sender', 'File name', 'Date']

            # self.first_start = False

            # self.log.add(LOG_ID, f"Отчёт обновлён", f"<span style='color:{colors.green_log_color};'>Отчёт обновлён</span>  ")
        except Exception as ex:
            ex_text = traceback.format_exc()
            self.log.error(LOG_ID, f"MailReportUpdate Error", ex_text)
