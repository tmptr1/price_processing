from sqlalchemy import create_engine, URL, text
import os
import time

properties = ["ip:", "user:", "password:", "db_name:", "mail_login:", "mail_imap_password:", "mail_files_dir:", "mail_files_dir_copy:", "catalogs_dir:",
              "local_logs_dir:", "exit_1_dir:"]

def check_settings_file():
    if not 'Settings.txt' in os.listdir():
        with open('Settings.txt', 'w', encoding='utf-8') as settings_file:
            settings_file.writelines('\n'.join(properties))

def get_engine():
    try:
        data = get_vars()
        url = URL.create("postgresql+psycopg2", username=data["user"], password=data["password"], host=data["ip"],
                         database=data["db_name"])
        engine = create_engine(url)

        engine.connect()
    except Exception as ex:
        print(ex)
        return False
    return engine

def get_vars():
    with open('Settings.txt', 'r', encoding='utf-8') as settings:
        data = {}
        # {settings.readline().split(':')[0]:settings.readline().lstrip(properties[i]) for i in range(len(properties))}
        for i in range(len(properties)):
            line = settings.readline()
            data[line.strip().split(':')[0]] = line.lstrip(properties[i]).strip() or None

        # print(f"{data=}")
        return data

def create_dirs():
    dirs = [r'logs', r'Archives']
    for d in dirs:
        if not os.path.exists(d):
            os.mkdir(d)

