from sqlalchemy import create_engine, URL, text
import os
import time
ip = None
user = None
password = None
db_name = None

properties = ["ip:\n", "user:\n", "password:\n", "db_name:\n"]

def get_engine():
    if not 'Settings.txt' in os.listdir():
        with open('Settings.txt', 'w', encoding='utf-8') as settings_file:
            settings_file.writelines(properties)

    with open('Settings.txt', 'r', encoding='utf-8') as settings:
        data = [settings.readline().lstrip(properties[i]).strip() for i in range(len(properties))]
        ip, user, password, db_name = data

    url = URL.create("postgresql+psycopg2", username=user, password=password, host=ip, database=db_name)
    engine = create_engine(url)
    return engine

    # with engine.connect() as con:
    #     res = con.execute(text("select count(*) from data07")).scalar()
    #     print(f"{res=}")

    # dirs = [r'Итог', r'pre Итог']
    # for d in dirs:
    #     if not os.path.exists(d):
    #         os.mkdir(d)

