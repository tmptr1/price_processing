import time
from PySide6.QtCore import QThread, Signal
from sqlalchemy import text, select, delete, insert, update, and_, not_, func, distinct, or_, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, UnboundExecutionError
from models import TotalPrice_2, FinalPrice, Base3, SaleDK, BuyersForm, Data07, PriceException, SuppliersForm
import traceback
import datetime

import colors
import setting
engine = setting.get_engine()
session = sessionmaker(engine)
settings_data = setting.get_vars()

LOG_ID = 4
WEEKDAYS = {0: "пон", 1: "втор", 2: "сре", 3: "чет", 4: "пят", 5: "суб", 6: "вос"}

# из "Анкета покупателя" взять прайсы (Имя прайса) если "Включен?" == "да"
# сначала формировать прайсы, где "Срок" == 1
# сопоставить "Наименование" в Справочник_Бренд3
# "Куда запрещено" "Код покупателя" (%PLG%) and "Примечание" == "Действует в экселе"
#

class Sender(QThread):
    SetButtonEnabledSignal = Signal(bool)
    isPause = None
    def __init__(self, log=None, parent=None):
        self.log = log
        QThread.__init__(self, parent)
    def run(self):
        global session, engine
        self.SetButtonEnabledSignal.emit(False)
        wait_sec = 2

        while not self.isPause:
            start_cycle_time = datetime.datetime.now()
            try:
                inspct = inspect(engine)
                if inspct.has_table(FinalPrice.__tablename__):
                    FinalPrice.__table__.drop(engine)

                Base3.metadata.create_all(engine)

                name = "2 Прайс Профит-Лига"

                with session() as sess:
                    self.price_settings = sess.execute(select(BuyersForm).where(BuyersForm.price_name==name)).scalar()

                    allow_prices = set(sess.execute(select(Data07.setting).where(Data07.access_pp.like(f"%{self.price_settings.buyer_price_code}%"))).scalars().all())
                    print(allow_prices)
                    weekday = WEEKDAYS[datetime.datetime.now().weekday()]
                    # weekday = 'чет'
                    # ДО 14 00 !!!

                    allow_prices_wd = set(sess.execute(select(SuppliersForm.setting).where(SuppliersForm.days.like(f"%{weekday}%"))).scalars().all())
                    print(allow_prices_wd)
                    allow_prices = allow_prices & allow_prices_wd


                    cols_for_price = [TotalPrice_2._01article, TotalPrice_2._03name, TotalPrice_2._05price_plus,
                                      TotalPrice_2._06mult_new, TotalPrice_2._07supplier_code, TotalPrice_2._13grad,
                                      TotalPrice_2._14brand_filled_in, TotalPrice_2._15code_optt, TotalPrice_2._17code_unique,
                                      TotalPrice_2._18short_name, TotalPrice_2.delay, TotalPrice_2.sell_for_OS,
                                      TotalPrice_2.markup_os, TotalPrice_2.markup_R, TotalPrice_2.markup_pb,
                                      TotalPrice_2.min_markup, TotalPrice_2.min_wholesale_markup, TotalPrice_2.grad_step,
                                      TotalPrice_2.wh_step, TotalPrice_2.access_pp, TotalPrice_2.put_away_zp,
                                      TotalPrice_2.offers_wh, TotalPrice_2.price_b, TotalPrice_2.count,
                                      TotalPrice_2.code_pb_p, TotalPrice_2.mult_less, TotalPrice_2.buy_count,
                                      TotalPrice_2.unload_percent, TotalPrice_2.min_price, TotalPrice_2.to_price]
                    cols_for_price = {i: i.__dict__['name'] for i in cols_for_price}
                    price = select(*cols_for_price.keys()).where(and_(TotalPrice_2._07supplier_code.in_(allow_prices),
                                                                      TotalPrice_2.to_price==self.price_settings.period))
                    sess.execute(insert(FinalPrice).from_select(cols_for_price.values(), price))

                    sess.commit()

                    sess.query(FinalPrice).where(FinalPrice.put_away_zp.notlike(f"%{self.price_settings.zp_brands_setting}%")).delete()

                    self.delete_exceptions(sess)

                    sess.commit()
                    self.log.add(LOG_ID, 'ok!')


                # FinalPrice.__table__.drop(engine)

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

            finish_cycle_time = datetime.datetime.now()
            if wait_sec > (finish_cycle_time - start_cycle_time).seconds:
                for _ in range(wait_sec - (finish_cycle_time - start_cycle_time).seconds):
                    if self.isPause:
                        break
                    time.sleep(1)
        else:
            self.log.add(LOG_ID, f"Пауза", f"<span style='color:{colors.orange_log_color};'>Пауза</span>  ")
            self.SetButtonEnabledSignal.emit(True)

    def delete_exceptions(self, sess):
        cols = {"03Наименование": FinalPrice._03name, "15КодТутОптТорг": FinalPrice._15code_optt, }
        conditions = {"Начинается с": lambda col, x: col.ilike(f"{x}%"),  # '^{}'], startswith
                      "Содержит": lambda col, x: col.ilike(f"%{x}%"),  # '{}'],
                      "Не содержит": lambda col, x: not_(col.contains(x)),  # '{}'],
                      "Заканчивается на": lambda col, x: col.ilike(f"%{x}"),  # '{}$'], endswith
                      "Равно": lambda col, x: func.upper(col) == x,
                      # "Не равно": lambda col, x: func.upper(col) != x,
                      }
        exceptions = sess.execute(
            select(PriceException).where(and_(PriceException.deny.like(f"%{self.price_settings.buyer_code}"),
                                              PriceException.extra == "Действует в экселе"))).scalars().all()

        for e in exceptions:
            print(e.text, e.condition)
            condition = conditions.get(e.condition, None)
            col = cols.get(e.find, None)
            if condition and col:
                sess.execute(update(FinalPrice).where(condition(col, e.text)).values(mult_less='0'))

        sess.query(FinalPrice).where(FinalPrice.mult_less != None).delete()