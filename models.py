import uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import REAL, NUMERIC, String, Uuid, text, Integer, Numeric, Index, Boolean
import datetime
from typing import Annotated

class Base(DeclarativeBase):
    pass

class Base1(DeclarativeBase):
    pass

class Base1_1(DeclarativeBase):
    pass

class Base2(DeclarativeBase):
    pass

class Base2_1(DeclarativeBase):
    pass

intpk = Annotated[int, mapped_column(primary_key=True)]
uuidpk = Annotated[uuid.UUID, mapped_column(Uuid, primary_key=True, server_default=text("gen_random_uuid()"))]
str_x = lambda x:Annotated[String, mapped_column(String(x), nullable=True)]
intgr = Annotated[int, mapped_column(Integer, nullable=True)]
real = Annotated[REAL, mapped_column(REAL, nullable=True)]
numeric = Annotated[Numeric, mapped_column(Numeric(12, 2), nullable=True)]

class SupplierPriceSettings(Base):
    __tablename__ = "supplier_price_settings"
    id: Mapped[intpk]
    # Издержки real,
    costs: Mapped[real]
    # Код_поставщика varchar(10),
    supplier_code: Mapped[str_x(10)]
    # Можем_купить varchar(20),
    buy: Mapped[str_x(20)]
    # Работаем varchar(20),
    works: Mapped[str_x(20)]
    # Почта varchar(256),
    # email: Mapped[str_x(256)]
    # Условие_имени_файла varchar(20),
    # file_name_cond: Mapped[str_x(20)]
    # Срок_обновление_не_более real,
    update_time: Mapped[real]
    # Имя_файла varchar(256),
    # file_name: Mapped[str_x(256)]
    # Прайс_оптовый varchar(20),
    wholesale: Mapped[str_x(20)]
    # Цену_считать_базовой varchar(20),
    is_base_price: Mapped[str_x(20)]
    # В_прайс real,
    in_price: Mapped[real]
    # Краткое_наименование integer,
    short_name: Mapped[intgr]
    # Закупка_для_оборотных_средств varchar(20),
    buy_for_working_capital: Mapped[real]
    # Разрешения_ПП varchar,
    access_pp: Mapped[str_x(500)]
    # Лот_поставщика real,
    supplier_lot: Mapped[real]
    # К_Превышения_базовой_цены real,
    over_base_price: Mapped[real]
    # Лот_удобный_нам real,
    convenient_lot: Mapped[real]
    # Обрабатываем varchar(20),
    calculate: Mapped[str_x(20)]
    # Стандартизируем varchar(20),
    standard: Mapped[str_x(20)]
    # Код_прайса varchar(10),
    price_code: Mapped[str_x(20)]
    # Наценка_мин real,
    min_markup: Mapped[real]
    # Наценка_опт real,
    markup_wholesale: Mapped[real]
    # Наценка_макс real,
    max_markup: Mapped[real]
    # Процент_отгрузки real,
    unload_percent: Mapped[real]
    # Отсрочка real,
    delay: Mapped[real]
    # Наценка_для_ОС real,
    markup_os: Mapped[real]
    # Процент_изменения_строк real,
    row_change_percent: Mapped[real]
    # Процент_изменения_цены real,
    price_change_percent: Mapped[real]
    # Рейтинг_поставщика real
    supplier_rating: Mapped[real]

class MailReport(Base):
    __tablename__ = "mail_report"
    id: Mapped[uuidpk]
    price_code: Mapped[str_x(20)]
    # sender varchar(256),
    sender: Mapped[str_x(256)]
    # file_name varchar(256),
    file_name: Mapped[str_x(256)]
    info_message: Mapped[str_x(100)]
    # date varchar(256)
    # date: Mapped[str_x(256)]
    date: Mapped[datetime.datetime] = mapped_column(nullable=True)

class MailReportUnloaded(Base):
    __tablename__ = "mail_report_unloaded"
    id: Mapped[uuidpk]
    # sender varchar(256),
    sender: Mapped[str_x(256)]
    # file_name varchar(256),
    file_name: Mapped[str_x(256)]
    # date: Mapped[str_x(256)]
    date: Mapped[datetime.datetime] = mapped_column(nullable=True)

class SumTable(Base1):
    __tablename__ = "sum_table"
    # __table_args__ = (#Index("sum_table_id_compare_index", "id_compare"),
    #                     Index("sum_table_id_compare_index", "id_compare"),#, postgresql_using="hash"),
    #                   )
    # id: Mapped[uuidpk]
    id: Mapped[intpk]
    # id_compare: Mapped[uuid.UUID] = mapped_column(Uuid, server_default=text("gen_random_uuid()"))
    # id_compare: Mapped[intpk]
    price_code: Mapped[str_x(20)]
    # price: Mapped[real]
    prev_sum: Mapped[real]
    # grad: Mapped[intgr]

class SumTable2(Base1_1):
    __tablename__ = "sum_table_2"
    id: Mapped[intpk]
    price_code: Mapped[str_x(20)]
    # price: Mapped[real]
    prev_sum: Mapped[real]
    # grad: Mapped[intgr]

class Price_1(Base1):
    __tablename__ = "price_1"
    __table_args__ = (Index("price_1_brand_s_low_index", "brand_s_low"),
                      # Index("price_1_id_compare_index", "id_compare"),# postgresql_using="hash"),
                      # Index('price_1_01article_re_index', "_01article",
                      #       postgresql_ops={"_01article": "gin_trgm_ops"},
                      #       postgresql_using='gin'),
                      # Index('price_1_article_s_g_index', 'article_s', postgresql_using='gist', postgresql_ops={'article_s': 'gist_trgm_ops'}),
                      # Index('price_1_article_s_g_index', 'article_s', postgresql_using='gist', postgresql_ops={'article_s': 'gist_trgm_ops'}),
                      # Index('price_1_brand_s_g_index', 'brand_s', postgresql_using='gist', postgresql_ops={'brand_s': 'gist_trgm_ops'}),
                      # Index('price_1_name_s_g_index', 'name_s', postgresql_using='gist', postgresql_ops={'name_s': 'gist_trgm_ops'}),
                      Index("price_1_key1_index", "key1_s"),
                      Index("price_1_05_price_index", "_05price"),
                      Index("price_1_07_index", "_07supplier_code"),
                      Index("price_1_article_brand_index", "article_s", "brand_s"),
                      Index("price_1_article_name_index", "article_s", "name_s"),
                      )
    # __table_args__ = (Index("price_file_name_index", "file_name"),
    #                   Index("price_1_14_low_index", "_01article", "_14brand_filled_in_low"),
    #                   Index("price_1_7_14_low_index", "_01article", "_07supplier_code", "_14brand_filled_in_low"),
    #                   Index("price_7_14_low_index", "_07supplier_code", "_14brand_filled_in_low"),
    #                   Index("price_07_index", "_07supplier_code"),
    #                   Index("price_09_index", "_09code_supl_goods"),
    #                   Index("price_14_low_index", "_14brand_filled_in_low"),
    #                   Index("price_15_index", "_15code_optt"),
    #                   )
    id: Mapped[intpk]
    # id_compare: Mapped[uuid.UUID] = mapped_column(Uuid, server_default=text("gen_random_uuid()"))
    # id_compare: Mapped[intpk]
    # id: Mapped[intpk]
    # Ключ1_поставщика varchar(256),
    key1_s: Mapped[str_x(256)]
    # Артикул_поставщика varchar(256),
    article_s: Mapped[str_x(256)]
    # Производитель_поставщика varchar(256),
    brand_s: Mapped[str_x(256)]
    brand_s_low: Mapped[str_x(256)]
    # Наименование_поставщика varchar(256),
    name_s: Mapped[str_x(256)]
    # Количество_поставщика REAL,
    count_s: Mapped[intgr]
    # Цена_поставщика NUMERIC(12,2),
    price_s: Mapped[numeric]
    currency_s: Mapped[str_x(50)]
    # Кратность_поставщика REAL,
    mult_s: Mapped[intgr]
    # Примечание_поставщика varchar(1000),
    notice_s: Mapped[str_x(256)]
    # _01Артикул varchar(256),
    _01article: Mapped[str_x(256)]
    _01article_comp: Mapped[str_x(256)]
    # _02Производитель varchar(256),
    _02brand: Mapped[str_x(256)]
    _14brand_filled_in: Mapped[str_x(256)]
    # _03Наименование varchar(500),
    _03name: Mapped[str_x(256)]
    # _04Количество REAL,
    _04count: Mapped[intgr]
    # _05Цена NUMERIC(12,2),
    _05price: Mapped[numeric]
    _12sum: Mapped[numeric]
    # _06Кратность_ REAL,
    _06mult: Mapped[intgr]
    _15code_optt: Mapped[str_x(256)]
    # _07Код_поставщика varchar(150),
    _07supplier_code: Mapped[str_x(20)]
    # _20ИслючитьИзПрайса varchar(50),
    _20exclude: Mapped[str_x(50)]
    # _13Градация REAL,
    _13grad: Mapped[intgr]
    # _17КодУникальности varchar(500),
    _17code_unique: Mapped[str_x(256)]
    # _18КороткоеНаименование varchar(256),
    _18short_name: Mapped[str_x(256)]

class Price_1_1(Base1_1):
    __tablename__ = "price_1_1"
    __table_args__ = (Index("price_1_1_brand_s_low_index", "brand_s_low"),
                      Index("price_1_1_key1_index", "key1_s"),
                      Index("price_1_1_05_price_index", "_05price"),
                      Index("price_1_1_07_index", "_07supplier_code"),
                      Index("price_1_1_article_brand_index", "article_s", "brand_s"),
                      Index("price_1_1_article_name_index", "article_s", "name_s"),
                      )

    id: Mapped[intpk]
    # Ключ1_поставщика varchar(256),
    key1_s: Mapped[str_x(256)]
    # Артикул_поставщика varchar(256),
    article_s: Mapped[str_x(256)]
    # Производитель_поставщика varchar(256),
    brand_s: Mapped[str_x(256)]
    brand_s_low: Mapped[str_x(256)]
    # Наименование_поставщика varchar(256),
    name_s: Mapped[str_x(256)]
    # Количество_поставщика REAL,
    count_s: Mapped[intgr]
    # Цена_поставщика NUMERIC(12,2),
    price_s: Mapped[numeric]
    currency_s: Mapped[str_x(50)]
    # Кратность_поставщика REAL,
    mult_s: Mapped[intgr]
    # Примечание_поставщика varchar(1000),
    notice_s: Mapped[str_x(256)]
    # _01Артикул varchar(256),
    _01article: Mapped[str_x(256)]
    _01article_comp: Mapped[str_x(256)]
    # _02Производитель varchar(256),
    _02brand: Mapped[str_x(256)]
    _14brand_filled_in: Mapped[str_x(256)]
    # _03Наименование varchar(500),
    _03name: Mapped[str_x(256)]
    # _04Количество REAL,
    _04count: Mapped[intgr]
    # _05Цена NUMERIC(12,2),
    _05price: Mapped[numeric]
    _12sum: Mapped[numeric]
    # _06Кратность_ REAL,
    _06mult: Mapped[intgr]
    _15code_optt: Mapped[str_x(256)]
    # _07Код_поставщика varchar(150),
    _07supplier_code: Mapped[str_x(20)]
    # _20ИслючитьИзПрайса varchar(50),
    _20exclude: Mapped[str_x(50)]
    # _13Градация REAL,
    _13grad: Mapped[intgr]
    # _17КодУникальности varchar(500),
    _17code_unique: Mapped[str_x(256)]
    # _18КороткоеНаименование varchar(256),
    _18short_name: Mapped[str_x(256)]

class TotalPrice_1(Base):
    __tablename__ = "total_price_1"

    # __table_args__ = (Index("total_price_1_07_cur_14_code_index", "_07supplier_code", "currency_s", "_14brand_filled_in"),)
                        # Index("price_1_brand_s_low_index", "brand_s_low"),
    #                   Index("price_1_id_compare_hash_index", "id_compare", postgresql_using="hash"),
    #                   Index("price_1_key1_index", "key1_s"),
    #                   Index("price_1_05_price_index", "_05price"),
    #                   Index("price_1_07_index", "_07supplier_code"),
    #                   Index("price_1_article_brand_index", "article_s", "brand_s"),
    #                   Index("price_1_article_name_index", "article_s", "name_s"),
    #                   )
    id: Mapped[uuidpk]
    # id: Mapped[intpk]
    # Ключ1_поставщика varchar(256),
    key1_s: Mapped[str_x(256)]
    # Артикул_поставщика varchar(256),
    article_s: Mapped[str_x(256)]
    # Производитель_поставщика varchar(256),
    brand_s: Mapped[str_x(256)]
    # brand_s_low: Mapped[str_x(256)]
    # Наименование_поставщика varchar(256),
    name_s: Mapped[str_x(256)]
    # Количество_поставщика REAL,
    count_s: Mapped[intgr]
    # Цена_поставщика NUMERIC(12,2),
    price_s: Mapped[numeric]
    currency_s: Mapped[str_x(50)]
    # Кратность_поставщика REAL,
    mult_s: Mapped[intgr]
    # Примечание_поставщика varchar(1000),
    notice_s: Mapped[str_x(256)]
    # _01Артикул varchar(256),
    _01article: Mapped[str_x(256)]
    _01article_comp: Mapped[str_x(256)]
    # _02Производитель varchar(256),
    _02brand: Mapped[str_x(256)]
    _14brand_filled_in: Mapped[str_x(256)]
    # _03Наименование varchar(500),
    _03name: Mapped[str_x(256)]
    # _04Количество REAL,
    _04count: Mapped[intgr]
    # _05Цена NUMERIC(12,2),
    _05price: Mapped[numeric]
    _12sum: Mapped[numeric]
    # _06Кратность_ REAL,
    _06mult: Mapped[intgr]
    _15code_optt: Mapped[str_x(256)]
    # _07Код_поставщика varchar(150),
    _07supplier_code: Mapped[str_x(20)]
    # _20ИслючитьИзПрайса varchar(50),
    _20exclude: Mapped[str_x(50)]
    # _13Градация REAL,
    _13grad: Mapped[intgr]
    # _17КодУникальности varchar(500),
    _17code_unique: Mapped[str_x(256)]
    # _18КороткоеНаименование varchar(256),
    _18short_name: Mapped[str_x(256)]

class Price_2(Base2):
    __tablename__ = "price_2"
    __table_args__ = (Index("price_2_09code_supl_goods_index", "_09code_supl_goods"),
                      Index("price_2_01article_14brand_filled_in_index", "_01article_comp", "_14brand_filled_in"),
                      Index("price_2_07supplier_code_14brand_filled_in_index", "_07supplier_code", "_14brand_filled_in"),
                      Index("price_2_15code_optt_index", "_15code_optt"),)

    id: Mapped[intpk]
    # Ключ1_поставщика varchar(256),
    key1_s: Mapped[str_x(256)]
    # Артикул_поставщика varchar(256),
    article_s: Mapped[str_x(256)]
    # Производитель_поставщика varchar(256),
    brand_s: Mapped[str_x(256)]
    # Наименование_поставщика varchar(256),
    name_s: Mapped[str_x(256)]
    # Количество_поставщика REAL,
    count_s: Mapped[intgr]
    # Цена_поставщика NUMERIC(12,2),
    price_s: Mapped[numeric]
    currency_s: Mapped[str_x(50)]
    # Кратность_поставщика REAL,
    mult_s: Mapped[intgr]
    # Примечание_поставщика varchar(1000),
    notice_s: Mapped[str_x(256)]
    # _01Артикул varchar(256),
    _01article: Mapped[str_x(256)]
    _01article_comp: Mapped[str_x(256)]
    #   _02Производитель varchar(256),
    _02brand: Mapped[str_x(256)]
    # _03Наименование varchar(500),
    _03name: Mapped[str_x(256)]
    # _04Количество REAL,
    _04count: Mapped[intgr]
    # _05Цена NUMERIC(12,2),
    _05price: Mapped[numeric]
    # _06Кратность_ REAL,
    _06mult: Mapped[intgr]
    # _07Код_поставщика varchar(150),
    _07supplier_code: Mapped[str_x(20)]
    # _09Код_Поставщик_Товар varchar(500),
    _09code_supl_goods: Mapped[str_x(256)]
    # _10Оригинал varchar(30),
    _10original: Mapped[str_x(30)]
    # _13Градация REAL,
    _13grad: Mapped[intgr]
    # _14Производитель_заполнен varchar(1000),
    _14brand_filled_in: Mapped[str_x(256)]
    # _14brand_filled_in_low: Mapped[str_x(256)]
    # _15КодТутОптТорг varchar(256),
    _15code_optt: Mapped[str_x(256)]
    # _17КодУникальности varchar(500),
    _17code_unique: Mapped[str_x(256)]
    # _18КороткоеНаименование varchar(256),
    _18short_name: Mapped[str_x(256)]
    #   _19МинЦенаПоПрайсу varchar(50),
    _19min_price: Mapped[str_x(50)]
    # _20ИслючитьИзПрайса varchar(50),
    _20exclude: Mapped[str_x(50)]
    # Название_файла varchar(50),
    file_name: Mapped[str_x(256)]
    # Отсрочка REAL,
    delay: Mapped[real]
    # В прайс
    to_price: Mapped[intgr]
    # Продаём_для_ОС varchar(20),
    sell_for_OS: Mapped[str_x(20)]
    # Наценка_для_ОС REAL,
    markup_os: Mapped[real]
    # Макс_снижение_от_базовой_цены REAL,
    max_decline: Mapped[real]
    # Наценка_на_праздники_1_02 REAL,
    markup_holidays: Mapped[real]
    # Наценка_Р REAL,
    markup_R: Mapped[real]
    # Мин_наценка REAL,
    min_markup: Mapped[real]
    # Мин опт наценка
    min_wholesale_markup: Mapped[real]
    # Наценка_на_оптовые_товары REAL,
    markup_wh_goods: Mapped[real]
    # Шаг_градаци REAL,
    grad_step: Mapped[real]
    # Шаг_опт REAL,
    wh_step: Mapped[real]
    # Разрешения_ПП varchar(3000),
    access_pp: Mapped[str_x(500)]
    # Процент_Отгрузки REAL,
    unload_percent: Mapped[real]
    # УбратьЗП varchar(3000),
    put_away_zp: Mapped[str_x(500)]
    # Предложений_опт REAL,
    offers_wh: Mapped[intgr]
    # ЦенаБ NUMERIC(12,2),
    price_b: Mapped[numeric]
    # Низкая_цена NUMERIC(12,2),
    low_price: Mapped[numeric]
    # Кол_во REAL,
    count: Mapped[intgr]
    # Наценка_ПБ REAL,
    markup_pb: Mapped[real]
    # Код_ПБ_П varchar,
    code_pb_p: Mapped[str_x(500)]
    # _06Кратность REAL,
    _06mult_new: Mapped[intgr]
    # Кратность_меньше varchar(25),
    mult_less: Mapped[str_x(20)]
    # _05Цена_плюс NUMERIC(12,2),
    _05price_plus: Mapped[numeric]
    # ШтР integer DEFAULT 0,
    reserve_count: Mapped[intgr]
    # Количество_закупок REAL,
    buy_count: Mapped[real]
    #   ЦенаМин numeric(12,2),
    min_price: Mapped[numeric]
    #   ЦенаМинПоставщик varchar(20)
    min_supplier: Mapped[str_x(20)]

    # # _09Код_Поставщик_Товар varchar(500),
    # _09code_supl_goods: Mapped[str_x(300)]
    # # _10Оригинал varchar(30),
    # _10original: Mapped[str_x(30)]
    # # _13Градация REAL,
    # _13grad: Mapped[real]
    # # _14Производитель_заполнен varchar(1000),
    # _14brand_filled_in: Mapped[str_x(256)]
    # _14brand_filled_in_low: Mapped[str_x(256)]
    # # _15КодТутОптТорг varchar(256),
    # _15code_optt: Mapped[str_x(256)]
    # # _17КодУникальности varchar(500),
    # _17code_unique: Mapped[str_x(256)]
    # # _19МинЦенаПоПрайсу varchar(50),
    # _19min_price: Mapped[str_x(50)]
    # # _20ИслючитьИзПрайса varchar(50),
    # _20exclude: Mapped[str_x(50)]
    # # Название_файла varchar(50),
    # file_name: Mapped[str_x(50)]
    # # Отсрочка REAL,
    # delay: Mapped[real]
    # # Продаём_для_ОС varchar(20),
    # sell_for_OS: Mapped[str_x(20)]
    # # Наценка_для_ОС REAL,
    # markup_os: Mapped[real]
    # # Макс_снижение_от_базовой_цены REAL,
    # max_decline: Mapped[real]
    # # Наценка_на_праздники_1_02 REAL,
    # markup_holidays: Mapped[real]
    # # Наценка_Р REAL,
    # markup_R: Mapped[real]
    # # Мин_наценка REAL,
    # min_markup: Mapped[real]
    # # Шаг_градаци REAL,
    # grad_step: Mapped[real]
    # # Шаг_опт REAL,
    # wholesale_step: Mapped[real]
    # # Разрешения_ПП varchar(3000),
    # access_pp: Mapped[str_x(500)]
    # # Процент_Отгрузки REAL,
    # unload_percent: Mapped[real]
    # # УбратьЗП varchar(3000),
    # put_away_zp: Mapped[str_x(500)]
    # # Предложений_опт REAL,
    # offers_wholesale: Mapped[intgr]
    # # ЦенаБ NUMERIC(12,2),
    # price_b: Mapped[numeric]
    # # Низкая_цена NUMERIC(12,2),
    # low_price: Mapped[numeric]
    # # Кол_во REAL,
    # count: Mapped[intgr]
    # # Наценка_ПБ REAL,
    # markup_pb: Mapped[real]
    # # Код_ПБ_П varchar,
    # code_pb_p: Mapped[str_x(500)]
    # # _06Кратность REAL,
    # _06milt: Mapped[intgr]
    # # Кратность_меньше varchar(25),
    # mult_less: Mapped[intgr]
    # # _05Цена_плюс NUMERIC(12,2),
    # _05price_plus: Mapped[numeric]
    # # ШтР integer DEFAULT 0,
    # reserve_count: Mapped[intgr]
    # # Количество_закупок REAL,
    # buy_count: Mapped[real]
    # # ЦенаМин numeric(12,2),
    # min_price: Mapped[numeric]
    # # ЦенаМинПоставщик varchar(20)
    # min_supplier: Mapped[str] = mapped_column(String(20), nullable=True)
class Price_2_2(Base2_1):
    __tablename__ = "price_2_2"
    __table_args__ = (Index("price_2_2_09code_supl_goods_index", "_09code_supl_goods"),
                      Index("price_2_2_01article_14brand_filled_in_index", "_01article_comp", "_14brand_filled_in"),
                      Index("price_2_2_07supplier_code_14brand_filled_in_index", "_07supplier_code", "_14brand_filled_in"),
                      Index("price_2_2_15code_optt_index", "_15code_optt"),)

    id: Mapped[intpk]
    # Ключ1_поставщика varchar(256),
    key1_s: Mapped[str_x(256)]
    # Артикул_поставщика varchar(256),
    article_s: Mapped[str_x(256)]
    # Производитель_поставщика varchar(256),
    brand_s: Mapped[str_x(256)]
    # Наименование_поставщика varchar(256),
    name_s: Mapped[str_x(256)]
    # Количество_поставщика REAL,
    count_s: Mapped[intgr]
    # Цена_поставщика NUMERIC(12,2),
    price_s: Mapped[numeric]
    currency_s: Mapped[str_x(50)]
    # Кратность_поставщика REAL,
    mult_s: Mapped[intgr]
    # Примечание_поставщика varchar(1000),
    notice_s: Mapped[str_x(256)]
    # _01Артикул varchar(256),
    _01article: Mapped[str_x(256)]
    _01article_comp: Mapped[str_x(256)]
    #   _02Производитель varchar(256),
    _02brand: Mapped[str_x(256)]
    # _03Наименование varchar(500),
    _03name: Mapped[str_x(256)]
    # _04Количество REAL,
    _04count: Mapped[intgr]
    # _05Цена NUMERIC(12,2),
    _05price: Mapped[numeric]
    # _06Кратность_ REAL,
    _06mult: Mapped[intgr]
    # _07Код_поставщика varchar(150),
    _07supplier_code: Mapped[str_x(20)]
    # _09Код_Поставщик_Товар varchar(500),
    _09code_supl_goods: Mapped[str_x(256)]
    # _10Оригинал varchar(30),
    _10original: Mapped[str_x(30)]
    # _13Градация REAL,
    _13grad: Mapped[intgr]
    # _14Производитель_заполнен varchar(1000),
    _14brand_filled_in: Mapped[str_x(256)]
    # _14brand_filled_in_low: Mapped[str_x(256)]
    # _15КодТутОптТорг varchar(256),
    _15code_optt: Mapped[str_x(256)]
    # _17КодУникальности varchar(500),
    _17code_unique: Mapped[str_x(256)]
    # _18КороткоеНаименование varchar(256),
    _18short_name: Mapped[str_x(256)]
    #   _19МинЦенаПоПрайсу varchar(50),
    _19min_price: Mapped[str_x(50)]
    # _20ИслючитьИзПрайса varchar(50),
    _20exclude: Mapped[str_x(50)]
    # Название_файла varchar(50),
    file_name: Mapped[str_x(256)]
    # Отсрочка REAL,
    delay: Mapped[real]
    # В прайс
    to_price: Mapped[intgr]
    # Продаём_для_ОС varchar(20),
    sell_for_OS: Mapped[str_x(20)]
    # Наценка_для_ОС REAL,
    markup_os: Mapped[real]
    # Макс_снижение_от_базовой_цены REAL,
    max_decline: Mapped[real]
    # Наценка_на_праздники_1_02 REAL,
    markup_holidays: Mapped[real]
    # Наценка_Р REAL,
    markup_R: Mapped[real]
    # Мин_наценка REAL,
    min_markup: Mapped[real]
    # Мин опт наценка
    min_wholesale_markup: Mapped[real]
    # Наценка_на_оптовые_товары REAL,
    markup_wh_goods: Mapped[real]
    # Шаг_градаци REAL,
    grad_step: Mapped[real]
    # Шаг_опт REAL,
    wh_step: Mapped[real]
    # Разрешения_ПП varchar(3000),
    access_pp: Mapped[str_x(500)]
    # Процент_Отгрузки REAL,
    unload_percent: Mapped[real]
    # УбратьЗП varchar(3000),
    put_away_zp: Mapped[str_x(500)]
    # Предложений_опт REAL,
    offers_wh: Mapped[intgr]
    # ЦенаБ NUMERIC(12,2),
    price_b: Mapped[numeric]
    # Низкая_цена NUMERIC(12,2),
    low_price: Mapped[numeric]
    # Кол_во REAL,
    count: Mapped[intgr]
    # Наценка_ПБ REAL,
    markup_pb: Mapped[real]
    # Код_ПБ_П varchar,
    code_pb_p: Mapped[str_x(500)]
    # _06Кратность REAL,
    _06mult_new: Mapped[intgr]
    # Кратность_меньше varchar(25),
    mult_less: Mapped[str_x(20)]
    # _05Цена_плюс NUMERIC(12,2),
    _05price_plus: Mapped[numeric]
    # ШтР integer DEFAULT 0,
    reserve_count: Mapped[intgr]
    # Количество_закупок REAL,
    buy_count: Mapped[real]
    #   ЦенаМин numeric(12,2),
    min_price: Mapped[numeric]
    #   ЦенаМинПоставщик varchar(20)
    min_supplier: Mapped[str_x(20)]

class TotalPrice_2(Base):
    __tablename__ = "total_price_2"
    # __table_args__ = (Index("total_price_2_09code_supl_goods_index", "_09code_supl_goods"),
    #                   Index("total_price_2_07supplier_code_index", "_07supplier_code"),
    #                   Index("total_price_2_15code_optt_index", "_15code_optt"),
    #                   Index("total_price_2_07_14_code_index", "_07supplier_code", "_14brand_filled_in"),
    #                   # Index("total_price_2_07_cur_14_code_index", "_07supplier_code", "currency_s", "_14brand_filled_in"),
    #                   )
    #                   Index("price_2_01article_14brand_filled_in_index", "_01article", "_14brand_filled_in"),
    #                   Index("price_2_07supplier_code_14brand_filled_in_low_index", "_07supplier_code", "_14brand_filled_in_low"),
    #                   Index("price_2_15code_optt_index", "_15code_optt"),)

    id: Mapped[uuidpk]
    # Ключ1_поставщика varchar(256),
    key1_s: Mapped[str_x(256)]
    # Артикул_поставщика varchar(256),
    article_s: Mapped[str_x(256)]
    # Производитель_поставщика varchar(256),
    brand_s: Mapped[str_x(256)]
    # Наименование_поставщика varchar(256),
    name_s: Mapped[str_x(256)]
    # Количество_поставщика REAL,
    count_s: Mapped[intgr]
    # Цена_поставщика NUMERIC(12,2),
    price_s: Mapped[numeric]
    currency_s: Mapped[str_x(50)]
    # Кратность_поставщика REAL,
    mult_s: Mapped[intgr]
    # Примечание_поставщика varchar(1000),
    notice_s: Mapped[str_x(256)]
    # _01Артикул varchar(256),
    _01article: Mapped[str_x(256)]
    _01article_comp: Mapped[str_x(256)]
    #   _02Производитель varchar(256),
    _02brand: Mapped[str_x(256)]
    # _03Наименование varchar(500),
    _03name: Mapped[str_x(256)]
    # _04Количество REAL,
    _04count: Mapped[intgr]
    # _05Цена NUMERIC(12,2),
    _05price: Mapped[numeric]
    # _06Кратность_ REAL,
    _06mult: Mapped[intgr]
    # _07Код_поставщика varchar(150),
    _07supplier_code: Mapped[str_x(20)]
    # _09Код_Поставщик_Товар varchar(500),
    _09code_supl_goods: Mapped[str_x(256)]
    # _10Оригинал varchar(30),
    _10original: Mapped[str_x(30)]
    # _13Градация REAL,
    _13grad: Mapped[intgr]
    # _14Производитель_заполнен varchar(1000),
    _14brand_filled_in: Mapped[str_x(256)]
    # _14brand_filled_in_low: Mapped[str_x(256)]
    # _15КодТутОптТорг varchar(256),
    _15code_optt: Mapped[str_x(256)]
    # _17КодУникальности varchar(500),
    _17code_unique: Mapped[str_x(256)]
    # _18КороткоеНаименование varchar(256),
    _18short_name: Mapped[str_x(256)]
    #   _19МинЦенаПоПрайсу varchar(50),
    _19min_price: Mapped[str_x(50)]
    # _20ИслючитьИзПрайса varchar(50),
    _20exclude: Mapped[str_x(50)]
    # Название_файла varchar(50),
    # file_name: Mapped[str_x(256)]
    # Отсрочка REAL,
    delay: Mapped[real]
    # В прайс
    to_price: Mapped[intgr]
    # Продаём_для_ОС varchar(20),
    sell_for_OS: Mapped[str_x(20)]
    # Наценка_для_ОС REAL,
    markup_os: Mapped[real]
    # Макс_снижение_от_базовой_цены REAL,
    max_decline: Mapped[real]
    # Наценка_на_праздники_1_02 REAL,
    markup_holidays: Mapped[real]
    # Наценка_Р REAL,
    markup_R: Mapped[real]
    # Мин_наценка REAL,
    min_markup: Mapped[real]
    # Мин опт наценка
    min_wholesale_markup: Mapped[real]
    # Наценка_на_оптовые_товары REAL,
    markup_wh_goods: Mapped[real]
    # Шаг_градаци REAL,
    grad_step: Mapped[real]
    # Шаг_опт REAL,
    wh_step: Mapped[real]
    # Разрешения_ПП varchar(3000),
    access_pp: Mapped[str_x(500)]
    # Процент_Отгрузки REAL,
    unload_percent: Mapped[real]
    # УбратьЗП varchar(3000),
    put_away_zp: Mapped[str_x(500)]
    # Предложений_опт REAL,
    offers_wh: Mapped[intgr]
    # ЦенаБ NUMERIC(12,2),
    price_b: Mapped[numeric]
    # Низкая_цена NUMERIC(12,2),
    low_price: Mapped[numeric]
    # Кол_во REAL,
    count: Mapped[intgr]
    # Наценка_ПБ REAL,
    markup_pb: Mapped[real]
    # Код_ПБ_П varchar,
    code_pb_p: Mapped[str_x(500)]
    # _06Кратность REAL,
    _06mult_new: Mapped[intgr]
    # Кратность_меньше varchar(25),
    mult_less: Mapped[str_x(20)]
    # _05Цена_плюс NUMERIC(12,2),
    _05price_plus: Mapped[numeric]
    # ШтР integer DEFAULT 0,
    reserve_count: Mapped[intgr]
    # Количество_закупок REAL,
    buy_count: Mapped[real]
    #   ЦенаМин numeric(12,2),
    min_price: Mapped[numeric]
    #   ЦенаМинПоставщик varchar(20)
    min_supplier: Mapped[str_x(20)]

# class Price(Base):
#     __tablename__ = "price"
#     __table_args__ = (Index("price_file_name_index", "file_name"),
#                       Index("price_1_14_low_index", "_01article", "_14brand_filled_in_low"),
#                       Index("price_1_7_14_low_index", "_01article", "_07supplier_code", "_14brand_filled_in_low"),
#                       Index("price_7_14_low_index", "_07supplier_code", "_14brand_filled_in_low"),
#                       Index("price_07_index", "_07supplier_code"),
#                       Index("price_09_index", "_09code_supl_goods"),
#                       Index("price_14_low_index", "_14brand_filled_in_low"),
#                       Index("price_15_index", "_15code_optt"),
#                       )
#     # id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, server_default=text("gen_random_uuid()"))
#     id: Mapped[uuidpk]
#     # Ключ1_поставщика varchar(256),
#     key_s: Mapped[str_x(256)]
#     # Артикул_поставщика varchar(256),
#     article_s: Mapped[str_x(256)]
#     # Производитель_поставщика varchar(256),
#     brand_s: Mapped[str_x(256)]
#     # Наименование_поставщика varchar(256),
#     name_s: Mapped[str_x(256)]
#     # Количество_поставщика REAL,
#     count_s: Mapped[real]
#     # Цена_поставщика NUMERIC(12,2),
#     price_s: Mapped[numeric]
#     # Кратность_поставщика REAL,
#     mult_s: Mapped[intgr]
#     # Примечание_поставщика varchar(1000),
#     notice_s: Mapped[str_x(256)]
#     # _01Артикул varchar(256),
#     _01article: Mapped[str_x(256)]
#     # _02Производитель varchar(256),
#     _02brand: Mapped[str_x(256)]
#     # _03Наименование varchar(500),
#     _03name: Mapped[str_x(256)]
#     # _04Количество REAL,
#     _04count: Mapped[intgr]
#     # _05Цена NUMERIC(12,2),
#     _05price: Mapped[numeric]
#     # _06Кратность_ REAL,
#     _06milt_: Mapped[intgr]
#     # _07Код_поставщика varchar(150),
#     _07supplier_code: Mapped[str_x(20)]
#     # _09Код_Поставщик_Товар varchar(500),
#     _09code_supl_goods: Mapped[str_x(300)]
#     # _10Оригинал varchar(30),
#     _10original: Mapped[str_x(30)]
#     # _13Градация REAL,
#     _13grad: Mapped[real]
#     # _14Производитель_заполнен varchar(1000),
#     _14brand_filled_in: Mapped[str_x(256)]
#     _14brand_filled_in_low: Mapped[str_x(256)]
#     # _15КодТутОптТорг varchar(256),
#     _15code_optt: Mapped[str_x(256)]
#     # _17КодУникальности varchar(500),
#     _17code_unique: Mapped[str_x(256)]
#     # _18КороткоеНаименование varchar(256),
#     _18short_name: Mapped[str_x(256)]
#     # _19МинЦенаПоПрайсу varchar(50),
#     _19min_price: Mapped[str_x(50)]
#     # _20ИслючитьИзПрайса varchar(50),
#     _20exclude: Mapped[str_x(50)]
#     # Название_файла varchar(50),
#     file_name: Mapped[str_x(50)]
#     # Отсрочка REAL,
#     delay: Mapped[real]
#     # Продаём_для_ОС varchar(20),
#     sell_for_OS: Mapped[str_x(20)]
#     # Наценка_для_ОС REAL,
#     markup_os: Mapped[real]
#     # Макс_снижение_от_базовой_цены REAL,
#     max_decline: Mapped[real]
#     # Наценка_на_праздники_1_02 REAL,
#     markup_holidays: Mapped[real]
#     # Наценка_Р REAL,
#     markup_R: Mapped[real]
#     # Мин_наценка REAL,
#     min_markup: Mapped[real]
#     # Шаг_градаци REAL,
#     grad_step: Mapped[real]
#     # Шаг_опт REAL,
#     wholesale_step: Mapped[real]
#     # Разрешения_ПП varchar(3000),
#     access_pp: Mapped[str_x(500)]
#     # Процент_Отгрузки REAL,
#     unload_percent: Mapped[real]
#     # УбратьЗП varchar(3000),
#     put_away_zp: Mapped[str_x(500)]
#     # Предложений_опт REAL,
#     offers_wholesale: Mapped[intgr]
#     # ЦенаБ NUMERIC(12,2),
#     price_b: Mapped[numeric]
#     # Низкая_цена NUMERIC(12,2),
#     low_price: Mapped[numeric]
#     # Кол_во REAL,
#     count: Mapped[intgr]
#     # Наценка_ПБ REAL,
#     markup_pb: Mapped[real]
#     # Код_ПБ_П varchar,
#     code_pb_p: Mapped[str_x(500)]
#     # _06Кратность REAL,
#     _06milt: Mapped[intgr]
#     # Кратность_меньше varchar(25),
#     mult_less: Mapped[intgr]
#     # _05Цена_плюс NUMERIC(12,2),
#     _05price_plus: Mapped[numeric]
#     # ШтР integer DEFAULT 0,
#     reserve_count: Mapped[intgr]
#     # Количество_закупок REAL,
#     buy_count: Mapped[real]
#     # ЦенаМин numeric(12,2),
#     min_price: Mapped[numeric]
#     # ЦенаМинПоставщик varchar(20)
#     min_supplier: Mapped[str] = mapped_column(String(20), nullable=True)
#
class Data07(Base):
    __tablename__ = "data07"

    id: Mapped[intpk]
    # Работаем varchar(20),
    works: Mapped[str_x(20)]
    # Период_обновления_не_более REAL,
    update_time: Mapped[real]
    # Настройка varchar(50),
    setting: Mapped[str_x(50)]
    # В прайс
    to_price: Mapped[intgr]
    # Отсрочка REAL,
    delay: Mapped[real]
    # Продаём_для_ОС varchar(20) DEFAULT null,
    sell_os: Mapped[str_x(20)]
    # Наценка_для_ОС REAL,
    markup_os: Mapped[real]
    # Макс_снижение_от_базовой_цены REAL,
    max_decline: Mapped[real]
    # Наценка_на_праздники_1_02 REAL,
    markup_holidays: Mapped[real]
    # Наценка_Р REAL,
    markup_R: Mapped[real]
    # Мин_наценка REAL,
    min_markup: Mapped[real]
    # Мин опт наценка
    min_wholesale_markup: Mapped[real]
    # Наценка_на_оптовые_товары REAL,
    markup_wholesale: Mapped[real]
    # Шаг_градаци REAL,
    grad_step: Mapped[intgr]
    # Шаг_опт REAL DEFAULT 0,
    wholesale_step: Mapped[real]
    # Разрешения_ПП varchar(3000),
    access_pp: Mapped[str_x(500)]
    # Процент_Отгрузки REAL
    unload_percent: Mapped[real]
#
class Data07_14(Base):
    __tablename__ = "data07_14"
    __table_args__ = (Index("data07_14_setting_correct_index", "setting", "correct"),)

    # id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, server_default=text("gen_random_uuid()"))
    id: Mapped[intpk]
    # Работаем varchar(20),
    works: Mapped[str_x(20)]
    # Период_обновления_не_более REAL,
    update_time: Mapped[real]
    # Настройка varchar(100),
    setting: Mapped[str_x(50)]
    # Макс_снижение_от_базовой_цены REAL,
    max_decline: Mapped[real]
    # Правильное varchar(256),
    correct: Mapped[str_x(256)]
    # correct_low: Mapped[str_x(256)]
    # Наценка_ПБ REAL,
    markup_pb: Mapped[real]
    # Код_ПБ_П varchar)
    code_pb_p: Mapped[str_x(500)]
#
class Data15(Base):
    __tablename__ = "data15"
    id: Mapped[intpk]
    # _15 varchar(300),
    code_15: Mapped[str_x(500)]
    # Предложений_опт REAL,
    offers_wholesale: Mapped[intgr]
    # ЦенаБ REAL
    price_b: Mapped[numeric]
#
class Data09(Base):
    __tablename__ = "data09"
    __table_args__ = (Index("data09_code_09_index", "code_09"),)

    id: Mapped[intpk]
    # УбратьЗП varchar(3000),
    put_away_zp: Mapped[str_x(500)]
    # ШтР REAL DEFAULT NULL,
    reserve_count: Mapped[intgr]
    # _09 varchar(300)
    code_09: Mapped[str_x(300)]
#
class Buy_for_OS(Base):
    __tablename__ = "buy_for_os"
    __table_args__ = (Index("buy_for_os_article_producer_index", "article_producer"),)

    id: Mapped[intpk]
    # Количество_закупок REAL,
    buy_count: Mapped[intgr]
    # АртикулПроизводитель varchar(300)
    article_producer: Mapped[str_x(300)]
#
class Reserve(Base):
    __tablename__ = "reserve"
    __table_args__ = (Index("reserve_code_09_index", "code_09"),)

    id: Mapped[intpk]
    # _09 varchar(300)
    code_09: Mapped[str_x(300)]
    # ШтР REAL DEFAULT NULL,
    reserve_count: Mapped[intgr]
    # _07 (price_code) varchar(20)
    code_07: Mapped[str_x(20)]


class FileSettings(Base):
    __tablename__ = "file_settings"
    # 	id UUID default gen_random_uuid(),
    id: Mapped[intpk]
    # 	Прайс varchar(20),
    price_code: Mapped[str_x(20)]
    parent_code: Mapped[str_x(10)]
    # Сохраняем varchar(20),
    save: Mapped[str_x(20)]
    email: Mapped[str_x(256)]
    # Условие_имени_файла varchar(20),
    file_name_cond: Mapped[str_x(20)]
    # Имя_файла varchar(256),
    file_name: Mapped[str_x(256)]
    # 	Пропуск_сверху integer default 0,
    pass_up: Mapped[Integer] = mapped_column(Integer, nullable=True, default=0)
    # 	Пропуск_снизу integer default 0,
    pass_down: Mapped[Integer] = mapped_column(Integer, nullable=True, default=0)
    # Сопоставление_по varchar(100),
    compare: Mapped[str_x(100)]
    # 	Строка_КлючП integer,
    r_key_s: Mapped[intgr]
    # 	Столбец_КлючП integer,
    c_key_s: Mapped[intgr]
    # 	rc_КлючП varchar(50),
    rc_key_s: Mapped[str_x(50)]
    # 	Название_КлючП varchar(256),
    name_key_s: Mapped[str_x(256)]
    # 	Строка_АртикулП integer,
    r_article_s: Mapped[intgr]
    # 	Столбец_АртикулП integer,
    c_article_s: Mapped[intgr]
    # 	rc_АртикулП varchar(50),
    rc_article_s: Mapped[str_x(50)]
    # 	Название_АртикулП varchar(256),
    name_article_s: Mapped[str_x(256)]
    # 	Строка_БрендП integer,
    r_brand_s: Mapped[intgr]
    # 	Столбец_БрендП integer,
    replace_brand_s: Mapped[str_x(256)]
    c_brand_s: Mapped[intgr]
    # 	rc_БрендП varchar(50),
    rc_brand_s: Mapped[str_x(50)]
    # 	Название_БрендП varchar(256),
    name_brand_s: Mapped[str_x(256)]
    # 	Строка_НаименованиеП integer,
    r_name_s: Mapped[intgr]
    # 	Столбец_НаименованиеП integer,
    c_name_s: Mapped[intgr]
    # 	rc_НаименованиеП varchar(50),
    rc_name_s: Mapped[str_x(50)]
    # 	Название_НаименованиеП varchar(256),
    name_name_s: Mapped[str_x(256)]
    # 	Строка_КоличествоП integer,
    r_count_s: Mapped[intgr]
    # 	Столбец_КоличествоП integer,
    c_count_s: Mapped[intgr]
    # 	rc_КоличествоП varchar(50),
    rc_count_s: Mapped[str_x(50)]
    # 	Название_КоличествоП varchar(256),
    name_count_s: Mapped[str_x(256)]
    # 	Строка_ЦенаП integer,
    r_price_s: Mapped[intgr]
    # 	Столбец_ЦенаП integer,
    c_price_s: Mapped[intgr]
    # 	rc_ЦенаП varchar(50),
    rc_price_s: Mapped[str_x(50)]
    # 	Название_ЦенаП varchar(256),
    name_price_s: Mapped[str_x(256)]
    # 	Строка_КратностьП integer,
    r_mult_s: Mapped[intgr]
    # 	Столбец_КратностьП integer,
    c_mult_s: Mapped[intgr]
    # 	rc_КратностьП varchar(50),
    rc_mult_s: Mapped[str_x(50)]
    # 	Название_КратностьП varchar(256),
    name_mult_s: Mapped[str_x(256)]
    # 	Строка_ПримечаниеП integer,
    r_notice_s: Mapped[intgr]
    # 	Столбец_ПримечаниеП integer,
    c_notice_s: Mapped[intgr]
    # 	rc_ПримечаниеП varchar(50),
    rc_notice_s: Mapped[str_x(50)]
    # 	Название_ПримечаниеП varchar(256),
    name_notice_s: Mapped[str_x(256)]
    # 	Строка_Валюта integer,
    r_currency_s: Mapped[intgr]
    # 	Столбец_Валюта integer,
    c_currency_s: Mapped[intgr]
    # 	rc_Валюта varchar(50),
    rc_currency_s: Mapped[str_x(50)]
    # 	Название_Валюта varchar(256)
    name_currency_s: Mapped[str_x(256)]
    change_price_type: Mapped[str_x(50)]
    change_price_val: Mapped[str_x(50)]

class ColsFix(Base):
    __tablename__ = "cols_fix"
    id: Mapped[intpk]
    price_code: Mapped[str_x(20)]
    col_find: Mapped[str_x(50)]
    find: Mapped[str_x(500)]
    change_type: Mapped[str_x(50)]
    col_change: Mapped[str_x(50)]
    set: Mapped[str_x(500)]

# class ColsFix(Base):
#     __tablename__ = "cols_fix"
#     id: Mapped[int] = mapped_column(primary_key=True)
#     price_code: Mapped[str_x(20)]
#     col_name: Mapped[str_x(50)]
#     change_type: Mapped[str_x(50)]
#     find: Mapped[str_x(500)]
#     change: Mapped[str_x(500)]

class Brands(Base):
    __tablename__ = "brands"
    __table_args__ = (Index("brands_brand_low_index", "brand_low"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    correct_brand: Mapped[str_x(500)]
    brand: Mapped[str_x(500)]
    brand_low: Mapped[str_x(500)]
    mass_offers: Mapped[str_x(10)]
    base_price: Mapped[str_x(10)]

# class PriceChange(Base):
#     __tablename__ = "price_change"
#     id: Mapped[int] = mapped_column(primary_key=True)
#     price_code: Mapped[str_x(20)]
#     brand: Mapped[str_x(256)]
#     discount: Mapped[real]

# class WordsOfException(Base):
#     __tablename__ = "words_of_exception"
#     id: Mapped[int] = mapped_column(primary_key=True)
#     price_code: Mapped[str_x(20)]
#     colunm_name: Mapped[str_x(256)]
#     condition: Mapped[str_x(50)]
#     text: Mapped[str_x(500)]

class SupplierGoodsFix(Base):
    __tablename__ = "supplier_goods_fix"
    __table_args__ = (Index("supplier_goods_fix_key1_index", "key1"),
                      Index("supplier_goods_fix_article_brand_index", "article_s", "brand_s"),
                      Index("supplier_goods_fix_article_name_index", "article_s", "name_s"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    supplier: Mapped[str_x(500)]
    import_setting: Mapped[str_x(20)]
    key1: Mapped[str_x(256)]
    article_s: Mapped[str_x(256)]
    brand_s: Mapped[str_x(256)]
    name: Mapped[str_x(500)]
    brand: Mapped[str_x(256)]
    article: Mapped[str_x(256)]
    price_s: Mapped[numeric]
    sales_ban: Mapped[str_x(50)]
    original: Mapped[str_x(50)]
    marketable_appearance: Mapped[str_x(50)]
    put_away_percent: Mapped[real]
    put_away_count: Mapped[intgr]
    nomenclature: Mapped[str_x(500)]
    mult_s: Mapped[intgr]
    name_s: Mapped[str_x(500)]

class ExchangeRate(Base):
    __tablename__ = "exchange_rate"
    id: Mapped[uuidpk]
    code: Mapped[str_x(20)]
    rate: Mapped[real]

class AppSettings(Base):
    __tablename__ = "app_settings"
    id: Mapped[uuidpk]
    param: Mapped[str_x(100)]
    var: Mapped[str_x(100)]

class BasePrice(Base):
    __tablename__ = "base_price"
    __table_args__ = (Index("base_price_article_brand_index", "article", "brand"),)
    id: Mapped[intpk]
    # Артикул varchar(256),
    article: Mapped[str_x(256)]
    # Бренд varchar(256),
    brand: Mapped[str_x(256)]
    # brand_low: Mapped[str_x(256)]
    # ЦенаБ numeric(12,2),
    price_b: Mapped[numeric]
    duple: Mapped[Boolean] = mapped_column(Boolean, default=True)
    # ЦенаМин numeric(12,2),
    min_price: Mapped[numeric]
    # ЦенаМинПоставщик varchar(20)
    min_supplier: Mapped[str_x(20)]

class MassOffers(Base):
    __tablename__ = "mass_offers"
    __table_args__ = (Index("mass_offers_article_brand_index", "article", "brand"),
                      Index("mass_offers_article_brand_price_index", "article", "brand", "price_code"),)
    id: Mapped[intpk]
    # Артикул varchar(256),
    article: Mapped[str_x(256)]
    # Бренд varchar(256),
    brand: Mapped[str_x(256)]
    # brand_low: Mapped[str_x(256)]
    price_code: Mapped[str_x(20)]
    duple: Mapped[Boolean] = mapped_column(Boolean, default=False)
    # Предложений_в_опте integer
    offers_count: Mapped[int] = mapped_column(Integer, default=1)

class PriceReport(Base):
    __tablename__ = "price_report"
    # Полное_название varchar(256),
    id: Mapped[uuidpk]
    file_name: Mapped[str_x(256)] #= mapped_column(String(256), primary_key=True)
    # Название_файла varchar(256),
    price_code: Mapped[str_x(256)]
    # Время_изменения varchar(256)
    info_message: Mapped[str_x(256)]
    info_message2: Mapped[str_x(256)]
    row_count: Mapped[intgr]
    row_wo_article: Mapped[intgr]
    updated_at: Mapped[datetime.datetime] = mapped_column(nullable=True)
    updated_at_2_step: Mapped[datetime.datetime] = mapped_column(nullable=True)
    row_count_2: Mapped[intgr]
    del_pos: Mapped[intgr]

# class PriceUpdateTime(Base):
#     __tablename__ = "price_update_time"
#     price_name: Mapped[str] = mapped_column(String(100), primary_key=True)
#     info_message: Mapped[str] = mapped_column(String(100), nullable=True)
#     updated_at: Mapped[datetime.datetime]
# class PriceReport(Base):
#     __tablename__ = "price_report"
#     price_name: Mapped[str] = mapped_column(String(100), primary_key=True)
#     info_message: Mapped[str] = mapped_column(String(100), nullable=True)
#     updated_at: Mapped[datetime.datetime]

class CatalogUpdateTime(Base):
    __tablename__ = "catalog_update_time"
    catalog_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    updated_at: Mapped[datetime.datetime]
