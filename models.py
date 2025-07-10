import uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import REAL, NUMERIC, String, Uuid, text, Integer, Numeric, Index
import datetime

class Base(DeclarativeBase):
    pass

class Price(Base):
    __tablename__ = "price"
    __table_args__ = (Index("price_file_name_index", "file_name"),
                      Index("price_1_14_low_index", "_01article", "_14brand_filled_in_low"),
                      Index("price_1_7_14_low_index", "_01article", "_07supplier_code", "_14brand_filled_in_low"),
                      Index("price_7_14_low_index", "_07supplier_code", "_14brand_filled_in_low"),
                      Index("price_07_index", "_07supplier_code"),
                      Index("price_09_index", "_09code_supl_goods"),
                      Index("price_14_low_index", "_14brand_filled_in_low"),
                      Index("price_15_index", "_15code_optt"),
                      )
    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, server_default=text("gen_random_uuid()"))
    # Ключ1_поставщика varchar(256),
    key_s: Mapped[str] = mapped_column(String(256), nullable=True)
    # Артикул_поставщика varchar(256),
    article_s: Mapped[str] = mapped_column(String(256), nullable=True)
    # Производитель_поставщика varchar(256),
    brnd_s: Mapped[str] = mapped_column(String(256), nullable=True)
    # Наименование_поставщика varchar(256),
    name_s: Mapped[str] = mapped_column(String(256), nullable=True)
    # Количество_поставщика REAL,
    count_s: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Цена_поставщика NUMERIC(12,2),
    price_s: Mapped[Numeric] = mapped_column(Numeric(12,2), nullable=True)
    # Кратность_поставщика REAL,
    mult_s: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # Примечание_поставщика varchar(1000),
    notice_s: Mapped[str] = mapped_column(String(256), nullable=True)
    # _01Артикул varchar(256),
    _01article: Mapped[str] = mapped_column(String(256), nullable=True)
    # _02Производитель varchar(256),
    _02brand: Mapped[str] = mapped_column(String(256), nullable=True)
    # _03Наименование varchar(500),
    _03name: Mapped[str] = mapped_column(String(256), nullable=True)
    # _04Количество REAL,
    _04count: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # _05Цена NUMERIC(12,2),
    _05price: Mapped[Numeric] = mapped_column(Numeric(12,2), nullable=True)
    # _06Кратность_ REAL,
    _06milt_: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # _07Код_поставщика varchar(150),
    _07supplier_code: Mapped[str] = mapped_column(String(20), nullable=True)
    # _09Код_Поставщик_Товар varchar(500),
    _09code_supl_goods: Mapped[str] = mapped_column(String(300), nullable=True)
    # _10Оригинал varchar(30),
    _10original: Mapped[str] = mapped_column(String(30), nullable=True)
    # _13Градация REAL,
    _13grad: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # _14Производитель_заполнен varchar(1000),
    _14brand_filled_in: Mapped[str] = mapped_column(String(256), nullable=True)
    _14brand_filled_in_low: Mapped[str] = mapped_column(String(256), nullable=True)
    # _15КодТутОптТорг varchar(256),
    _15code_optt: Mapped[str] = mapped_column(String(256), nullable=True)
    # _17КодУникальности varchar(500),
    _17code_unique: Mapped[str] = mapped_column(String(256), nullable=True)
    # _18КороткоеНаименование varchar(256),
    _18short_name: Mapped[str] = mapped_column(String(256), nullable=True)
    # _19МинЦенаПоПрайсу varchar(50),
    _19min_price: Mapped[str] = mapped_column(String(50), nullable=True)
    # _20ИслючитьИзПрайса varchar(50),
    _20exclude: Mapped[str] = mapped_column(String(50), nullable=True)
    # Название_файла varchar(50),
    file_name: Mapped[str] = mapped_column(String(50), nullable=True)
    # Отсрочка REAL,
    delay: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Продаём_для_ОС varchar(20),
    sell_for_OS: Mapped[str] = mapped_column(String(20), nullable=True)
    # Наценка_для_ОС REAL,
    markup_os: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Макс_снижение_от_базовой_цены REAL,
    max_decline: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Наценка_на_праздники_1_02 REAL,
    markup_holidays: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Наценка_Р REAL,
    markup_R: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Мин_наценка REAL,
    min_markup: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Шаг_градаци REAL,
    grad_step: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Шаг_опт REAL,
    wholesale_step: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Разрешения_ПП varchar(3000),
    access_pp: Mapped[str] = mapped_column(String(500), nullable=True)
    # Процент_Отгрузки REAL,
    unload_percent: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # УбратьЗП varchar(3000),
    put_away_zp: Mapped[str] = mapped_column(String(500), nullable=True)
    # Предложений_опт REAL,
    offers_wholesale: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # ЦенаБ NUMERIC(12,2),
    price_b: Mapped[Numeric] = mapped_column(Numeric(12,2), nullable=True)
    # Низкая_цена NUMERIC(12,2),
    low_price: Mapped[Numeric] = mapped_column(Numeric(12,2), nullable=True)
    # Кол_во REAL,
    count: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # Наценка_ПБ REAL,
    markup_pb: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Код_ПБ_П varchar,
    code_pb_p: Mapped[str] = mapped_column(String(500), nullable=True)
    # _06Кратность REAL,
    _06milt: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # Кратность_меньше varchar(25),
    mult_less: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # _05Цена_плюс NUMERIC(12,2),
    _05price_plus: Mapped[Numeric] = mapped_column(Numeric(12,2), nullable=True)
    # ШтР integer DEFAULT 0,
    reserve_count: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # Количество_закупок REAL,
    buy_count: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # ЦенаМин numeric(12,2),
    min_price: Mapped[Numeric] = mapped_column(Numeric(12,2), nullable=True)
    # ЦенаМинПоставщик varchar(20)
    min_supplier: Mapped[str] = mapped_column(String(20), nullable=True)

class Data07(Base):
    __tablename__ = "data07"
    id: Mapped[int] = mapped_column(primary_key=True)
    # Работаем varchar(20),
    works: Mapped[str] = mapped_column(String(20), nullable=True)
    # Период_обновления_не_более REAL,
    update_time: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Настройка varchar(50),
    setting: Mapped[str] = mapped_column(String(50), nullable=True)#primary_key=True)
    # Отсрочка REAL,
    delay: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Продаём_для_ОС varchar(20) DEFAULT null,
    sell_os: Mapped[str] = mapped_column(String(20), nullable=True)
    # Наценка_для_ОС REAL,
    markup_os: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Макс_снижение_от_базовой_цены REAL,
    max_decline: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Наценка_на_праздники_1_02 REAL,
    markup_holidays: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Наценка_Р REAL,
    markup_R: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Мин_наценка REAL,
    min_markup: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Наценка_на_оптовые_товары REAL,
    markup_wholesale: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Шаг_градаци REAL,
    grad_step: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Шаг_опт REAL DEFAULT 0,
    wholesale_step: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Разрешения_ПП varchar(3000),
    access_pp: Mapped[str] = mapped_column(String(500), nullable=True)
    # Процент_Отгрузки REAL
    unload_percent: Mapped[REAL] = mapped_column(REAL, nullable=True)

class Data07_14(Base):
    __tablename__ = "data07_14"
    # id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, server_default=text("gen_random_uuid()"))
    id: Mapped[int] = mapped_column(primary_key=True)
    # Работаем varchar(20),
    works: Mapped[str] = mapped_column(String(20), nullable=True)
    # Период_обновления_не_более REAL,
    update_time: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Настройка varchar(100),
    setting: Mapped[str] = mapped_column(String(50), nullable=True) # primary_key=True)
    # Макс_снижение_от_базовой_цены REAL,
    max_decline: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Правильное varchar(256),
    correct: Mapped[str] = mapped_column(String(256), nullable=True) #primary_key=True)
    correct_low: Mapped[str] = mapped_column(String(256), nullable=True)
    # Наценка_ПБ REAL,
    markup_pb: Mapped[REAL] = mapped_column(REAL, nullable=True)
    # Код_ПБ_П varchar)
    code_pb_p: Mapped[str] = mapped_column(String(500), nullable=True)

class Data15(Base):
    __tablename__ = "data15"
    id: Mapped[int] = mapped_column(primary_key=True)
    # _15 varchar(300),
    code_15: Mapped[str] = mapped_column(String(500), nullable=True)
    # Предложений_опт REAL,
    offers_wholesale: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # ЦенаБ REAL
    price_b: Mapped[Numeric] = mapped_column(Numeric(12,2), nullable=True)

class Data09(Base):
    __tablename__ = "data09"
    id: Mapped[int] = mapped_column(primary_key=True)
    # УбратьЗП varchar(3000),
    put_away_zp: Mapped[str] = mapped_column(String(500), nullable=True)
    # ШтР REAL DEFAULT NULL,
    reserve_count: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # _09 varchar(300)
    code_09: Mapped[str] = mapped_column(String(300), nullable=True)

class Buy_for_OS(Base):
    __tablename__ = "buy_for_OS"
    id: Mapped[int] = mapped_column(primary_key=True)
    # Количество_закупок REAL,
    buy_count: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # АртикулПроизводитель varchar(300)
    article_producer: Mapped[str] = mapped_column(String(300), nullable=True)

class Reserve(Base):
    __tablename__ = "reserve"
    id: Mapped[int] = mapped_column(primary_key=True)
    # _09Код varchar(300),
    code_09: Mapped[str] = mapped_column(String(300), nullable=True)
    # ШтР REAL DEFAULT NULL,
    reserve_count: Mapped[Integer] = mapped_column(Integer, nullable=True)
    # _07Код varchar(30)
    code_07: Mapped[str] = mapped_column(String(30), nullable=True)

class BasePrice(Base):
    __tablename__ = "base_price"
    __table_args__ = (Index("base_price_article_brand_low_index", "article", "brand_low"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    # Артикул varchar(256),
    article: Mapped[str] = mapped_column(String(256), nullable=True)
    # Бренд varchar(256),
    brand: Mapped[str] = mapped_column(String(256), nullable=True)
    brand_low: Mapped[str] = mapped_column(String(256), nullable=True)
    # ЦенаБ numeric(12,2),
    price_b: Mapped[Numeric] = mapped_column(Numeric(12,2), nullable=True)
    # ЦенаМин numeric(12,2),
    min_price: Mapped[Numeric] = mapped_column(Numeric(12,2), nullable=True)
    # ЦенаМинПоставщик varchar(20)
    min_supplier: Mapped[str] = mapped_column(String(20), nullable=True)

class MassOffers(Base):
    __tablename__ = "mass_offers"
    __table_args__ = (Index("mass_offers_article_brand_low_index", "article", "brand_low"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    # Артикул varchar(256),
    article: Mapped[str] = mapped_column(String(256), nullable=True)
    # Бренд varchar(256),
    brand: Mapped[str] = mapped_column(String(256), nullable=True)
    brand_low: Mapped[str] = mapped_column(String(256), nullable=True)
    # Предложений_в_опте integer
    offers_count: Mapped[Integer] = mapped_column(Integer, nullable=True)


class PriceUpdateTime(Base):
    __tablename__ = "price_update_time"
    price_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    info_message: Mapped[str] = mapped_column(String(100), nullable=True)
    updated_at: Mapped[datetime.datetime]

class CatalogUpdateTime(Base):
    __tablename__ = "catalog_update_time"
    catalog_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    updated_at: Mapped[datetime.datetime]
