import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, exists
from sqlalchemy.orm import sessionmaker

# Загрузка переменных окружения
load_dotenv(dotenv_path='.env')

db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

# Настройки логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_debtor(lots_data):
    connection_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata = MetaData()

    try:
        # Определение таблицы
        dolzhnik = Table('dolzhnik', metadata, autoload_with=engine)

        for lot in lots_data:
            # Извлечение данных из текущего лота
            debtor_inn = lot.get("ИНН_Должника")

            # Проверка наличия данных
            if not debtor_inn:
                raise ValueError(f"ИНН должника отсутствует в данных лота: {lot}")

            # Проверка должника в базе
            debtor_exists = session.query(
                exists().where(dolzhnik.c.Инн_Должника == debtor_inn)
            ).scalar()

            if not debtor_exists:
                raise Exception(f"Должник с ИНН {debtor_inn} из лота отсутствует в базе данных.")

            logging.info(f"Должник с ИНН {debtor_inn} из лота найден в базе данных.")

    finally:
        # Закрытие сессии
        session.close()




def dkp():
    print("hallo")

def result():
    print("hallo")

def annonce():
    print("hallo")

def grade():
    print("hallo")
