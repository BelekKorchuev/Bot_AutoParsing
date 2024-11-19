import logging
import time
import re
import json
from sqlalchemy import create_engine, Table, Column, String, Integer, MetaData, select, insert, exists
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Подключение к базе данных PostgreSQL
DATABASE_URL = r"postgresql+psycopg2://gen_user:\mk+{TSH3./:V6@176.53.160.95:5432/default_db"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Определение метаданных и таблиц
metadata = MetaData()

# Таблица 'arbitr_managers'
arbitr_managers = Table(
    'arbitr_managers', metadata,
    Column('ИНН_АУ', String),
    Column('ФИО_АУ', String),
    Column('ссылка_ЕФРСБ', String),
    Column('город_АУ', String),
    Column('СРО_АУ', String),
    Column('почта_ау', String),
)

# Таблица 'messages'
messages = Table(
    'messages', metadata,
    Column('id', Integer),
    Column('ФИО_АУ', String),
    Column('арбитр_ссылка', String),
    Column('адрес_корреспонденции', String),
    Column('СРО_АУ', String),
    Column('почта', String),
)

# Путь к файлу для хранения последних 5 ID
LAST_PROCESSED_FILE = "last_processed_ids.json"

# Функция для чтения последних 5 обработанных ID
def read_last_processed_ids():
    try:
        with open(LAST_PROCESSED_FILE, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

# Функция для сохранения последних 5 обработанных ID
def save_last_processed_ids(new_id):
    last_ids = read_last_processed_ids()
    last_ids.append(new_id)
    last_ids = sorted(set(last_ids))[-5:]  # Храним только последние 5 уникальных ID
    with open(LAST_PROCESSED_FILE, "w") as file:
        json.dump(last_ids, file)
    return last_ids

# Функция для получения максимального ID из сохраненных
def get_last_processed_id():
    last_ids = read_last_processed_ids()
    return max(last_ids) if last_ids else 0

# Функция для извлечения ИНН из строки ФИО_АУ
def extract_inn(text):
    text = str(text)
    match = re.search(r'ИНН\s*(\d+)', text)
    return match.group(1) if match else None

# Функция для очистки ФИО_АУ от лишних данных
def clean_fio(text):
    """
    Удаляет ИНН и СНИЛС из строки ФИО_АУ, оставляя только ФИО.
    """
    text = str(text)
    # Убираем части вида "(ИНН 123456789012, СНИЛС 123-456-789 00)"
    return re.sub(r'\s*\(ИНН\s*\d+,\s*СНИЛС\s*\d{3}-\d{3}-\d{3}\s*\d{2}\)', '', text).strip()

# Основная функция для обработки новых данных
def process_messages():
    session = Session()
    last_processed_id = get_last_processed_id()

    while True:
        try:
            # Проверяем новые данные
            log_message = "Проверяем новые данные в таблице 'messages'."
            logging.info(log_message)
            print(log_message)

            # Извлекаем записи с ID больше последнего обработанного
            records = session.execute(select(
                messages.c.id,
                messages.c.ФИО_АУ,
                messages.c.арбитр_ссылка,
                messages.c.адрес_корреспонденции,
                messages.c.СРО_АУ,
                messages.c.почта
            ).where(messages.c.id > last_processed_id).order_by(messages.c.id)).fetchall()

            if not records:
                log_message = "Новых записей не найдено. Ожидание..."
                logging.info(log_message)
                print(log_message)
                time.sleep(3)
                continue

            for record in records:
                last_processed_id = record.id  # Обновляем последний обработанный ID
                save_last_processed_ids(last_processed_id)  # Сохраняем ID в файл

                # Логируем обработанный ID
                log_message = f"Обрабатывается запись с ID: {last_processed_id}"
                logging.info(log_message)
                print(log_message)

                # Проверяем, подходит ли ссылка
                арбитр_ссылка = record.арбитр_ссылка
                if not арбитр_ссылка.startswith("https://old.bankrot.fedresurs.ru/ArbitrManagerCard.aspx?"):
                    log_message = f"Ссылка {арбитр_ссылка} не соответствует шаблону. Запись игнорируется."
                    logging.info(log_message)
                    print(log_message)
                    continue

                # Обработка данных
                try:
                    raw_fio = record.ФИО_АУ
                    адрес_корреспонденции = record.адрес_корреспонденции
                    СРО_АУ = record.СРО_АУ
                    почта = record.почта

                    # Извлечение ИНН из ФИО_АУ
                    ИНН_АУ = extract_inn(raw_fio)
                    cleaned_fio = clean_fio(raw_fio)  # Очищаем ФИО

                    if not ИНН_АУ:
                        log_message = f"ИНН не удалось извлечь из строки ФИО_АУ: {raw_fio}. Запись игнорируется."
                        logging.info(log_message)
                        print(log_message)
                        continue

                    # Проверка наличия ИНН в arbitr_managers
                    inn_exists = session.query(exists().where(arbitr_managers.c.ИНН_АУ == ИНН_АУ)).scalar()

                    if inn_exists:
                        # Если ИНН уже существует, игнорируем запись
                        log_message = f"ИНН {ИНН_АУ} уже существует в таблице 'arbitr_managers'. Запись игнорируется."
                        logging.info(log_message)
                        print(log_message)
                        continue
                    else:
                        # Логи для добавления новой записи
                        log_message = f"Добавляем новую запись с ИНН: {ИНН_АУ}"
                        logging.info(log_message)
                        print(log_message)

                        # Добавляем новую запись
                        session.execute(
                            insert(arbitr_managers).values(
                                ИНН_АУ=ИНН_АУ,
                                ФИО_АУ=cleaned_fio,
                                ссылка_ЕФРСБ=арбитр_ссылка,
                                город_АУ=адрес_корреспонденции,
                                СРО_АУ=СРО_АУ,
                                почта_ау=почта
                            )
                        )
                        log_message = f"Новая запись с ИНН {ИНН_АУ} успешно добавлена."
                        logging.info(log_message)
                        print(log_message)

                except SQLAlchemyError as db_error:
                    log_message = f"Ошибка базы данных при обработке записи с ID: {record.id}, ошибка: {db_error}"
                    logging.error(log_message)
                    print(log_message)

            session.commit()
            log_message = "Данные успешно обработаны и сохранены."
            logging.info(log_message)
            print(log_message)

        except Exception as e:
            log_message = f"Ошибка при обработке сообщений: {e}"
            logging.error(log_message)
            print(log_message)
            session.rollback()
        finally:
            time.sleep(3)

# Запуск функции
if __name__ == "__main__":
    process_messages()
