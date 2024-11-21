import asyncio
import re
from Main import logger
import os
import json
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, select, insert

# Загрузка переменных окружения
load_dotenv(dotenv_path='.env')

db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

# Формируем строку подключения для SQLAlchemy с использованием asyncpg
db_url = f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Создаем асинхронный движок SQLAlchemy
engine = create_async_engine(db_url, echo=False, future=True)

# Создаем асинхронную сессию
AsyncSessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)

# Настройка логирования
logger.basicConfig(level=logger.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Файл для хранения последних 5 обработанных ID
LAST_PROCESSED_FILE = "last_processed_ids.json"

def clean_sro(sro_text):
    """
    Удаляет части вида (ИНН XXXXXXXX, ОГРН XXXXXXXXXX) из строки СРО_АУ.
    """
    return re.sub(r'\s*\(ИНН[:\s]*\d+,?\s*ОГРН[:\s]*\d+\)', '', str(sro_text)).strip()


# Функции для работы с последними обработанными ID
def read_last_processed_ids():
    try:
        with open(LAST_PROCESSED_FILE, "r") as file:
            return json.load(file)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_last_processed_ids(new_id):
    last_ids = read_last_processed_ids()
    last_ids.append(new_id)
    last_ids = sorted(set(last_ids))[-5:]  # Храним только последние 5 уникальных ID
    with open(LAST_PROCESSED_FILE, "w") as file:
        json.dump(last_ids, file)
    return last_ids


def get_last_processed_id():
    last_ids = read_last_processed_ids()
    return max(last_ids) if last_ids else 0


# Функции для обработки данных
def extract_inn(text):
    match = re.search(r'ИНН[:\s]*(\d+)', str(text))
    return match.group(1) if match else None


def clean_fio(text):
    return re.sub(r'\s*\(ИНН[:\s]*\d+.*?СНИЛС.*?\)', '', str(text)).strip()


# Основная функция
async def fetch_data():
    async with AsyncSessionLocal() as session:
        metadata = MetaData()
        async with engine.begin() as connection:
            await connection.run_sync(metadata.reflect)

        messages_table = metadata.tables.get('messages')
        arbitr_managers_table = metadata.tables.get('arbitr_managers')
        dolzhnik_table = metadata.tables.get('dolzhnik')

        # Проверяем, что таблицы были найдены корректно
        if messages_table is None or arbitr_managers_table is None or dolzhnik_table is None:
            logger.error("Одна или несколько таблиц не найдены.")
            return

        while True:
            try:
                last_processed_id = get_last_processed_id()
                logger.info(f"Последний обработанный ID: {last_processed_id if last_processed_id else 'Не найден'}")

                messages_query = select(
                    messages_table.c['id'],
                    messages_table.c['ФИО_АУ'],
                    messages_table.c['арбитр_ссылка'],
                    messages_table.c['адрес_корреспонденции'],
                    messages_table.c['СРО_АУ'],
                    messages_table.c['почта'],
                    messages_table.c['ИНН'],
                    messages_table.c['наименование_должника'],
                    messages_table.c['должник_ссылка'],
                    messages_table.c['номер_дела']
                ).where(messages_table.c['id'] > last_processed_id).order_by(messages_table.c['id'])

                messages_result_proxy = await session.execute(messages_query)
                messages_rows = messages_result_proxy.mappings().all()

                if not messages_rows:
                    logger.info("Новых строк для обработки нет. Ожидание...")
                    await asyncio.sleep(3)
                    continue

                for message_row in messages_rows:
                    message_id = message_row['id']
                    raw_fio = message_row['ФИО_АУ']
                    arbiter_link = message_row['арбитр_ссылка']
                    address = message_row['адрес_корреспонденции']
                    sro = clean_sro(message_row['СРО_АУ'])
                    email = message_row['почта']
                    message_inn = message_row['ИНН']
                    debtor_name = message_row['наименование_должника']
                    debtor_link = message_row['должник_ссылка']
                    case_number = message_row['номер_дела']

                    # Записываем ID даже если ссылка содержит OrgToCard или PrsToCard
                    if "OrgToCard" in arbiter_link or "PrsToCard" in arbiter_link:
                        logger.info(
                            f"Ссылка {arbiter_link} содержит OrgToCard или PrsToCard. Записываем ID и пропускаем запись.")
                        save_last_processed_ids(message_id)
                        last_processed_id = message_id
                        continue

                    inn_au = extract_inn(raw_fio)
                    if not inn_au:
                        logger.info(f"ИНН не удалось извлечь из строки ФИО_АУ: {raw_fio}. Запись игнорируется.")
                        continue

                    arbitr_manager_query = select(arbitr_managers_table.c['ИНН_АУ']).where(
                        arbitr_managers_table.c['ИНН_АУ'] == inn_au
                    )
                    existing_record = await session.execute(arbitr_manager_query)
                    record_exists = existing_record.fetchone()

                    if record_exists:
                        logger.info(
                            f"ИНН {inn_au} уже существует. Запись игнорируется в таблице 'arbitr_managers'. ФИО_АУ: {raw_fio}")
                    else:
                        insert_stmt = insert(arbitr_managers_table).values(
                            ИНН_АУ=inn_au,
                            ФИО_АУ=clean_fio(raw_fio),
                            ссылка_ЕФРСБ=arbiter_link,
                            город_АУ=address,
                            СРО_АУ=sro,
                            почта_ау=email
                        )
                        await session.execute(insert_stmt)
                        logger.info(f"Добавлена запись в 'arbitr_managers' с ИНН {inn_au}. ФИО_АУ: {raw_fio}")

                    # Теперь проверяем наличие записи в таблице dolzhnik
                    debtor_query = select(dolzhnik_table.c['Инн_Должника']).where(
                        dolzhnik_table.c['Инн_Должника'] == message_inn
                    )
                    existing_debtor = await session.execute(debtor_query)
                    debtor_exists = existing_debtor.fetchone()

                    if debtor_exists:
                        logger.info(
                            f"ИНН должника {message_inn} уже существует в таблице 'dolzhnik'. Запись игнорируется. Наименование должника: {debtor_name}")
                    else:
                        insert_debtor_stmt = insert(dolzhnik_table).values(
                            Инн_Должника=message_inn,
                            Должник_текст=debtor_name,
                            Должник_ссылка_ЕФРСБ=debtor_link,
                            Должник_ссылка_ББ='',
                            Номер_дела=case_number,
                            Фл_Юл=('ЮЛ' if len(message_inn) == 10 else 'ФЛ' if len(message_inn) == 12 else ''),
                            ЕФРСБ_ББ='ЕФРСБ',
                            АУ_текст=clean_fio(raw_fio),
                            ИНН_АУ=inn_au
                        )
                        await session.execute(insert_debtor_stmt)
                        logger.info(
                            f"Добавлена запись в 'dolzhnik' с ИНН должника {message_inn}. Наименование должника: {debtor_name}")

                    # Обновляем последний обработанный ID и сохраняем его в файл
                    last_processed_id = message_id
                    save_last_processed_ids(last_processed_id)

                await session.commit()
                logger.info("Все изменения зафиксированы.")

            except Exception as e:
                logger.error(f"Ошибка при обработке: {e}")
                await session.rollback()

            await asyncio.sleep(3)


if __name__ == "__main__":
    asyncio.run(fetch_data())
