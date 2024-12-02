import asyncio
import re
from logScript import logger
import json
import os
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

def clean_sro(sro_text):
    """
    Удаляет части вида (ИНН XXXXXXXX, ОГРН XXXXXXXXXX) из строки СРО_АУ.
    """
    return re.sub(r'\s*\(ИНН[:\s]*\d+,?\s*ОГРН[:\s]*\d+\)', '', str(sro_text)).strip()


# Функции для обработки данных
def extract_inn(text):
    match = re.search(r'ИНН[:\s]*(\d+)', str(text))
    return match.group(1) if match else None


def clean_fio(text):
    return re.sub(r'\s*\(ИНН[:\s]*\d+.*?СНИЛС.*?\)', '', str(text)).strip()


# Основная функция
async def au_debtorsDetecting(data):
    # Проверка типа данных, если передан словарь, преобразуем его в список
    if isinstance(data, dict):
        data = [data]

    async with AsyncSessionLocal() as session:
        metadata = MetaData()
        async with engine.begin() as connection:
            await connection.run_sync(metadata.reflect)

        arbitr_managers_table = metadata.tables.get('arbitr_managers')
        dolzhnik_table = metadata.tables.get('dolzhnik')

        if arbitr_managers_table is None or dolzhnik_table is None:
            logger.error("Одна или несколько таблиц не найдены.")
            return

        for message_row in data:
            try:
                raw_fio = message_row['ФИО_АУ']
                arbiter_link = message_row['арбитр_ссылка']
                address = message_row['адрес_корреспонденции']
                sro = clean_sro(message_row['СРО_АУ'])
                email = message_row['почта']
                message_inn = message_row['ИНН']
                debtor_name = message_row['наименование_должника']
                debtor_link = message_row['должник_ссылка']
                case_number = message_row['номер_дела']

                # Пропускаем запись, если ссылка содержит OrgToCard или PrsToCard
                if "OrgToCard" in arbiter_link or "PrsToCard" in arbiter_link:
                    logger.info(
                        f"Ссылка {arbiter_link} содержит OrgToCard или PrsToCard. Запись пропускается.")
                    continue

                inn_au = extract_inn(raw_fio)
                if not inn_au:
                    logger.info(f"ИНН не удалось извлечь из строки ФИО_АУ: {raw_fio}. Запись игнорируется.")
                    continue

                # Проверка наличия арбитражного управляющего в таблице arbitr_managers
                arbitr_manager_query = select(arbitr_managers_table.c['ИНН_АУ']).where(
                    arbitr_managers_table.c['ИНН_АУ'] == inn_au
                )
                existing_manager = await session.execute(arbitr_manager_query)
                if existing_manager.fetchone():
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

                # Проверка наличия должника в таблице dolzhnik
                debtor_query = select(dolzhnik_table.c['Инн_Должника']).where(
                    dolzhnik_table.c['Инн_Должника'] == message_inn
                )
                existing_debtor = await session.execute(debtor_query)
                if existing_debtor.fetchone():
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

                await session.commit()
                logger.info("Изменения зафиксированы.")

            except Exception as e:
                logger.error(f"Ошибка при обработке: {e}")
                await session.rollback()
