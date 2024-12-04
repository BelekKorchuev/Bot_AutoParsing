import re
import os
import psycopg2
from dotenv import load_dotenv
from logScript import logger
from city import process_address

# Загрузка переменных окружения
load_dotenv(dotenv_path='.env')

db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

# Функции для обработки данных
def clean_sro(sro_text):
    """
    Удаляет части вида (ИНН XXXXXXXX, ОГРН XXXXXXXXXX) из строки СРО_АУ.
    """
    return re.sub(r'\s*\(ИНН[:\s]*\d+,?\s*ОГРН[:\s]*\d+\)', '', str(sro_text)).strip()

def extract_inn(text):
    match = re.search(r'ИНН[:\s]*(\d+)', str(text))
    return match.group(1) if match else None

def clean_fio(text):
    return re.sub(r'\s*\(ИНН[:\s]*\d+.*?\u0421НИЛС.*?\)', '', str(text)).strip()

# Основная функция
def au_debtorsDetecting(data):
    # Проверка типа данных, если передан словарь, преобразуем его в список
    if isinstance(data, dict):
        data = [process_address(data)]

    try:
        connection = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cursor = connection.cursor()

        for message_row in data:
            try:
                raw_fio = message_row['ФИО_АУ']
                arbiter_link = message_row['арбитр_ссылка']
                address = message_row['адрес_корреспонденции']
                timezoneCity = message_row['часовой_пояс']
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
                cursor.execute(
                    """
                    SELECT ИНН_АУ FROM arbitr_managers WHERE ИНН_АУ = %s
                    """,
                    (inn_au,)
                )
                existing_manager = cursor.fetchone()
                if existing_manager:
                    logger.info(
                        f"ИНН {inn_au} уже существует. Запись игнорируется в таблице 'arbitr_managers'. ФИО_АУ: {raw_fio}")
                else:
                    cursor.execute(
                        """
                        INSERT INTO arbitr_managers (ИНН_АУ, ФИО_АУ, ссылка_ЕФРСБ, город_АУ, СРО_АУ, почта_ау, часовой_пояс)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (inn_au, clean_fio(raw_fio), arbiter_link, address, sro, email, timezoneCity)
                    )
                    logger.info(f"Добавлена запись в 'arbitr_managers' с ИНН {inn_au}. ФИО_АУ: {raw_fio}")

                # Проверка наличия должника в таблице dolzhnik
                cursor.execute(
                    """
                    SELECT Инн_Должника FROM dolzhnik WHERE Инн_Должника = %s
                    """,
                    (message_inn,)
                )
                existing_debtor = cursor.fetchone()
                if existing_debtor:
                    logger.info(
                        f"ИНН должника {message_inn} уже существует в таблице 'dolzhnik'. Запись игнорируется. Наименование должника: {debtor_name}")
                else:
                    cursor.execute(
                        """
                        INSERT INTO dolzhnik (Инн_Должника, Должник_текст, Должник_ссылка_ЕФРСБ, Должник_ссылка_ББ, Номер_дела, Фл_Юл, ЕФРСБ_ББ, АУ_текст, ИНН_АУ)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (message_inn, debtor_name, debtor_link, '', case_number,
                         'ЮЛ' if len(message_inn) == 10 else 'ФЛ' if len(message_inn) == 12 else '',
                         'ЕФРСБ', clean_fio(raw_fio), inn_au)
                    )
                    logger.info(
                        f"Добавлена запись в 'dolzhnik' с ИНН должника {message_inn}. Наименование должника: {debtor_name}")

                connection.commit()
                logger.info("Изменения зафиксированы.")

            except Exception as e:
                logger.error(f"Ошибка при обработке: {e}")
                connection.rollback()

    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")

    finally:
        if connection:
            cursor.close()
            connection.close()
