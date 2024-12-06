import re
import logging
import psycopg2
from transliterate import translit
import pytz
from datetime import datetime
import os
from logScript import logger


# Подключение к базе данных
def load_timezones_from_db():
    timezones_dict = []
    try:
        connection = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM \"TimeZone\";")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        timezones_dict = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Ошибка при подключении к базе данных: {e}")
    finally:
        if connection:
            connection.close()
    return timezones_dict


# Функция для поиска населенного пункта по почтовому индексу
def find_city_by_postal_code(postal_code):
    try:
        import requests
        response = requests.get(f'https://api.zippopotam.us/ru/{postal_code}')
        if response.status_code == 200:
            data = response.json()
            city_name = data['places'][0]['place name']
            city_name = re.sub(r'\d+', '', city_name).strip()
            return city_name
    except Exception as e:
        logger.error(f"Ошибка при поиске города по почтовому индексу {postal_code}: {e}")
    return None


# Аналогичные вспомогательные функции
def find_city_by_regex(address):
    city_patterns = [
        r'г\.\s*(\w+)',
        r'город\s+(\w+)',
        r'деревня\s+(\w+)',
        r'село\s+(\w+)',
        r'поселок\s+(\w+)',
        r'пгт\s+(\w+)',
    ]

    for pattern in city_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def get_msk_offset(timezone_str):
    try:
        tz = pytz.timezone(timezone_str)
        now = datetime.now(tz)
        offset = now.utcoffset().total_seconds() / 3600
        msk_offset = int(offset - 3)
        return f"+{msk_offset}" if msk_offset > 0 else str(msk_offset)
    except Exception as e:
        logger.error(f"Ошибка при определении смещения для {timezone_str}: {e}")
    return None


# Основная функция обработки
def process_address(data_dict):
    timezones_dict = load_timezones_from_db()
    address = data_dict.get('адрес_корреспонденции', '')
    postal_code_match = re.search(r'\b\d{6}\b', address)
    city = None
    method = ""
    timezone = None

    if postal_code_match:
        postal_code = postal_code_match.group(0)
        city = find_city_by_postal_code(postal_code)
        if city:
            method = f"Найден по почтовому индексу: {postal_code}"
            timezone = next((tz['TimeZone'] for tz in timezones_dict if tz['Rus'] == city), None)

    if not city:
        city_name_ru = find_city_by_regex(address)
        if city_name_ru:
            city = city_name_ru
            method = "Найден по регулярному выражению"
            timezone = next((tz['TimeZone'] for tz in timezones_dict if tz['Rus'] == city_name_ru), None)

    if timezone:
        msk_offset = get_msk_offset(timezone)
    else:
        msk_offset = None

    # Обновление словаря
    data_dict.update({
        'адрес_корреспонденции': city if city else 'Не найден',
        'часовой_пояс': msk_offset if msk_offset is not None else 'Не найден'
    })
    logger.info(f'Способом \'{method}\' найден город {city} для адресса {address} и часовой пояс={msk_offset}')
    return data_dict
