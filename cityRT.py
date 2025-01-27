import os
import re
import asyncio
import asyncpg
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pytz
from datetime import datetime
from dotenv import load_dotenv

# Настройка логирования
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

load_dotenv()

# Данные для подключения к БД
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
from urllib.parse import quote_plus

DB_PASSWORD = quote_plus(os.getenv("DB_PASSWORD"))
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Регулярное выражение для поиска адреса
ADDRESS_ROW_PATTERN = r"Адрес для корреспонденции"


async def fetch_arbitr_managers(pool):
    async with pool.acquire() as conn:
        query = """
        SELECT ссылка_ЕФРСБ
        FROM arbitr_managers
        WHERE статус_города IS NULL
        """
        return await conn.fetch(query)


def create_webdriver():
    options = Options()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def extract_address_from_message(driver):
    """
    Извлекает адрес для корреспонденции из секций div.headInfo.
    """
    try:
        # Получаем все div с классом headInfo
        head_info_divs = driver.find_elements(By.CLASS_NAME, "headInfo")
        if not head_info_divs:
            logging.warning("Секции headInfo не найдены")
            return None

        for div in head_info_divs:
            # Ищем строки tr внутри текущего div
            rows = div.find_elements(By.TAG_NAME, "tr")
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")  # Получаем ячейки строки
                if len(cells) > 1:  # Проверяем, что есть хотя бы две ячейки
                    header = cells[0].text.strip()
                    if "Адрес для корреспонденции" in header:  # Проверяем текст в первой ячейке
                        address = cells[1].text.strip()
                        if address:  # Если адрес найден, возвращаем его
                            logging.info(f"Извлечен адрес для корреспонденции: {address}")
                            return address
                        else:
                            logging.warning("Ячейка с адресом для корреспонденции пустая")
                            return None

        logging.warning("Строка с адресом для корреспонденции не найдена")
    except Exception as e:
        logging.error(f"Ошибка извлечения адреса для корреспонденции: {e}", exc_info=True)
    return None



def find_city_by_postal_code(postal_code):
    """
    Ищет город по почтовому индексу через API zippopotam.us
    и возвращает очищенное название без лишних пробелов и цифр.
    """
    try:
        response = requests.get(f"https://api.zippopotam.us/ru/{postal_code}")
        if response.status_code == 200:
            data = response.json()
            city_name = data['places'][0]['place name']
            # Убираем только лишние цифры и пробелы в начале и конце
            cleaned_city_name = re.sub(r'^\s*\d+|\d+\s*$', '', city_name).strip()
            logging.info(f"Найден город по индексу {postal_code}: {cleaned_city_name}")
            return cleaned_city_name
    except Exception as e:
        logging.error(f"Ошибка при поиске города по индексу {postal_code}: {e}")
    return None



def find_city_by_regex(address):
    city_patterns = [
        r'\b(?:город|г\.?|деревня|д\.?|село|поселок|п\.?|пгт\.?|станица|ст\.?|хутор|аул)\s+([^,\.\s]+)',
        r'\b(?:г\.|д\.|п\.|пгт\.|ст\.|рп\.)\s*([^,\.\s]+)',
        r'\b(?:республика|респ\.?|область|обл\.?|край|автономный округ|АО|округ|регион|район|р-н)\s+\w+,\s*([^,\.\s]+)'
    ]
    for pattern in city_patterns:
        match = re.search(pattern, address, re.IGNORECASE)
        if match:
            logging.info(f"Найден город по регулярному выражению: {match.group(1)}")
            return match.group(1)
    return None


def get_timezone_by_city(city, timezones_dict):
    for tz in timezones_dict:
        if tz['Rus'] == city:
            return tz['TimeZone']
    return None



def get_msk_offset(timezone_str):
    try:
        tz = pytz.timezone(timezone_str)
        now = datetime.now(tz)
        offset = now.utcoffset().total_seconds() / 3600
        msk_offset = int(offset - 3)
        return f"+{msk_offset}" if msk_offset > 0 else str(msk_offset)
    except Exception as e:
        logging.error(f"Ошибка определения смещения для {timezone_str}: {e}")
    return None


async def load_timezones_from_db(pool):
    async with pool.acquire() as conn:
        query = "SELECT * FROM \"TimeZone\";"
        rows = await conn.fetch(query)
        return [dict(row) for row in rows]


async def update_arbitr_manager(pool, link, city, timezone, status):
    async with pool.acquire() as conn:
        assert isinstance(city, (str, type(None))), "Город должен быть строкой или None"
        assert isinstance(timezone, (str, type(None))), "Часовой пояс должен быть строкой или None"
        assert isinstance(status, str), "Статус должен быть строкой"
        assert isinstance(link, str), "Ссылка должна быть строкой"

        logging.info(f"Обновление записи: city={city}, timezone={timezone}, status={status}, link={link}")

        query = """
        UPDATE arbitr_managers
        SET город_АУ = $1, часовой_пояс = $2, статус_города = $3
        WHERE ссылка_ЕФРСБ = $4
        """
        await conn.execute(query, city, timezone, status, link)


def get_first_row_link(row):
    """Получает ссылку из строки таблицы, поддерживая href и onclick."""
    try:
        link_element = row.find_element(By.TAG_NAME, "a")
        href = link_element.get_attribute("href")
        if href:
            logging.info(f"Найдена ссылка (href): {href}")
            return href

        # Если href отсутствует, проверяем onclick
        onclick = link_element.get_attribute("onclick")
        if onclick:
            base_url = "https://old.bankrot.fedresurs.ru"
            match = re.search(r"openNewWin\('([^']+)", onclick)
            if match:
                relative_url = match.group(1)
                full_url = f"{base_url}{relative_url}"
                logging.info(f"Ссылка сформирована из onclick: {full_url}")
                return full_url

        logging.warning("Ссылка не найдена ни в href, ни в onclick.")
    except Exception as e:
        logging.error(f"Ошибка при извлечении ссылки: {e}")
    return None


async def process_manager(pool, driver, manager, timezones_dict):
    link = manager['ссылка_ЕФРСБ']
    logging.info(f"Переход по ссылке: {link}")
    driver.get(link)

    try:
        table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "ctl00_cphBody_gvMessages")))
        logging.info("Таблица сообщений найдена")

        rows = table.find_elements(By.TAG_NAME, "tr")
        if rows:
            first_row_link = get_first_row_link(rows[1])  # Пропускаем заголовок таблицы

            if first_row_link:
                logging.info(f"Переход по сформированной ссылке: {first_row_link}")
                driver.execute_script("window.open(arguments[0]);", first_row_link)
                driver.switch_to.window(driver.window_handles[-1])
            else:
                logging.warning("Не удалось найти ссылку в первой строке таблицы.")
                await update_arbitr_manager(pool, link, None, None, "Ссылка на сообщение не найдена")
                return
        else:
            logging.warning("Таблица сообщений пуста.")
            await update_arbitr_manager(pool, link, None, None, "Сообщений в таблице нет")
            return

        address = extract_address_from_message(driver)
        if not address:
            logging.warning("Адрес не найден")
            await update_arbitr_manager(pool, link, None, None, "Адрес не найден")
            return

        postal_code_match = re.search(r"\b\d{6}\b", address)
        city = None
        status = ""

        if postal_code_match:
            postal_code = postal_code_match.group(0)
            logging.info(f"Найден почтовый индекс: {postal_code}")
            city = find_city_by_postal_code(postal_code)
            if city:
                status = f"Найден по почтовому индексу: {postal_code}"
            else:
                status = "Город не найден по индексу"

        if not city:
            city = find_city_by_regex(address)
            if city:
                status = "Найден по регулярному выражению"
            else:
                status = "Город не найден"

        timezone = None
        if city:
            timezone = get_timezone_by_city(city, timezones_dict)
            if timezone:
                msk_offset = get_msk_offset(timezone)
                logging.info(f"Найден часовой пояс: {timezone}, смещение: {msk_offset}")
            else:
                status = "Часовой пояс не найден в БД"

        await update_arbitr_manager(pool, link, city, msk_offset if timezone else None, status)

    finally:
        driver.close()
        driver.switch_to.window(driver.window_handles[0])


async def main():
    pool = await asyncpg.create_pool(DATABASE_URL)
    driver = create_webdriver()

    try:
        timezones_dict = await load_timezones_from_db(pool)
        managers = await fetch_arbitr_managers(pool)

        for manager in managers:
            await process_manager(pool, driver, manager, timezones_dict)

    finally:
        await pool.close()
        driver.quit()


if __name__ == "__main__":
    asyncio.run(main())
