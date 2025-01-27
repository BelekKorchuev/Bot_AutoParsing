import os
import logging
import subprocess

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import re

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Загрузка переменных окружения
load_dotenv()
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

XML_FILE_PATH = "banks.xml"
BIC_PATTERN = r"БИК.*?(04\d{7})"

# Функция для получения названия банка по БИК из XML

def get_bank_name_from_xml(bic):
    """
    Ищет название банка по БИК в XML-файле.
    Если указан PrntBIC, ищет название родительского банка.
    """
    try:

        tree = ET.parse(XML_FILE_PATH)
        root = tree.getroot()

        namespace = "urn:cbr-ru:ed:v2.0"  # Указать пространство имен, если необходимо
        bic_entry = root.find(f".//{{{namespace}}}BICDirectoryEntry[@BIC='{bic}']")

        if bic_entry is None:
            return "не найден", "БИК не найден в базе"

        participant_info = bic_entry.find(f"{{{namespace}}}ParticipantInfo")
        if participant_info is None:
            return "не найден", "Отсутствует информация о банке"

        prnt_bic = participant_info.get("PrntBIC")
        if prnt_bic:
            parent_entry = root.find(f".//{{{namespace}}}BICDirectoryEntry[@BIC='{prnt_bic}']")
            if parent_entry is not None:  # Используем явную проверку вместо if parent_entry
                parent_info = parent_entry.find(f"{{{namespace}}}ParticipantInfo")
                if parent_info is not None:
                    return parent_info.get("NameP"), ""

        return participant_info.get("NameP"), ""

    except ET.ParseError:
        return "не найден", "Ошибка парсинга XML"
    except Exception as e:
        return "не найден", f"Ошибка: {str(e)}"


def setup_virtual_display():
    try:
        xvfb_process = subprocess.Popen(['Xvfb', ':113', '-screen', '0', '1920x1080x24', '-nolisten', 'tcp'])
        os.environ["DISPLAY"] = ":113"
        logging.info("Виртуальный дисплей успешно настроен с использованием Xvfb.")
        return xvfb_process
    except Exception as e:
        logging.error(f"Ошибка при настройке виртуального дисплея: {e}")
        return None

def create_webdriver():
    """
    Создает WebDriver с виртуальным дисплеем.
    """
    # Настройка виртуального дисплея
    xvfb_process = setup_virtual_display()
    if not xvfb_process:
        raise RuntimeError("Не удалось настроить виртуальный дисплей.")

    # Настройка WebDriver
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
    driver.xvfb_process = xvfb_process  # Сохраняем процесс для последующего завершения
    return driver

def cleanup_virtual_display(driver):
    """
    Завершает процесс Xvfb.
    """
    if hasattr(driver, "xvfb_process") and driver.xvfb_process:
        driver.xvfb_process.terminate()
        logging.info("Процесс Xvfb завершен.")

def check_connection(connection):
    try:
        if connection.closed:
            logging.info("Переподключение к базе данных...")
            connection = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
            )
            connection.autocommit = True
        return connection
    except psycopg2.Error as e:
        logging.error(f"Ошибка переподключения к базе данных: {e}")
        raise



def process_link(link, driver):
    """
    Обрабатывает ссылку, используя Selenium, и возвращает найденный банк и БИК.
    """
    try:
        driver.get(link)
        wait = WebDriverWait(driver, 10)

        page_number = 1
        while True:
            logging.info(f"Открыта страница {page_number} по ссылке: {link}")

            try:
                table = wait.until(EC.presence_of_element_located((By.ID, "ctl00_cphBody_gvMessages")))
            except Exception as e:
                logging.error(f"Ошибка загрузки таблицы на странице {page_number}: {e}")
                return "не найден", "Не найдена таблица сообщений", None

            rows = table.find_elements(By.TAG_NAME, "tr")

            for row in rows:
                columns = row.find_elements(By.TAG_NAME, "td")
                if len(columns) > 1 and "Объявление о проведении торгов" in columns[1].text:
                    logging.info(f"Найдена строка с объявлением на странице {page_number}: {columns[1].text}")

                    link_element = columns[1].find_element(By.TAG_NAME, "a")
                    href = link_element.get_attribute("href")

                    if not href:
                        onclick_value = link_element.get_attribute("onclick")
                        if onclick_value:
                            match = re.search(r"openNewWin\('([^']+)", onclick_value)
                            if match:
                                relative_url = match.group(1)
                                base_url = "https://old.bankrot.fedresurs.ru"
                                href = f"{base_url}{relative_url}"
                                logging.info(f"Сформированная ссылка из onclick: {href}")

                    if href:
                        driver.execute_script("window.open(arguments[0]);", href)
                        driver.switch_to.window(driver.window_handles[-1])

                        try:
                            messages = WebDriverWait(driver, 10).until(
                                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.msg"))
                            )
                            for msg in messages:
                                text = msg.text
                                logging.info(f"Проверка сообщения: {text[:100]}...")
                                matches = re.findall(BIC_PATTERN, text, re.IGNORECASE)
                                if matches:
                                    bic = matches[-1]
                                    bank_name, reason = get_bank_name_from_xml(bic)
                                    logging.info(f"Найден БИК: {bic}, Банк: {bank_name}")
                                    return bank_name, reason, bic

                            logging.info("БИК не найден в div.msg, ищем в строках с 'Правила подачи заявок'")
                            rules_rows = driver.find_elements(By.TAG_NAME, "tr")
                            for rules_row in rules_rows:
                                if "Правила подачи заявок" in rules_row.text:
                                    cells = rules_row.find_elements(By.TAG_NAME, "td")
                                    for cell in cells:
                                        matches = re.findall(BIC_PATTERN, cell.text, re.IGNORECASE)
                                        if matches:
                                            bic = matches[-1]
                                            bank_name, reason = get_bank_name_from_xml(bic)
                                            logging.info(f"Найден БИК в 'Правила подачи заявок': {bic}, Банк: {bank_name}")
                                            return bank_name, reason, bic

                        finally:
                            cleanup_virtual_display(driver)
                            driver.close()
                            driver.switch_to.window(driver.window_handles[0])
            if page_number > 30:
                logging.info("Количество страниц превысило 30, БИК не найден. Переход к следующей ссылке.")
                return "не найден", "Пройдено более 30 страниц и не найден БИК", None

            try:
                pagination = driver.find_elements(By.CLASS_NAME, "pager")
                if pagination:
                    next_buttons = pagination[0].find_elements(By.TAG_NAME, "a")
                    next_button = None

                    if page_number % 10 == 0:
                        ellipsis_buttons = [button for button in next_buttons if button.text == "..."]
                        if len(ellipsis_buttons) == 2:
                            next_button = ellipsis_buttons[1]
                        elif len(ellipsis_buttons) == 1:
                            next_button = ellipsis_buttons[0]

                    else:
                        for button in next_buttons:
                            if button.text == str(page_number + 1):
                                next_button = button
                                break

                    if next_button:
                        logging.info(f"Переход на страницу {page_number + 1} через кнопку: {next_button.text}")
                        next_button.click()
                        page_number += 1
                        WebDriverWait(driver, 10).until(EC.staleness_of(table))
                    else:
                        logging.info("Не удалось найти кнопку для перехода на следующую страницу.")
                        return "не найден", "Не найдено объявление", None
                else:
                    logging.info("Пагинация завершена. Объявления больше не найдено.")
                    return "не найден", "Не найдено объявление", None
            except Exception as e:
                logging.error(f"Ошибка при переходе на следующую страницу: {e}")
                return "не найден", "Ошибка обработки пагинации", None

        return "не найден", "", None
    except Exception as e:
        logging.error(f"Ошибка при обработке ссылки {link}: {e}")
        return "не найден", "Ошибка обработки ссылки", None

# Основная функция

def main():
    import time  # Импорт для работы с задержками в цикле
    try:
        # Подключение к базе данных
        connection = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        connection.autocommit = True
        connection = check_connection(connection)

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            logging.info("Начинаем обработку таблицы dolzhnik. Подключение к базе данных успешно.")

            while True:
                logging.info("Выполняем запрос к таблице dolzhnik на пустые записи Статус_банка...")
                connection = check_connection(connection)
                cursor.execute("SELECT * FROM dolzhnik WHERE Статус_банка IS NULL OR Статус_банка = ''")
                rows_dolzhnik = cursor.fetchall()

                logging.info("Выполняем запрос к таблице messages для записей с типом сообщения 'объявлен' и пустым Статус_банка...")
                cursor.execute("SELECT * FROM messages WHERE Статус_банка IS NULL AND LOWER(тип_сообщения) LIKE '%объявлен%' LIMIT 1000")
                rows_messages = cursor.fetchall()

                logging.info(f"Найдено записей в dolzhnik: {len(rows_dolzhnik)}")
                logging.info(f"Найдено записей в messages: {len(rows_messages)}")

                if not rows_dolzhnik and not rows_messages:
                    logging.info("Нет новых записей. Ожидание новых данных...")
                    time.sleep(60)
                    continue

                driver = create_webdriver()

                try:
                    logging.info(f"Начинаем обработку {len(rows_dolzhnik)} записей из таблицы dolzhnik...")
                    for row in rows_dolzhnik:
                        connection = check_connection(connection)  # Проверяем соединение перед использованием
                        link = row.get("Должник_ссылка_ЕФРСБ")
                        if link:
                            bank_name, reason, bic = process_link(link, driver)

                            if bank_name == "не найден":
                                cursor.execute(
                                    """
                                    UPDATE dolzhnik
                                    SET Статус_банка = %s
                                    WHERE Инн_Должника = %s
                                    """,
                                    (f"Не нашел: {reason}", row["Инн_Должника"])
                                )
                            else:
                                cursor.execute(
                                    """
                                    UPDATE dolzhnik
                                    SET Статус_банка = %s, Банк_в_котором_хранятся_деньги = %s
                                    WHERE Инн_Должника = %s
                                    """,
                                    (bic, bank_name, row["Инн_Должника"])
                                )

                    connection.commit()

                    logging.info(f"Начинаем обработку {len(rows_messages)} записей из таблицы messages...")
                    while rows_messages:
                        connection = check_connection(connection)  # Проверяем соединение перед каждым запросом
                        for row in rows_messages:
                            link = row.get("должник_ссылка")
                            inn = row.get("ИНН")

                            if link:
                                bank_name, reason, bic = process_link(link, driver)

                                if bank_name == "не найден":
                                    cursor.execute(
                                        """
                                        UPDATE messages
                                        SET Статус_банка = %s
                                        WHERE должник_ссылка = %s
                                        """,
                                        (f"Не нашел: {reason}", link)
                                    )
                                else:
                                    cursor.execute(
                                        """
                                        UPDATE messages
                                        SET Статус_банка = %s
                                        WHERE должник_ссылка = %s
                                        """,
                                        (bic, link)
                                    )

                                    if inn:
                                        cursor.execute(
                                            """
                                            UPDATE dolzhnik
                                            SET Статус_банка = %s, Банк_в_котором_хранятся_деньги = %s
                                            WHERE Инн_Должника = %s
                                            """,
                                            (bic, bank_name, inn)
                                        )

                        connection.commit()

                        cursor.execute("SELECT * FROM messages WHERE Статус_банка IS NULL AND LOWER(тип_сообщения) LIKE '%объявлен%' LIMIT 1000")
                        rows_messages = cursor.fetchall()

                finally:
                    logging.info("Завершаем работу WebDriver и закрываем браузер.")
                    driver.quit()

    except Exception as e:
        logging.error(f"Ошибка: {e}")
        if connection:
            connection = check_connection(connection)  # Проверяем соединение перед откатом
            connection.rollback()
    finally:
        if connection:
            connection = check_connection(connection)  # Убедиться, что соединение открыто
            connection.close()
            logging.info("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    main()
