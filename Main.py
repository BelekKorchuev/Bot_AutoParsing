
import time

from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from DBManager import prepare_data_for_db, get_db_connection, insert_message_to_db
from detecting import fetch_and_parse_first_page
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

from lots_integrator import lots_analyze
from parsing import parse_message_page
from split import split_columns
from logScript import logger

# Конфигурация Chrome
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Инициализация драйвера с автоматической установкой ChromeDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
connection = get_db_connection()

while True:
    new_messages = fetch_and_parse_first_page(driver)
    if new_messages is None:
        print("Новых сообщений нет, продолжаем проверку...\n\n")
        time.sleep(0.5)
        continue

    link = new_messages["сообщение_ссылка"]
    try:
        # парсинг содержимого сообщения
        message_content = parse_message_page(link, driver)
        new_messages['message_content'] = message_content

        # Подготовка данных перед вставкой в базу
        prepared_data = prepare_data_for_db(new_messages)
        logger.info(f'Сырые сообщения: {prepared_data}')

        # отправка сырых собщений в БД и возврат их id
        new_id = insert_message_to_db(prepared_data, connection)

        try:
            # отправка данных на форматирование и разделение сообщений по лотам
            formatted_data = split_columns(prepared_data)

            # отправка данных о лоте в БД
            # insert_lots_to_db(formatted_data, connection)

            try:
                # проверка отформатированных данных на наличие должника и сравнение лота с базой
                lots_analyze(formatted_data)
            except Exception as e:
                print(f"Ошибка при проверке должника и лота: {e}")
                continue

        except Exception as e:
            print("Ошибка при форматировании и отправке данных в базу:", e)

    except Exception as e:
        print("Ошибка при парсинге страницы или вставке данных:", e)

    print("\n \n Ожидание 0.5 секунды для следующего обновления...")
    time.sleep(0.5)  # Задержка 0.5 секунды перед следующим циклом проверки
