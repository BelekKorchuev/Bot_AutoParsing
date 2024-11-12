import time
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from DBManager import prepare_data_for_db, get_db_connection, insert_message_to_db
from detecting import fetch_and_parse_first_page
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from parsing import parse_message_page

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
        print("Новых сообщений нет, продолжаем проверку...")
        time.sleep(0.5)
        continue

    link = new_messages["message_link"]
    try:
        message_content = parse_message_page(link, driver)
        new_messages['message_content'] = message_content
        print(new_messages)
        # Подготовка данных перед вставкой в базу
        prepared_data = prepare_data_for_db(new_messages)

        # Вставка данных в базу
        new_id = insert_message_to_db(prepared_data, connection)
        print(f"Данные успешно вставлены с ID: {new_id} \n\n")
    except Exception as e:
        print("Ошибка при парсинге страницы или вставке данных:", e)

    print("Ожидание 0.5 секунды для следующего обновления...")
    time.sleep(0.5)  # Задержка 0.5 секунды перед следующим циклом проверки
