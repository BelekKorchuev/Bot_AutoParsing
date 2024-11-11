import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import hashlib
import time
import os
from collections import deque

# Допустимые типы сообщений
valid_message_types = {
    'Сведения о заключении договора купли-продажи',
    'Сообщение о результатах торгов',
    'Объявление о проведении торгов',
    'Отчет оценщика об оценке имущества должника'
}

# Идентификаторы для проверенных сообщений, с ограничением на 1000 элементов
checked_messages = deque(maxlen=1000)

# Конфигурация Chrome
chrome_options = Options()
# Убираем аргумент "--headless", чтобы открыть браузер в видимом режиме
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Инициализация драйвера с автоматической установкой ChromeDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Укажите путь к Excel-файлу
excel_file = "messages.xlsx"


def save_to_excel(data, file_name):
    """Сохраняет данные в Excel-файл, добавляя новые строки, если файл уже существует."""
    df = pd.DataFrame(data)
    if os.path.exists(file_name):
        # Если файл существует, добавляем данные в конец
        with pd.ExcelWriter(file_name, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
            df.to_excel(writer, index=False, header=False, startrow=writer.sheets["Sheet1"].max_row)
    else:
        # Создаем новый файл, если его нет
        df.to_excel(file_name, index=False)


def fetch_and_parse_first_page():
    url = "https://old.bankrot.fedresurs.ru/Messages.aspx?attempt=1"
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Открытие основной страницы...")

    # Открываем страницу
    driver.get(url)
    time.sleep(1)  # Ждем 1 секунду для загрузки контента

    # Получаем HTML-код страницы
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Находим строки таблицы сообщений
    table_rows = soup.select("table tr")

    new_messages = []
    for row in table_rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        # Извлекаем данные из ячеек
        date = cells[0].get_text(strip=True)
        message_type = cells[1].get_text(strip=True)
        debtor = cells[2].get_text(strip=True)
        address = cells[3].get_text(strip=True)
        published_by = cells[4].get_text(strip=True)
        link = cells[1].find("a")["href"] if cells[1].find("a") else None

        # Проверка типа сообщения
        if message_type in valid_message_types:
            # Создаем уникальный идентификатор для строки
            msg_id = hashlib.md5((date + message_type + debtor).encode()).hexdigest()

            # Проверяем, является ли сообщение новым
            if msg_id not in checked_messages:
                checked_messages.append(msg_id)  # Добавляем в очередь (старые автоматически удаляются при переполнении)

                # Добавляем данные для Excel
                new_messages.append({
                    "Дата": date,
                    "Тип сообщения": message_type,
                    "Должник": debtor,
                    "Адрес": address,
                    "Кем опубликовано": published_by,
                    "Ссылка на сообщение": f"https://old.bankrot.fedresurs.ru{link}" if link else "Нет ссылки"
                })

                # Выводим в консоль информацию только о релевантном сообщении
                print("Найдено релевантное сообщение:")
                print(f"Дата: {date}")
                print(f"Тип сообщения: {message_type}")
                print(f"Должник: {debtor}")
                print(f"Адрес: {address}")
                print(f"Кем опубликовано: {published_by}")
                print(f"Ссылка на сообщение: https://old.bankrot.fedresurs.ru{link}\n")

    # Если есть новые сообщения, сохраняем их в Excel сразу
    if new_messages:
        save_to_excel(new_messages, excel_file)
    else:
        print("Нет новых сообщений, соответствующих вашим критериям, на основной странице.")


# Основной цикл с обновлением каждые 0.5 секунды
try:
    while True:
        fetch_and_parse_first_page()
        print("Ожидание 0.5 секунды для следующего обновления...")
        time.sleep(0.5)  # Задержка 0.5 секунды перед следующим циклом проверки
except KeyboardInterrupt:
    print("Завершение работы бота.")
finally:
    driver.quit()
