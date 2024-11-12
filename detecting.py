from bs4 import BeautifulSoup
import hashlib
import time
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

def fetch_and_parse_first_page(driver):
    url = "https://old.bankrot.fedresurs.ru/Messages.aspx?attempt=1"
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Открытие основной страницы...")

    # Открываем страницу
    driver.get(url)
    time.sleep(1)  # Ждем 1 секунду для загрузки контента

    # Получаем HTML-код страницы
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # Находим строки таблицы сообщений
    table_rows = soup.select("table tr")

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

            new_messages = {
                "date": date,
                "message_type": message_type,
                "debtor_name": debtor,
                "address": address,
                "arbiter_name": published_by,
                "message_link": f"https://old.bankrot.fedresurs.ru{link}" if link else "Нет ссылки"
            }

            # Проверяем, является ли сообщение новым
            if msg_id not in checked_messages:
                checked_messages.append(msg_id)

                print("Найдено релевантное сообщение:")
                print(f"Дата: {date}")
                print(f"Тип сообщения: {message_type}")
                print(f"Должник: {debtor}")
                print(f"Адрес: {address}")
                print(f"Кем опубликовано: {published_by}")
                print(f"Ссылка на сообщение: https://old.bankrot.fedresurs.ru{link}\n")
                return new_messages
