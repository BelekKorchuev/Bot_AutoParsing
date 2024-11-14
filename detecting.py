from ctypes import c_wchar, c_char

from bs4 import BeautifulSoup
import hashlib
import time
from collections import deque

# Допустимые типы сообщений
valid_message_types = {
    'Сведения о заключении договора купли-продажи',
    'Сообщение о результатах торгов',
    'Объявление о проведении торгов',
    'Отчет оценщика об оценке имущества должника',
    'Сообщение об изменении объявления о проведении торгов'
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
        published_by = cells[4].get_text(strip=True)
        link_messeges = cells[1].find("a")["href"] if cells[1].find("a") else None
        link_arbitr = cells[4].find("a")["href"] if cells[4].find("a") else None
        link_debtor = cells[2].find("a")["href"] if cells[2].find("a") else None


        # Проверка типа сообщения
        if message_type in valid_message_types:
            # Создаем уникальный идентификатор для строки
            msg_id = hashlib.md5((date + message_type + debtor).encode()).hexdigest()

            new_messages = {
                "дата": date,
                "тип_сообщения": message_type,
                "должник": debtor,
                "арбитр": published_by,
                "сообщение_ссылка": f"https://old.bankrot.fedresurs.ru{link_messeges}" if link_messeges else "Нет ссылки",
                "арбитр_ссылка": f"https://old.bankrot.fedresurs.ru{link_arbitr}" if link_arbitr else "Нет ссылки",
                "должник_ссылка": f"https://old.bankrot.fedresurs.ru{link_debtor}" if link_debtor else "Нет ссылки"
            }

            # Проверяем, является ли сообщение новым
            if msg_id not in checked_messages:
                checked_messages.append(msg_id)

                print("Найдено релевантное сообщение:")
                print(f"Дата: {date}")
                print(f"Тип сообщения: {message_type}")
                print(f"Должник: {debtor}")
                print(f"Кем опубликовано: {published_by}")
                print(f"Ссылка на сообщение: https://old.bankrot.fedresurs.ru{link_messeges}\n")
                return new_messages
