from bs4 import BeautifulSoup
import hashlib
import time
from collections import deque
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from datetime import datetime, timedelta
from logScript import logger

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

def clear_form_periodically(driver, target_hour=0, target_minute=0, repeat_count=5, repeat_interval=5):
    """
    Нажимает на кнопку "Очистить" в заданное время суток (например, в 12:00 ночи), нажимая несколько раз подряд.

    :param driver: Объект Selenium WebDriver.
    :param target_hour: Час запуска (по умолчанию 0 - полночь).
    :param target_minute: Минута запуска (по умолчанию 0).
    :param repeat_count: Количество последовательных нажатий (по умолчанию 5).
    :param repeat_interval: Интервал между нажатиями в секундах (по умолчанию 5 секунд).
    """
    while True:
        try:
            # Текущее время
            now = datetime.now()
            target_time = datetime.combine(now.date(), datetime.min.time()) + timedelta(
                hours=target_hour, minutes=target_minute
            )

            # Если текущее время уже прошло заданное, берём следующий день
            if now >= target_time:
                target_time += timedelta(days=1)

            # Вычисляем время до следующего запуска
            sleep_time = (target_time - now).total_seconds()
            logger.info(f"Следующее нажатие кнопки 'Очистить' запланировано на {target_time}. Спим {sleep_time:.2f} секунд.")

            # Спим до нужного времени
            time.sleep(sleep_time)

            # Последовательно нажимаем на кнопку
            for i in range(1, repeat_count + 1):
                try:
                    # Ждём, пока кнопка станет кликабельной
                    WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="ctl00_cphBody_imgClear"]'))
                    )
                    # Нажимаем на кнопку
                    clear_button = driver.find_element(By.XPATH, '//*[@id="ctl00_cphBody_imgClear"]')
                    if clear_button.is_displayed():
                        logger.info("Кнопка 'Очистить' видима. Попытка нажатия...")
                        clear_button.click()
                        logger.info(
                            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Нажатие {i} из {repeat_count}: Кнопка 'Очистить' нажата.")
                    else:
                        logger.warning("Кнопка 'Очистить' скрыта!")
                    # clear_button.click()
                    # logger.info(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Нажатие {i} из {repeat_count}: Кнопка 'Очистить' нажата.")
                except Exception as e:
                    logger.error(f"Ошибка при нажатии кнопки 'Очистить' на попытке {i}: {e}")

                # Ждём перед следующим нажатием
                time.sleep(repeat_interval)

        except Exception as e:
            logger.error(f"Ошибка в функции clear_form_periodically: {e}")


def fetch_and_parse_first_page(driver):
    url = "https://old.bankrot.fedresurs.ru/Messages.aspx?attempt=1"
    logger.info(f'[{time.strftime("%Y-%m-%d %H:%M:%S")}] Открытие основной страницы: {url}')

    try:
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
                    "должник_ссылка": f"https://old.bankrot.fedresurs.ru{link_debtor}" if link_debtor else "Нет ссылки",
                    "арбитр": published_by,
                    "арбитр_ссылка": f"https://old.bankrot.fedresurs.ru{link_arbitr}" if link_arbitr else "Нет ссылки",
                    "сообщение_ссылка": f"https://old.bankrot.fedresurs.ru{link_messeges}" if link_messeges else "Нет ссылки",
                }

                # Проверяем, является ли сообщение новым
                if msg_id not in checked_messages:
                    checked_messages.append(msg_id)
                    logger.info('Найдено новое релевантное сообщение')
                    logger.debug(
                        f'Дата: {date}, Тип сообщения: {message_type}, Должник: {debtor}, Кем опубликовано: {published_by}')
                    return new_messages

    except Exception as e:
        logger.error(f'Ошибка при обработке страницы {url}: {e}')

    return None
