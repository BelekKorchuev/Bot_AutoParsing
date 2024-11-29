import asyncio
import time
from threading import Thread
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from DBManager import prepare_data_for_db, insert_message_to_db
from detecting import fetch_and_parse_first_page, clear_form_periodically
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from fioDETECTING import fetch_data
from lots_integrator import lots_analyze
from parsing import parse_message_page
from split import split_columns
from logScript import logger
from queue import Queue

# Конфигурация Chrome
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Функция для создания нового драйвера
def create_driver():
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Функция для перезапуска драйвера
def restart_driver(driver):
    try:
        driver.quit()  # Завершаем текущую сессию
    except Exception as e:
        logger.error(f"Ошибка при завершении WebDriver: {e}")
    return create_driver()

# Функция для асинхронной обработки получения данных
async def fetch_data_periodically():
    try:
        await fetch_data()
    except Exception as e:
        logger.error(f"Ошибка в fetch_data: {e}")


def monitor_threads(threads, restart_queue):
    """
    Мониторинг состояния потоков. Перезапускает поток, если он завершился.
    """
    while True:
        for i, thread in enumerate(threads):
            if not thread.is_alive():
                logger.error(f"Поток {thread.name} завершился. Перезапуск...")

                # Перезапуск потока
                if thread.name == "ClearFormThread":
                    new_thread = Thread(target=clear_form_periodically, args=(17, 15, restart_queue), daemon=True,
                                        name="ClearFormThread")
                elif thread.name == "FetchDataThread":
                    new_thread = Thread(target=asyncio.run, args=(fetch_data_periodically(),), daemon=True,
                                        name="FetchDataThread")
                else:
                    continue

                threads[i] = new_thread  # Заменяем завершившийся поток на новый
                new_thread.start()

        time.sleep(5)  # Проверка каждые 5 секунд


# Основной цикл программы
def main():
    driver = create_driver()  # Инициализация WebDriver

    # Очередь для перезапуска драйвера
    restart_queue = Queue()

    # Список потоков
    threads = []

    # Создаём потоки
    clear_thread = Thread(target=clear_form_periodically, args=(17, 15, restart_queue), daemon=True, name="ClearFormThread")
    fetch_data_thread = Thread(target=asyncio.run, args=(fetch_data_periodically(),), daemon=True, name="FetchDataThread")

    # Запускаем потоки
    threads.append(clear_thread)
    threads.append(fetch_data_thread)

    for thread in threads:
        thread.start()

    # Мониторим потоки
    monitor_thread = Thread(target=monitor_threads, args=(threads, restart_queue), daemon=True, name="MonitorThread")
    monitor_thread.start()

    while True:
        try:
            # Проверка, нужно ли перезапустить драйвер
            if not restart_queue.empty():
                restart_signal = restart_queue.get()
                if restart_signal:
                    logger.info("Перезапуск сессии WebDriver.")
                    driver = restart_driver(driver)

            # Получаем новые сообщения
            new_messages = fetch_and_parse_first_page(driver)
            if new_messages is None:
                print("Новых сообщений нет, продолжаем проверку...\n\n")
                time.sleep(0.5)
                continue

            link = new_messages["сообщение_ссылка"]
            try:
                # Парсим содержимое сообщения
                message_content = parse_message_page(link, driver)
                new_messages['message_content'] = message_content

                # Подготовка данных перед вставкой в БД
                prepared_data = prepare_data_for_db(new_messages)
                logger.info(f'Сырые сообщения: %s' , str(prepared_data))

                # Вставляем данные в БД и получаем ID
                insert_message_to_db(prepared_data)

                # Форматируем данные
                formatted_data = split_columns(prepared_data)

                # Проверяем отформатированные данные
                lots_analyze(formatted_data)

            except Exception as e:
                logger.error(f"Ошибка при обработке сообщения: {e}")
                continue

        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")
            driver = restart_driver(driver)  # Перезапустите WebDriver

        time.sleep(0.5)  # Задержка перед следующим циклом
        print("\n \n Ожидание 0.5 секунды для следующего обновления...")

if __name__ == "__main__":
    main()
