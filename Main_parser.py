import time
from threading import Thread
from DBManager import prepare_data_for_db, insert_message_to_db
from detecting import fetch_and_parse_first_page, clear_form_periodically, parse_all_pages_reverse, pop_last_elem

from fioDETECTING import au_debtorsDetecting
from lots_integrator import lots_analyze
from parsing import parse_message_page
from split import split_columns
from logScript import logger
from queue import Queue
from webdriver import create_webdriver_with_display, cleanup_virtual_display, is_browser_alive, restart_driver

# from selenium.webdriver.common.proxy import *

# proxy_url = "160.86.242.23:8080"
# proxy = Proxy({
#     'proxyType': ProxyType.MANUAL,
#     'httpProxy': proxy_url,
#     'sslProxy': proxy_url,
#     'noProxy': ''})

# proxy = "54.67.125.45:3128"  # Замените на ваш прокси

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
                    new_thread = Thread(target=clear_form_periodically, args=(0, 2, restart_queue), daemon=True,
                                        name="ClearFormThread")
                else:
                    continue

                threads[i] = new_thread  # Заменяем завершившийся поток на новый
                new_thread.start()

        time.sleep(5)  # Проверка каждые 5 секунд

# Основной цикл программы
def main():
    while True:
        driver = create_webdriver_with_display()  # Инициализация WebDriver

        # Очередь для перезапуска драйвера
        restart_queue = Queue()

        # Обход всех страниц при старте
        logger.info("Запускаем полный парсинг всех страниц.")
        pars_sagnal = parse_all_pages_reverse(driver)
        if pars_sagnal is None:
            cleanup_virtual_display(driver)
            driver.quit()
            continue

        # Список потоков
        threads = []

        # Создаём потоки
        clear_thread = Thread(target=clear_form_periodically, args=(3, 1, restart_queue), daemon=True, name="ClearFormThread")

        # Запускаем потоки
        threads.append(clear_thread)
        clear_thread.start()

        # Мониторим потоки
        monitor_thread = Thread(target=monitor_threads, args=(threads, restart_queue), daemon=True, name="MonitorThread")
        monitor_thread.start()

        while True:
            try:
                # Проверка, нужно ли перезапустить драйвер
                if not is_browser_alive(driver):
                    logger.warning("Браузер перестал отвечать. Перезапуск...")
                    driver = restart_driver(driver)
                    continue

                # Проверка, нужно ли перезапустить драйвер
                if not restart_queue.empty():
                    restart_signal = restart_queue.get()
                    if restart_signal:
                        logger.info("Перезапуск сессии WebDriver.")
                        driver = restart_driver(driver)
                        continue

                # Получаем новые сообщения
                new_messages = fetch_and_parse_first_page(driver)
                if "not yet" in new_messages:
                    logger.warning("Новых сообщений нет, продолжаем проверку...\n\n")
                    time.sleep(0.5)
                    continue

                if new_messages is None:
                    logger.error(f"поизошла ошибка в fetch_and_parse_first_page: {e}")
                    driver = restart_driver(driver)
                    continue

                link = new_messages["сообщение_ссылка"]
                try:
                    # Парсим содержимое сообщения
                    message_content = parse_message_page(link, driver)
                    new_messages['message_content'] = message_content

                    # Подготовка данных перед вставкой в БД
                    prepared_data = prepare_data_for_db(new_messages)
                    logger.info(f'Сырые сообщения: %s' , str(prepared_data))

                    # добавление новых АУ и должников
                    au_debtorsDetecting(prepared_data)

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
                driver = restart_driver(driver)

                time.sleep(0.5)
                logger.info("Ожидание 0.5 секунды для следующего обновления...")

if __name__ == "__main__":
    main()
