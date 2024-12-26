import os
import subprocess
import time
from threading import Thread
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from DBManager import prepare_data_for_db, insert_message_to_db
from detecting import fetch_and_parse_first_page, clear_form_periodically, parse_all_pages_reverse
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from fioDETECTING import au_debtorsDetecting
from lots_integrator import lots_analyze
from parsing import parse_message_page
from split import split_columns
from logScript import logger
from queue import Queue
from selenium.webdriver.common.proxy import *

# proxy_url = "160.86.242.23:8080"
# proxy = Proxy({
#     'proxyType': ProxyType.MANUAL,
#     'httpProxy': proxy_url,
#     'sslProxy': proxy_url,
#     'noProxy': ''})

# proxy = "54.67.125.45:3128"  # Замените на ваш прокси


# создание виртуального дисплея
def setup_virtual_display():
    """
    Настройка виртуального дисплея через Xvfb.
    """
    try:
        # Запуск Xvfb
        xvfb_process = subprocess.Popen(['Xvfb', ':100', '-screen', '0', '1920x1080x24', '-nolisten', 'tcp'])
        # Установка переменной окружения DISPLAY
        os.environ["DISPLAY"] = ":100"
        logger.info("Виртуальный дисплей успешно настроен с использованием Xvfb.")
        return xvfb_process
    except Exception as e:
        logger.error(f"Ошибка при настройке виртуального дисплея: {e}")
        return None

# создание веб драйвера с виртуальным дисплем
def create_webdriver_with_display():
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

# очистка виртуального дисплея
def cleanup_virtual_display(driver):
    """
    Завершает процесс Xvfb.
    """
    if hasattr(driver, "xvfb_process") and driver.xvfb_process:
        driver.xvfb_process.terminate()
        logger.info("Процесс Xvfb завершен.")

# Функция для перезапуска драйвера
def restart_driver(driver):
    try:
        cleanup_virtual_display(driver)
        driver.quit()  # Завершаем текущую сессию
    except Exception as e:
        logger.error(f"Ошибка при завершении WebDriver: {e}")
    return create_webdriver_with_display()

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
                    new_thread = Thread(target=clear_form_periodically, args=(3, 1, restart_queue), daemon=True,
                                        name="ClearFormThread")
                else:
                    continue

                threads[i] = new_thread  # Заменяем завершившийся поток на новый
                new_thread.start()

        time.sleep(5)  # Проверка каждые 5 секунд

# Функция проверки состояния браузера
def is_browser_alive(driver):
    """
    Проверяет, жив ли браузер.
    :param driver: WebDriver instance.
    :return: True, если браузер работает, иначе False.
    """
    try:
        driver.title  # Пробуем получить заголовок текущей страницы
        return True
    except Exception as e:
        logger.warning(f"Браузер не отвечает: {e}")
        return False

# Основной цикл программы
def main():
    driver = create_webdriver_with_display()  # Инициализация WebDriver

    # Очередь для перезапуска драйвера
    restart_queue = Queue()

    # Обход всех страниц при старте
    logger.info("Запускаем полный парсинг всех страниц.")
    parse_all_pages_reverse(driver)

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
            driver = restart_driver(driver)  # Перезапустите WebDriver

        time.sleep(0.5)  # Задержка перед следующим циклом
        print("\n \n Ожидание 0.5 секунды для следующего обновления...")

if __name__ == "__main__":
    main()
