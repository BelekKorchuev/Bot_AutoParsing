from bs4 import BeautifulSoup
from pycparser.ply.yacc import token
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import time
from selenium.webdriver.support import expected_conditions as EC

from DBManager import get_db_connection, insert_message_to_db, prepare_data_for_db

# Настройка Selenium (установка драйвера Chrome)
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Функция для получения и парсинга HTML-кода по предоставленной ссылке
def parse_message_page(url, driver):
    # Переход по ссылке
    driver.get(url)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, 'red_small'))
    )
    # Подождем несколько секунд, чтобы страница полностью загрузилась
    time.sleep(2)

    # Получение HTML-кода страницы
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    # Словарь для сохранения данных
    data = {}

    # Извлечение заголовка сообщения
    title = soup.find('h1', class_='red_small').text.strip()
    data['title'] = title

    # Основная информация
    table_main = soup.find('table', class_='headInfo')
    if table_main:
        rows = table_main.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) == 2:
                field = cells[0].text.strip()
                value = cells[1].text.strip()
                data[field] = value

    # Данные о должнике
    debtor_section = soup.find('div', string="Должник")
    if debtor_section:
        debtor_table = debtor_section.find_next('table')
        if debtor_table:
            debtor_rows = debtor_table.find_all('tr')
            for row in debtor_rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    field = cells[0].text.strip()
                    value = cells[1].text.strip()
                    data[field] = value

    # Информация об арбитражном управляющем
    arbiter_section = soup.find('div', string="Кем опубликовано")
    if arbiter_section:
        arbiter_table = arbiter_section.find_next('table')
        if arbiter_table:
            arbiter_rows = arbiter_table.find_all('tr')
            for row in arbiter_rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    field = cells[0].text.strip()
                    value = cells[1].text.strip()
                    data[field] = value

    if "Сведения о заключении договора" in title:
        lot_numbers = []
        descriptions = []
        agreements = []
        contract_numbers = []
        contract_dates = []
        prices = []

        lot_section = soup.find('div', string="Публикуемые сведения")
        if lot_section:
            lot_table = lot_section.find_next("table")
            if lot_table:
                lot_rows = lot_table.find_all('tr')
                for row in lot_rows:
                    cells = row.find_all('td')
                    if len(cells) >= 7:
                        lot_numbers.append(f'{cells[1].text.strip()}')
                        descriptions.append(f'{cells[2].text.strip()}')
                        agreements.append(f'{cells[3].text.strip()}')
                        contract_numbers.append(f'{cells[4].text.strip()}')
                        contract_dates.append(f'{cells[5].text.strip()}')
                        prices.append(f'{cells[6].text.strip()}')

                data.update({
                    'Номер лота': "&&& ".join(lot_numbers),
                    'Описание': "&&& ".join(descriptions),
                    'Сведения о заключении договора': "&&& ".join(agreements),
                    'Номер договора': "&&& ".join(contract_numbers),
                    'Дата заключения договора': "&&& ".join(contract_dates),
                    'Цена приобретения имущества, руб.': "&&& ".join(prices),
                })


    elif "Сообщение о результатах торгов" in title:
        lot_number = []
        description = []
        winner = []
        best_price = []
        classification = []

        lot_tablet = soup.find('table', class_='lotInfo')
        if lot_tablet:
            lot_rows = lot_tablet.find_all('tr')[1:]
            for row in lot_rows:
                cells = row.find_all('td')
                if len(cells) >= 5:
                    # Извлекаем данные из ячеек таблицы
                    lot_number.append(f'{cells[0].text.strip()}')
                    description.append(f'{cells[1].text.strip()}')
                    winner.append(f'{cells[2].text.strip()}')
                    best_price.append(f'{ cells[3].text.strip()}')
                    classification.append(f'{cells[4].text.strip()}')

            data.update({
                'Номер лота': "&&& ".join(lot_number),
                'Описание': "&&& ".join(description),
                'Наименование покупателя': "&&& ".join(winner),
                'Лучшая цена': "&&& ".join(best_price),
                'Классификация': "&&& ".join(classification),
            })


    elif "Объявление о проведении торгов" in title:
        lot_section = soup.find('div', string="Публикуемые сведения")
        if lot_section:
            lot_table = lot_section.find_next("table")
            if lot_table:
                lot_rows = lot_table.find_all('tr')
                for row in lot_rows:
                    cells = row.find_all('td')
                    if len(cells) == 2:
                        field = cells[0].text.strip()
                        value = cells[1].text.strip()
                        data[field] = value

        lot_numbers = []
        lot_descriptions = []
        lot_prices = []
        lot_classification = []

        lot_tablet = soup.find('table', class_='lotInfo')
        if lot_tablet:
            lot_rows = lot_tablet.find_all('tr')[1:]
            for row in lot_rows:
                cells = row.find_all('td')

                try:
                    lot_numbers.append(cells[0].text.strip())  # Номер лота
                except IndexError:
                    lot_numbers.append("")
                try:
                    lot_descriptions.append(cells[1].text.strip())  # Описание
                except IndexError:
                    lot_descriptions.append("")
                try:
                    lot_prices.append(cells[2].text.strip())  # Начальная цена, руб
                except IndexError:
                    lot_prices.append("")
                try:
                    lot_classification.append(cells[6].text.strip())  # Классификация имущества
                except IndexError:
                    lot_classification.append("")

            data.update({
                'Номер лота': "&&& ".join(lot_numbers),
                'Описание': "&&& ".join(lot_descriptions),
                'Начальная цена, руб': "&&& ".join(lot_prices),
                'Классификация': "&&& ".join(lot_classification),
            })

        agreements = []  # Для хранения сведений о заключении договоров
        auction_results = []

        addition_info = soup.find('div', class_='containerInfo')
        if addition_info:
            spans = addition_info.find_all('span')
            for span in spans:
                span_text =span.text.strip()
                if "Сведения о заключении договора купли-продажи" in span_text:
                    agreements.append(span_text)
                elif "Сообщение о результатах торгов" in span_text:
                    auction_results.append(span_text)

            data.update({
                'Сведения о заключении договора купли-продажи': "&&& ".join(agreements),
                'Сообщение о результатах торгов': "&&& ".join(auction_results),
            })

    elif "Отчет оценщика об оценке имущества должника" in title:
        types = []
        descriptions = []
        dates = []
        estimated_prices = []
        balance_values = []

        lot_section = soup.find('div', string="Сведения об объектах оценки")
        if lot_section:
            lot_table = lot_section.find_next("table")
            if lot_table:
                lot_rows = lot_table.find_all('tr')[1:]
                for row in lot_rows:
                    cells = row.find_all('td')
                    if len(cells) >= 5:
                        types.append(cells[0].text.strip())
                        descriptions.append(cells[1].text.strip())
                        dates.append(cells[2].text.strip())
                        estimated_prices.append(cells[3].text.strip())
                        balance_values.append(cells[4].text.strip()) # Классификация имущества

                data.update({
                    'Тип': "&&& ".join(types),
                    'Описание': "&&& ".join(descriptions),
                    'Дата определения стоимости': "&&& ".join(dates),
                    'Стоимость, определенная оценщиком': "&&& ".join(estimated_prices),
                    'Балансовая стоимость': "&&& ".join(balance_values)
                })
    return data


# Открытие страницы с сообщениями
url = "https://old.bankrot.fedresurs.ru/Messages.aspx"  # Замените на URL вашей страницы
driver.get(url)
time.sleep(3)
WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.CLASS_NAME, 'bank'))
)

# Получение HTML-кода страницы
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')
table = soup.find('table', class_='bank')
rows = table.find_all('tr')

# Получаем подключение к базе данных
connection = get_db_connection()

# Ограничение количества сообщений для парсинга
max_messages = 5
current_count = 0

for row in rows:
    # if current_count >= max_messages:
    #     break  #
    cells = row.find_all('td')
    if len(cells) >= 5:
        raw_data = {
            'date': cells[0].text.strip(),
            'message_type': cells[1].text.strip(),
            'debtor_name': cells[2].text.strip(),
            'address': cells[3].text.strip(),
            'arbiter_name': cells[4].text.strip()
        }

        message_type = cells[1].text.strip()
        print(message_type)
        if any(keyword in message_type for keyword in ['Сведения о заключении договора купли-продажи',
                                               'Сообщение о результатах торгов',
                                               'Объявление о проведении торгов',
                                               'Отчет оценщика об оценке имущества должника']):

            # Извлечение ссылки на сообщение
            message_link_tag = row.find('a', href=True)
            if message_link_tag:
                message_link = "https://old.bankrot.fedresurs.ru" + message_link_tag['href']
                print("Ссылка на сообщение:", message_link)

                try:
                    message_content  = parse_message_page(message_link, driver)
                    raw_data['message_content'] = message_content
                    raw_data['message_link'] = message_link
                    # Подготовка данных перед вставкой в базу
                    prepared_data = prepare_data_for_db(raw_data)
                    print(raw_data)
                    # Вставка данных в базу
                    new_id = insert_message_to_db(prepared_data, connection)
                    print(f"Данные успешно вставлены с ID: {new_id}")

                    # current_count += 1
                except Exception as e:
                    print("Ошибка при парсинге страницы или вставке данных:", e)
            else:
                print("Ссылка не найдена в текущей строке.")
        else:
            print("нето")

# Закрытие браузера после завершения работы
driver.quit()
connection.close()