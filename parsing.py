from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import time
from selenium.webdriver.support import expected_conditions as EC

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
