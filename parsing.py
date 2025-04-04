from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import time
from selenium.webdriver.support import expected_conditions as EC
from logScript import logger
from webdriver import restart_driver


# Функция для получения и парсинга HTML-кода по предоставленной ссылке
def parse_message_page(url, driver):
    try:
        logger.info(f'Переход по ссылке: {url}')
        try:
            driver.get(url)
        except Exception as e:
            logger.error(f'не получилось открыть ссылку в parse_message_page: {e}')
            driver = restart_driver(driver)
            driver.get(url)
            logger.info(f'Повторная попытка открытия ссылки')

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

        if "Сообщение об отмене сообщения об объявлении торгов или сообщения о результатах торгов" in title:
            # Поиск секции "Публикуемые сведения"
            cancel_section = soup.find('div', string="Публикуемые сведения")
            if cancel_section:
                cancel_table = cancel_section.find_next("table")
                if cancel_table:
                    rows = cancel_table.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) == 2 and cells[0].text.strip() == "Отмененное сообщение":
                            data["Объявление о проведении торгов"] = cells[1].text.strip()
                            break  # Достаточно первой найденной записи
            else:
                logger.warning("Секция 'Публикуемые сведения' не найдена.")

        elif "Сведения о заключении договора" in title:
            lot_numbers = []
            descriptions = []
            agreements = []
            contract_numbers = []
            contract_dates = []
            prices = []
            winner = []

            lot_platform = soup.find('div', string='Публикуемые сведения')
            if lot_platform:
                lot_table = lot_platform.find_next("table")
                if lot_table:
                    lot_rows = lot_table.find_all('tr')
                    for row in lot_rows:
                        cells = row.find_all('td')
                        if len(cells) == 2:
                            field = cells[0].text.strip()
                            value = cells[1].text.strip()
                            data[field] = value

            lot_section = soup.find('div', string=lambda x: x and 'Заключенные договоры' in x)
            if lot_section:
                lot_table = lot_section.find_next("table")
                if lot_table:
                    lot_rows = lot_table.find_all('tr')

                    current_lot = {
                        "Номер лота": '',
                        "Описание": ' ',
                        "Сведения о заключении договора": '',
                        "Номер договора": '',
                        "Дата заключения договора": '',
                        "Цена приобретения имущества, руб.": '',
                        "Наименование покупателя": ''
                    }

                    for row in lot_rows:
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            field = cells[0].text.strip()
                            value = cells[1].text.strip()

                            if field in current_lot:
                                current_lot[field] = value if value else ' '

                        # Проверяем, что строка закончилась, и начинаем новый лот
                        if "Наименование покупателя" in current_lot and current_lot["Наименование покупателя"]:
                            # Добавляем данные текущего лота в итоговые списки
                            lot_numbers.append(current_lot["Номер лота"])
                            descriptions.append(current_lot["Описание"])
                            agreements.append(current_lot["Сведения о заключении договора"])
                            contract_numbers.append(current_lot["Номер договора"])
                            contract_dates.append(current_lot["Дата заключения договора"])
                            prices.append(current_lot["Цена приобретения имущества, руб."])
                            winner.append(current_lot["Наименование покупателя"])

                            # Обнуляем временный словарь для следующего лота
                            current_lot = {
                                "Номер лота": '',
                                "Описание": '',
                                "Сведения о заключении договора": '',
                                "Номер договора": '',
                                "Дата заключения договора": '',
                                "Цена приобретения имущества, руб.": '',
                                "Наименование покупателя": ''
                            }

                    # Проверяем, не остались ли данные последнего лота
                    if any(value != '' for value in current_lot.values()):
                        lot_numbers.append(current_lot["Номер лота"] if current_lot["Номер лота"].strip() else ' ')
                        descriptions.append(current_lot["Описание"] if current_lot["Описание"].strip() else ' ')
                        agreements.append(current_lot["Сведения о заключении договора"] if current_lot[
                            "Сведения о заключении договора"].strip() else ' ')
                        contract_numbers.append(
                            current_lot["Номер договора"] if current_lot["Номер договора"].strip() else ' ')
                        contract_dates.append(current_lot["Дата заключения договора"] if current_lot[
                            "Дата заключения договора"].strip() else ' ')
                        prices.append(current_lot["Цена приобретения имущества, руб."] if current_lot[
                            "Цена приобретения имущества, руб."].strip() else ' ')
                        winner.append(current_lot["Наименование покупателя"] if current_lot[
                            "Наименование покупателя"].strip() else ' ')

                    data.update({
                        'Номер лота': "&&& ".join(lot_numbers),
                        'Описание': "&&& ".join(descriptions),
                        'Сведения о заключении договора': "&&& ".join(agreements),
                        'Номер договора': "&&& ".join(contract_numbers),
                        'Дата заключения договора': "&&& ".join(contract_dates),
                        'Цена': "&&& ".join(prices),
                        'Наименование покупателя': " ".join(winner)
                    })
                else:
                    logger.info("Таблица не найдена.")
            else:
                logger.debug("Секция лотов не спарсина")

            pre_text = soup.find_all('div', class_='msg')[-1]
            data['текст'] = pre_text.text.strip() if pre_text else ""

        elif "Сообщение о результатах торгов" in title:
            text_section = soup.find_all('div', class_='msg')
            data['текст'] = "; ".join(text.text.strip() for text in text_section if text.text.strip())

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
                    'Цена': "&&& ".join(best_price),
                    'Классификация': "&&& ".join(classification),
                })


        elif "Объявление о проведении торгов" in title or "Сообщение об изменении" in title:
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

            text_section = soup.find_all('div', class_='msg')
            data['текст'] = "; ".join(text.text.strip() for text in text_section if text.text.strip())

            lot_table = soup.find('table', class_='lotInfo')
            if lot_table:
                header_row = lot_table.find_all('tr')[0]
                headers = [th.text.strip() for th in header_row.find_all('th')]
                lot_rows = lot_table.find_all('tr')[1:]

                collected_values = {
                    'Номер лота': [],
                    'Описание': [],
                    'Начальная цена, руб': [],
                    'Классификация имущества': []
                }

                for row in lot_rows:
                    cells = row.find_all('td')
                    for header in collected_values.keys():
                        if header in headers:
                            index = headers.index(header)
                            if index < len(cells):
                                value = cells[index].text.strip()
                            else:
                                value = ""
                            collected_values[header].append(value)

                data.update({
                    'Номер лота': "&&& ".join(collected_values['Номер лота']),
                    'Описание': "&&& ".join(collected_values['Описание']),
                    'Цена': "&&& ".join(collected_values['Начальная цена, руб']),
                    'Классификация': "&&& ".join(collected_values['Классификация имущества'])
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

        elif "Отчет оценщика об оценке" in title:
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
                        'Классификация': "&&& ".join(types),
                        'Описание': "&&& ".join(descriptions),
                        'Дата определения стоимости': "&&& ".join(dates),
                        'Цена': "&&& ".join(estimated_prices),
                        'Балансовая стоимость': "&&& ".join(balance_values)
                    })

            text_section = soup.find_all('div', class_='msg')
            data['текст'] = "; ".join(text.text.strip() for text in text_section if text.text.strip())
        return data

    except Exception as e:
        logger.error(f'Ошибка при обработке URL {url}: {e}')
        return None