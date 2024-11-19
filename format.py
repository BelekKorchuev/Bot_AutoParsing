import datetime
import re
import logging
from tabulate import tabulate

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s',
    handlers=[logging.StreamHandler()]
)



def convert_to_date_only(column):
    """
    Преобразует значения столбца в формат дд.мм.гггг.
    :param column: список, содержащий даты.
    :return: список, где каждая дата в формате дд.мм.гггг.
    """
    logging.debug("Преобразование дат в формат дд.мм.гггг")
    return [
        x.strftime('%d.%m.%Y') if isinstance(x, datetime.datetime) else
        datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%d.%m.%Y') if isinstance(x, str) else
        None
        for x in column
    ]


def price_text(priceN):
    logging.debug(f"Обработка строки цены: {priceN}")
    if priceN:
        match = re.search(r'\b\d{1,3}(?:\s?\d{3})*(?:,\d{2})?\b', priceN)
        result = match.group(0).replace(' ', '') if match else None
        logging.debug(f"Найденная цена: {result}")
        return result
    return None


def rename_type_message(message_type):
    logging.debug(f"Переименование типа сообщения: {message_type}")
    if isinstance(message_type, str):
        if "заключении" in message_type.lower():
            return "ДКП"
        elif "результ" in message_type.lower():
            return "Результаты торгов"
        elif "оцен" in message_type.lower():
            return "Оценка"
        elif "публич" in message_type.lower():
            return "Публичка"
        elif re.search(r"аукц|конкур", message_type.lower()):
            return "Аукцион"
    return message_type


def filter_lots_by_property_type(data):
    logging.debug("Фильтрация лотов по типу имущества")
    result = []

    for lot in data:
        # Обрабатываем 'вид_торгов', используя значение по умолчанию ''
        vid_torgov = lot.get('вид_торгов', '')
        if isinstance(vid_torgov, str):  # Проверяем, что это строка
            vid_torgov = vid_torgov.lower()
        else:
            vid_torgov = ''

        klass = lot.get("классификация", "")
        imush = lot.get("описание", "")

        # Удаляем записи, если они содержат нежелательные ключевые слова
        if re.search(r'дебиторск', klass, re.IGNORECASE) or \
           re.search(r"\bправ(о|а|ам|ах|у) \(?арен|треб", imush, re.IGNORECASE):
            # Удаляем запись только если вид_торгов не "оцен" или "объяв"
            if "оцен" not in vid_torgov and "объяв" not in vid_torgov:
                logging.debug(f"Удалено: {lot}")
                continue

        # Если запись прошла все проверки, сохраняем её
        result.append(lot)

    logging.debug(f"Отфильтрованные лоты: {len(result)} из {len(data)}")
    return result



def delete_org(text):
    logging.debug(f"Удаление ссылок на PrsTOCard/OrgToCard: {text}")
    if "PrsTOCard" in text or "OrgToCard" in text:
        return None
    return text


def extract_inn(text):
    logging.debug(f"Извлечение ИНН из текста: {text}")
    match = re.search(r'ИНН\s*(\d+)', text)
    return match.group(1) if match else None


def extract_number(text):
    logging.debug(f"Извлечение номера из текста: {text}")
    match = re.search(r'№(\d+)', text)
    return match.group(1) if match else None


def extract_date(text):
    logging.debug(f"Извлечение даты из текста: {text}")
    match = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
    return match.group(1) if match else None


def clean_special_chars(text):
    logging.debug(f"Очистка строки от специальных символов: {text}")
    if isinstance(text, str):
        text = text.replace('\xa0', ' ').replace('\t', ' ')
        text = re.sub(r'[^\x20-\x7Eа-яА-ЯёЁ]', ' ', text).strip()
        return re.sub(' +', ' ', text)
    return text


def remove_rows_with_cancelled_messages(data):
    logging.debug("Удаление строк с аннулированными сообщениями")
    pattern = r'аннулир|отмен'
    result = [row for row in data if not re.search(pattern, row.get("тип_сообщения", ""), re.IGNORECASE)]
    logging.debug(f"Удалено {len(data) - len(result)} строк с аннулированными сообщениями")
    return result

lots_columns = [
    "ИНН_Должника", "Дата_публикации", "Дата_начала_торгов",  "Дата_окончания",
    "Номер_дела", "Действующий_номер_сообщения", "Номер_лота",
    "Ссылка_на_сообщение_ЕФРСБ", "Имущество",
    "Классификация_имущества", "Цена", "Предыдущий_номер_сообщения_по_лот",
    "Дата_публикации_предыдущего_сообщ", "Организатор_торгов",
    "Торговая_площадка", "Статус_ДКП", "Статус_сообщения_о_результатах_то",
    "ЕФРСБ_ББ", "Должник_текст", "вид_торгов", "Дата_публикации_сообщения_ДКП",
    "Дата_публикации_сообщения_о_резул"
]

mappings = {
    "дата_публикации": "Дата_публикации",
    "наименование_должника": "Должник_текст",
    "ИНН": "ИНН_Должника",
    "тип_сообщения": "вид_торгов",
    "номер_дела": "Номер_дела",
    "номер_сообщения": "Действующий_номер_сообщения",
    "номер_лота": "Номер_лота",
    "сообщение_ссылка": "Ссылка_на_сообщение_ЕФРСБ",
    "дата_начала_подачи_заявок": "Дата_начала_торгов",
    "дата_окончания_подачи_заявок": "Дата_окончания",
    "описание": "Имущество",
    "классификация": "Классификация_имущества",
    "цена": "Цена",
    "торговая_площадка": "Торговая_площадка",
    "сведения_о_заключении_договора": "Cведения о заключении договора",
    "объявление_о_проведении_торгов": "Предыдущий_номер_сообщения_по_лот",
    "ДКП": "Статус_ДКП",
    "результат": "Статус_сообщения_о_результатах_то"
}


def transfer_and_order_data(raw_data, lots_columns, mappings):
    logging.debug("Перенос и упорядочивание данных")
    formatted_data = []

    for row in raw_data:
        formatted_row = {}
        for col in lots_columns:
            source_col = [key for key, value in mappings.items() if value == col]
            if source_col:
                # Обработка для вид_торгов
                if col == 'вид_торгов':
                    if "объяв" in row.get("тип_сообщения", "").lower():
                        formatted_row[col] = row.get("вид_торгов", None)  # Берём из исходного 'вид_торгов'
                    else:
                        formatted_row[col] = row.get("тип_сообщения", None)  # По умолчанию берём 'тип_сообщение'
                else:
                    formatted_row[col] = row.get(source_col[0], None)
            else:
                formatted_row[col] = None
        formatted_data.append(formatted_row)

    logging.debug(f"Обработано строк: {len(formatted_data)}")
    return formatted_data



def process_data(data):
    logging.info("Начало обработки данных")
    data = remove_rows_with_cancelled_messages(data)
    formatted_data = transfer_and_order_data(data, lots_columns, mappings)
    processed_data = []
    for row in formatted_data:
        row['Дата_начала_торгов'] = clean_special_chars(row.get('Дата_начала_торгов', ""))
        row['Дата_окончания'] = clean_special_chars(row.get('Дата_окончания', ""))
        # row['ИНН_Должника'] = extract_inn(row.get('Должник_текст', ""))
        row['вид_торгов'] = rename_type_message(row.get('вид_торгов', ""))
        row['Цена'] = price_text(row.get('Цена', ""))
        row['Дата_публикации'] = convert_to_date_only([row.get('Дата_публикации')])[0]
        processed_data.append(row)

    tableLots = filter_lots_by_property_type(processed_data)
    logging.info(f"Обработка завершена. Отфильтровано лотов: {len(tableLots)}")
    print(tabulate(tableLots, headers="keys", tablefmt="grid"))
    return tableLots



def get_massageLots(lots):
    data = process_data(lots)
    # data = delete_org(data['арбитр_ссылка'])
    #data = filter_results_before_transfer(data)
    return data

