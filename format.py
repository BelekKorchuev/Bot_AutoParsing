import datetime
import re
from Main import logger
from tabulate import tabulate

def filter_results_before_transfer(data):
    """
    Фильтрует строки на основе условий:
    Если в "тип_сообщение" есть "резул" и в "цена" или "наименование_покупателя"
    есть "не сост", "несост" или "не подан", то строка удаляется.
    """
    logger.debug("Фильтрация строк с условиями по 'тип_сообщение', 'цена', 'наименование_покупателя'")
    result = []

    for row in data:
        # Проверяем наличие "резул" в тип_сообщение
        typ_soobsh = row.get("тип_сообщение", "").lower()
        if "резул" in typ_soobsh:
            # Проверяем условия в "цена" и "наименование_покупателя"
            price = row.get("цена", "").lower()
            buyer = row.get("наименование_покупателя", "").lower()

            # Используем регулярное выражение для проверки слов
            if re.search(r"(не сост|несост|не подан)", price) or \
               re.search(r"(не сост|несост|не подан)", buyer):
                logger.debug(f"Удалено: {row}")
                continue  # Пропускаем эту строку
        # Если условия не выполнены, добавляем строку в результат
        result.append(row)

    logger.debug(f"Фильтрация завершена. Оставлено строк: {len(result)} из {len(data)}")
    return result

def convert_to_date_only(column):
    """
    Преобразует значения столбца в формат дд.мм.гггг.
    :param column: список, содержащий даты.
    :return: список, где каждая дата в формате дд.мм.гггг.
    """
    logger.debug("Преобразование дат в формат дд.мм.гггг")
    return [
        x.strftime('%d.%m.%Y') if isinstance(x, datetime.datetime) else
        datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%d.%m.%Y') if isinstance(x, str) else
        None
        for x in column
    ]


def price_text(priceN):
    logger.debug(f"Обработка строки цены: {priceN}")
    if priceN:
        match = re.search(r'\b\d{1,3}(?:\s?\d{3})*(?:,\d{2})?\b', priceN)
        result = match.group(0).replace(' ', '') if match else None
        logger.debug(f"Найденная цена: {result}")
        return result
    return None


def rename_type_message(message_type):
    logger.debug(f"Переименование типа сообщения: {message_type}")
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


def filter_lots_by_property_type(lot):
    logger.debug("Начало фильтрации лота: %s", lot)

    # Берем значения из словаря
    type_torg = lot.get("тип_сообщения", "").strip().lower()
    klass = lot.get("классификация", "").strip()
    imush = lot.get("описание", "").strip()
    dataDKP = lot.get("сведения_о_заключении_договора", "").strip() if lot.get("сведения_о_заключении_договора") else ""

    logger.debug("Проверка поля 'тип_сообщения': %s", type_torg)
    logger.debug("Проверка поля 'классификация': %s", klass)
    logger.debug("Проверка поля 'описание': %s", imush)
    logger.debug("Проверка поля 'сведения_о_заключении_договора': %s", dataDKP)

    # Проверяем условие на "оценк" или "объяв"
    if "оценк" in type_torg or "объяв" in type_torg:
        logger.debug("Тип сообщения содержит 'оценк' или 'объяв', проверяем классификацию и описание.")

        # Регулярные выражения для нежелательных фраз
        patterns = [
            r"дебитор",  # Любая форма слова "дебитор"
            r"прав(?:о|а|ам|ах|у)? ?\(?(?:аренд|треб)\)?",  # Формы "право аренды" или "право требования"
        ]

        # Проверяем классификацию и описание на соответствие нежелательным фразам
        for pattern in patterns:
            if re.search(pattern, klass, re.IGNORECASE):
                logger.debug("Классификация содержит нежелательное слово: %s", pattern)
                return None
            if re.search(pattern, imush, re.IGNORECASE):
                logger.debug("Описание содержит нежелательное слово: %s", pattern)
                return None

    # Проверяем условие на "догово" и пустое поле "сведения_о_заключении_договора"
    if "догово" in type_torg:
        logger.debug("Тип сообщения содержит 'догово'. Проверяем 'сведения_о_заключении_договора'.")
        if not dataDKP:
            logger.debug("Пустое поле 'сведения_о_заключении_договора'. Лот отклонен.")
            return None

    # Если запись прошла все проверки, возвращаем её
    logger.debug("Лот прошёл все проверки: %s", lot)
    return lot


def delete_org(text):
    logger.debug(f"Удаление ссылок на PrsTOCard/OrgToCard: {text}")
    if isinstance(text, str) and ("PrsTOCard" in text or "OrgToCard" in text):
        return None
    return text


def extract_number(text):
    logger.debug(f"Извлечение номера из текста: {text}")
    match = re.search(r'№(\d+)', text)
    return match.group(1) if match else None


def extract_date(text):
    logger.debug(f"Извлечение даты из текста: {text}")
    match = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
    return match.group(1) if match else None


def clean_special_chars(text):
    logger.debug(f"Очистка строки от специальных символов: {text}")
    if isinstance(text, str):
        text = text.replace('\xa0', ' ').replace('\t', ' ')
        text = re.sub(r'[^\x20-\x7Eа-яА-ЯёЁ]', ' ', text).strip()
        return re.sub(' +', ' ', text)
    return text


def remove_rows_with_cancelled_messages(data):
    logger.debug("Удаление строк с аннулированными сообщениями")
    pattern = r'аннулир|отмен'
    result = [row for row in data if not re.search(pattern, row.get("тип_сообщения", ""), re.IGNORECASE)]
    logger.debug(f"Удалено {len(data) - len(result)} строк с аннулированными сообщениями")
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
    logger.debug("Перенос и упорядочивание данных")
    formatted_data = []

    for row in raw_data:
        formatted_row = {}
        for col in lots_columns:
            source_col = [key for key, value in mappings.items() if value == col]
            if source_col:
                # Обработка для вид_торгов
                if col == 'вид_торгов':
                    if "объяв" in row.get("тип_сообщения", "").lower().strip():
                        formatted_row[col] = row.get("вид_торгов", None)  # Берём из исходного 'вид_торгов'
                    else:
                        formatted_row[col] = row.get("тип_сообщения", None)  # По умолчанию берём 'тип_сообщения'
                else:
                    formatted_row[col] = row.get(source_col[0], None)
            else:
                formatted_row[col] = None
        formatted_data.append(formatted_row)

    logger.debug(f"Обработано строк: {len(formatted_data)}")
    return formatted_data

def process_data(data):
    logger.info("Начало обработки данных")

    # Удаление строк с аннулированными сообщениями
    data = remove_rows_with_cancelled_messages(data)

    # Отфильтровываем лоты с использованием filter_lots_by_property_type
    filtered_data = []
    for row in data:
        filtered_lot = filter_lots_by_property_type(row)
        if filtered_lot == None:
            logger.debug(f"Пропущено: {row}")
            continue  # Пропускаем этот лот, если функция вернула "Пропуск"
        filtered_data.append(filtered_lot)

    # Перенос и упорядочивание данных после фильтрации
    formatted_data = transfer_and_order_data(filtered_data, lots_columns, mappings)

    # Преобразование и обработка данных после переноса
    processed_data = []
    for row in formatted_data:
        row['Дата_начала_торгов'] = clean_special_chars(row.get('Дата_начала_торгов', ""))
        row['Дата_окончания'] = clean_special_chars(row.get('Дата_окончания', ""))
        row['вид_торгов'] = rename_type_message(row.get('вид_торгов', ""))
        row['Цена'] = price_text(row.get('Цена', ""))
        row['Дата_публикации'] = convert_to_date_only([row.get('Дата_публикации')])[0]
        row['Дата_публикации_предыдущего_сообщ'] = extract_date(row.get('Предыдущий_номер_сообщения_по_лот', ""))
        row['Предыдущий_номер_сообщения_по_лот'] = extract_number(row.get('Предыдущий_номер_сообщения_по_лот', ""))
        row['Дата_публикации_сообщения_ДКП'] = extract_date(row.get('Статус_ДКП', ""))
        row['Статус_ДКП'] = extract_number(row.get('Статус_ДКП', ""))
        row['Дата_публикации_сообщения_о_резул'] = extract_date(row.get('Статус_сообщения_о_результатах_то', ""))
        row['Статус_сообщения_о_результатах_то'] = extract_number(row.get('Статус_сообщения_о_результатах_то', ""))
        processed_data.append(row)

    logger.info(f"Обработка завершена. Отфильтровано лотов: {len(processed_data)}")
    print(tabulate(processed_data, headers="keys", tablefmt="grid"))
    return processed_data


def get_massageLots(lots):
    data = lots.copy()  # Создание копии данных, чтобы не изменять исходный список
    cleaned_data = []  # Новый список для отфильтрованных данных

    for record in data:
        if record.get('арбитр_ссылка'):
            if delete_org(record['арбитр_ссылка']) is None:
                # Если delete_org вернул None, значит ссылка содержит PrsTOCard или OrgToCard
                logger.debug(f"Удаление записи с арбитром, ссылка на которого содержит PrsTOCard/OrgToCard: {record}")
                continue  # Пропускаем добавление этой записи в cleaned_data
        cleaned_data.append(record)  # Добавляем запись в результирующий список, если условие не сработало

    # Фильтрация и обработка данных
    cleaned_data = filter_results_before_transfer(cleaned_data)
    cleaned_data = process_data(cleaned_data)

    return cleaned_data
