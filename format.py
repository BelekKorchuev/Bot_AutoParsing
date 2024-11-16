from operator import index

import pandas as pd
import os
import datetime
import re


def convert_to_date_only(column):
    """
    Преобразует значения столбца в формат дд.мм.гггг.

    :param column: pandas.Series, содержащий даты
    :return: pandas.Series, где каждая дата в формате дд.мм.гггг
    """
    return column.apply(lambda x: pd.to_datetime(x).strftime('%d.%m.%Y') if pd.notna(x) else None)


def price_text(priceN):
    if pd.notna(priceN):
        # Извлекаем первую группу чисел с возможной дробной частью (например, "178 529,75")
        match = re.search(r'\b\d{1,3}(?:\s?\d{3})*(?:,\d{2})?\b', priceN)
        return match.group(0).replace(' ', '') if match else None
    return None  # Если значение NaN, возвращаем None


def rename_typeMessage(message_type):
    if isinstance(message_type, str):
        if "заключении" in message_type.lower():
            return "ДКП"
        elif "результ" in message_type.lower():
            return "Результаты торгов"
        elif "оцен" in message_type.lower():
            return "Оценка"
        elif "публич" in message_type.lower():
            return "Публичка"
        elif "аукц|конкур" in message_type.lower():
            return "Аукцион"
    return message_type  # Если не подходит ни один критерий, вернуть исходное значение

def filter_lots_by_property_type(format_lots_df):
    if format_lots_df['вид_торгов'].str.contains("оцен|объяв", case=False, na=False).any():
        # Удаляем строки, где "Классификация имущества" содержит "дебиторск"
        format_lots_df = format_lots_df[~format_lots_df["Классификация имущества"].str.contains(r"дебиторск", case=False, na=False)]

        # Удаляем строки, где "Имущество" содержит "право аренды" или "право требования", включая их варианты в скобках
        format_lots_df = format_lots_df[
            ~format_lots_df["Имущество"].str.contains(
                r"\bправ(о|а|ам|ах|у) \(?арен|треб"
,  # Условие: слово "право" с возможным текстом в скобках перед "арен" или "треб"
                case=False, na=False
            )
        ]

    return format_lots_df

#Удаление организаторов
def delete_org(text):
    text = str(text) if not isinstance(text, str) else text
    if "PrsTOCard" in text or "OrgToCard" in text:
        return None
    return text

# Функция для извлечения ИНН
def extract_inn(text):
    # Преобразуем в строку, если это не строка
    text = str(text) if not isinstance(text, str) else text
    match = re.search(r'ИНН\s*(\d+)', text)
    return match.group(1) if match else None

#Вытащить номер сообщения
def extract_number(text):
    text = str(text) if not isinstance(text, str) else text
    number_match = re.search(r'№(\d+)', text)
    number = number_match.group(1) if number_match else ""
    return number

#Извлечь даты публикации из номера сообщения
def extract_date(text):
    text = str(text) if not isinstance(text, str) else text
    date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', text)
    date = date_match.group(1) if date_match else ""
    return date

#В дата начала и конца подачи заявок удаляет невидимые символы
def clean_special_chars(text):
    if isinstance(text, str):
        text = text.replace('\xa0', ' ').replace('\t', ' ')  # Удаляем неразрывные пробелы и табуляции
        text = re.sub(r'[^\x20-\x7Eа-яА-ЯёЁ]', ' ', text).strip()  # Удаляем непечатные символы
        text = re.sub(' +', ' ', text)  # Убираем лишние пробелы
        return text
    return text

#Удаляет аннулированные и отменные сообщения
def remove_rows_with_cancelled_messages(assessment_df):
    pattern = r'аннулир|отмен'
    if "тип_сообщения" in assessment_df.columns:
        assessment_df = assessment_df[~assessment_df["тип_сообщения"].str.contains(pattern, na=False)]
    return assessment_df



#Итоговые столбцы
lots_columns = [
    "Дата_загрузки_лота/обновления", "ЕФРСБ/ББ", "Должник_текст",
    "ИНН_Должника", "вид_торгов", "Дата_публикации",
    "Дата_начала_торгов", "Дата_окончания", "Номер_дела",
    "Действующий_номер_сообщения", "Номер_лота",
    "Ссылка_на_сообщение_ЕФРСБ", "Имущество",
    "Классификация_имущества", "Цена", "Предыдущий_номер_сообщения_по_лот",
    "Дата_публикации_предыдущего_сообщ", "Организатор_торгов",
    "Торговая_площадка", "Комментарий", "Статус_ДКП",
    "Дата_публикации_сообщения_ДКП", "Статус_сообщения_о_результатах_то",
    "Дата_публикации_сообщения_о_резул", "Cведения о заключении договора"
]
lots_df = pd.DataFrame(columns=lots_columns)

#Маппинг для переноса данных
mappings = {
    "дата_публикации": "Дата_публикации",
    "наименование_должника": "Должник_текст",
    "должник_ссылка": "ЕФРСБ/ББ",
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
    "торгова_площадка": "Торговая_площадка",
    "сведения_о_заключении_договора": "Cведения о заключении договора",
    "объявление_о_проведении_торгов": "Предыдущий_номер_сообщения_по_лот",
    "ДКП": "Статус_ДКП",
    "результат": "Статус_сообщения_о_результатах_то"
}

#Функция для переноса столбцов
def transfer_data(assessment_df, format_lots_df):
    #Перенос столбцов
    for source_col, target_col in mappings.items():
        if source_col in assessment_df.columns:
            format_lots_df[target_col] = assessment_df[source_col]
        else:
            format_lots_df[target_col] = pd.NA
    #Замена объявлений на публичку и аукцион
    if "тип_сообщения" in assessment_df.columns and "вид_торгов" in assessment_df.columns:
        if assessment_df['тип_сообщения'].str.contains("объявл", case=False, na=False).any():
            format_lots_df["вид_торгов"] = assessment_df["вид_торгов"]

    return format_lots_df

#Запускает функцию переноса данных и проводи чистку базы
def process_data(assessment_df):
    format_lots_df = lots_df.copy()

    format_lots_df = transfer_data(assessment_df, format_lots_df)


    format_lots_df['Дата_начала_торгов'] = format_lots_df['Дата_начала_торгов'].apply(clean_special_chars)
    format_lots_df['Дата_окончания'] = format_lots_df['Дата_окончания'].apply(clean_special_chars)

    format_lots_df["Дата_публикации_предыдущего_сообщ"] = format_lots_df["Предыдущий_номер_сообщения_по_лот"].apply(extract_date)
    format_lots_df["Предыдущий_номер_сообщения_по_лот"] = format_lots_df["Предыдущий_номер_сообщения_по_лот"].apply(extract_number)

    format_lots_df["Дата_публикации_сообщения_ДКП"] = format_lots_df["Статус_ДКП"].apply(extract_date)
    format_lots_df["Статус_ДКП"] = format_lots_df["Статус_ДКП"].apply(extract_number)

    format_lots_df["Дата_публикации_сообщения_о_резул"] = format_lots_df["Статус_сообщения_о_результатах_то"].apply(extract_date)
    format_lots_df["Статус_сообщения_о_результатах_то"] = format_lots_df["Статус_сообщения_о_результатах_то"].apply(extract_number)

    # Нумерация лотов в оценках
    if format_lots_df['вид_торгов'].str.contains("оцен", case=False, na=False).any():
        # Сортировка по "Ссылка на сообщение ЕФРСБ" (А-Я)
        format_lots_df.sort_values(by="Ссылка_на_сообщение_ЕФРСБ", ascending=True, inplace=True)
        # Пронумеруем "Номер лота" по одинаковым ссылкам
        format_lots_df['Номер_лота'] = format_lots_df.groupby('Ссылка_на_сообщение_ЕФРСБ').cumcount() + 1

    # Вызываем функцию фильтрации по типу имущества для выбранных типов сообщений
    format_lots_df = filter_lots_by_property_type(format_lots_df)

    if format_lots_df['вид_торгов'].str.contains("заключ", case=False, na=False).any():
        format_lots_df = format_lots_df[format_lots_df['Cведения о заключении договора'].notna() & (format_lots_df['Cведения о заключении договора'] != '')]

    # Очищаем все ячейки от квадратных скобок и одинарных кавычек по всем столбцам
    for col in format_lots_df.columns:
        format_lots_df[col] = format_lots_df[col].map(
            lambda x: str(x).replace('[', '').replace(']', '').replace("'", '') if isinstance(x, str) else x)

    # Удаление строк в 'Лоты', где 'ИНН Должника' содержит 'не'
    format_lots_df = format_lots_df[~format_lots_df["ИНН_Должника"].str.contains("не", na=False, case=False)]

    # Переименования типов торгов на короткие названия
    format_lots_df['вид_торгов'] = format_lots_df['вид_торгов'].apply(rename_typeMessage)
    # Удаление лишнего текста из цены
    format_lots_df['Цена'] = format_lots_df['Цена'].apply(price_text)

    format_lots_df.drop(columns=["Cведения о заключении договора"], inplace=True)
    # Применение функции к столбцу 'Дата публикации'
    format_lots_df['Дата_публикации'] = convert_to_date_only(format_lots_df['Дата_публикации'])

    return format_lots_df

def get_massageLots(massageLots):
    assessment_df = pd.DataFrame(massageLots)
    assessment_df = remove_rows_with_cancelled_messages(assessment_df)

    # Удаление не состоявшихся торгов в результатах
    if "тип_сообщения" in assessment_df.columns and assessment_df['тип_сообщения'].str.contains("результ", case=False,
                                                                                                na=False).any():
        if "Победитель/Покупатель" in assessment_df.columns:
            assessment_df = assessment_df[
                ~assessment_df["Победитель/Покупатель"].str.contains(r'несосто|не состо|за собой', na=False,
                                                                     case=False)]
        if "Лучшая цена, руб. / Обоснование" in assessment_df.columns:
            assessment_df = assessment_df[
                ~assessment_df["Лучшая цена, руб. / Обоснование"].str.contains(r'несосто|не состо|за собой', na=False,
                                                                               case=False)]

    # Удаление организаторов торгов
    assessment_df['ИНН АУ'] = assessment_df['ФИО_АУ'].apply(extract_inn)
    assessment_df["арбитр_ссылка"] = assessment_df["арбитр_ссылка"].apply(delete_org)
    assessment_df.dropna(subset=["арбитр_ссылка"], inplace=True)
    assessment_df = assessment_df[assessment_df["ИНН АУ"].notna() & (assessment_df["ИНН АУ"] != '')]

    data = process_data(assessment_df)

    return data




