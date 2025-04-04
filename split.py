from format import get_massageLots

def ensure_list(data):
    """
    Преобразует входные данные в список словарей, если передан один словарь.
    """
    if isinstance(data, dict):
        return [data]  # Преобразуем словарь в список с одним элементом
    elif isinstance(data, list):
        return data  # Если уже список, ничего не делаем
    else:
        raise ValueError(f"Ожидались данные в формате dict или list, получено: {type(data)}")

def normalize_separator(value, separator="&&&"):
    """
    Удаляет пробелы после разделителя "&&&".
    """
    if isinstance(value, str):
        return value.replace("&&& ", "&&&")  # Убираем пробел после &&&
    return value

def split_columns(SplitDB):
    """
    Разделяет выбранные столбцы таблицы по заданному разделителю, оставляя остальные столбцы в каждой строке.

    :param SplitDB: Входные данные в виде словаря или списка словарей.
    """
    table = [SplitDB] if isinstance(SplitDB, dict) else SplitDB

    if not isinstance(SplitDB, (dict, list)):
        raise ValueError(f"Ожидался словарь или список словарей. Получено: {type(SplitDB)}")

    separator = "&&&"
    columns_to_split = ["классификация", "номер_лота", "цена", "описание", "номер_торгов", "балансовая_стоимость", "сведения_о_заключении_договора", "наименование_покупателя"]
    # Проверяем, что указанные столбцы есть в таблице
    missing_cols = set(columns_to_split) - set(table[0].keys())
    if missing_cols:
        raise ValueError(f"Отсутствуют столбцы: {missing_cols}")

    split_data = []

    # Разделяем данные в указанных столбцах
    for row in table:
        # Приводим данные к правильному формату без пробела после '&&&'
        for col in columns_to_split:
            if col in row and row[col]:
                row[col] = normalize_separator(row[col])

        max_len = max(
            len(str(row.get(col, "")).split(separator)) if col in columns_to_split and row.get(col) else 1
            for col in columns_to_split
        )
        for i in range(max_len):
            new_row = {}
            for col in row:
                if col in columns_to_split and row.get(col):  # Проверяем, что столбец не пустой
                    parts = str(row[col]).split(separator)
                    new_row[col] = parts[i] if i < len(parts) else None
                else:
                    # Сохраняем данные в каждой строке
                    new_row[col] = row[col]
            split_data.append(new_row)

    data = get_massageLots(split_data)
    return data
