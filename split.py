from operator import index

import pandas as pd
from format import get_massageLots



def split_columns(SplitDB):
    """
    Разделяет выбранные столбцы таблицы по заданному разделителю, оставляя остальные столбцы в каждой строке.

    :param file_path: Путь к исходному Excel файлу.
    :param output_path: Путь для сохранения разделённого файла.
    :param columns_to_split: Список столбцов, которые нужно разделить.
    :param separator: Разделитель, по которому производится разделение.
    """
    table = pd.DataFrame(SplitDB, index=[0])
    separator = "&&& "
    columns_to_split = ["классификация", "номер_лота", "цена", "описание", "номер_торгов", "балансовая_стоимость",
                        "наименование_покупателя"]
    # Проверяем, что указанные столбцы есть в таблице
    for col in columns_to_split:
        if col not in table.columns:
            raise ValueError(f"Столбец '{col}' отсутствует в таблице.")

    split_data = []

    # Разделяем данные в указанных столбцах
    for _, row in table.iterrows():
        max_len = max(
            len(str(row[col]).split(separator)) if col in columns_to_split and pd.notna(row[col]) else 1
            for col in columns_to_split
        )
        for i in range(max_len):
            new_row = {}
            for col in table.columns:
                if col in columns_to_split and pd.notna(row[col]):  # Проверяем, что столбец не пустой
                    parts = str(row[col]).split(separator)
                    new_row[col] = parts[i] if i < len(parts) else None
                else:
                    # Сохраняем данные в каждой строке
                    new_row[col] = row[col]
            split_data.append(new_row)

    # Создаём новый DataFrame
    split_df = pd.DataFrame(split_data)

    data = get_massageLots(split_df)
    return data


