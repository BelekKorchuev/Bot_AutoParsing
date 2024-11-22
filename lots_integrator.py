import asyncio
from logScript import logger
import os

from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, exists
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

# Загрузка переменных окружения
load_dotenv(dotenv_path='.env')

db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")

db_url = f"postgresql+asyncpg://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

def get_engine():
    return create_async_engine(db_url, echo=False)

def get_session_maker(engine):
    return sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# Проверка должников и лотов
async def check_debtor(lots_data, session_maker):
    metadata = MetaData()
    async with session_maker() as session:
        async with session.begin():
            conn = await session.connection()
            await conn.run_sync(metadata.reflect)
            dolzhnik = Table('dolzhnik', metadata, autoload_with=conn)

            for lot in lots_data:
                debtor_inn = lot.get("ИНН_Должника")

                if not debtor_inn:
                    logger.error(f"ИНН должника отсутствует в данных лота: {lot}")
                    continue

                debtor_exists = await session.execute(
                    exists().where(dolzhnik.c.Инн_Должника == debtor_inn).select()
                )
                debtor_exists = debtor_exists.scalar()

                if not debtor_exists:
                    logger.warning(f"Должник с ИНН {debtor_inn} из лота отсутствует в базе данных.")
                    return None

                logger.info(f"Должник с ИНН {debtor_inn} из лота найден в базе данных.")

# Логика первой программы (ДКП или Результаты)
async def process_data_dkp_or_results(data_list, session_maker):

    async with session_maker() as session:
        for data in data_list:
            logger.info(f"Обрабатываем данные: {data}")
            previous_message_number = str(data['Предыдущий_номер_сообщения_по_лот'])
            lot_number = str(data['Номер_лота'])
            case_number = str(data['Номер_дела'])

            # Проверка пустых полей
            if not previous_message_number or not lot_number:
                logger.info(f"Добавляем в error_table из-за пустого значения: 'Предыдущий_номер_сообщения_по_лот'='{previous_message_number}', 'Номер_лота'='{lot_number}'.")
                insert_error_table_query = text("""
                    INSERT INTO error_table (
                        ИНН_Должника, Дата_публикации, Дата_начала_торгов, Дата_окончания, Номер_дела,
                        Действующий_номер_сообщения, Номер_лота, Ссылка_на_сообщение_ЕФРСБ, Имущество,
                        Классификация_имущества, Цена, Предыдущий_номер_сообщения_по_лот,
                        Дата_публикации_предыдущего_сообщ, Организатор_торгов, Торговая_площадка,
                        Статус_ДКП, Статус_сообщения_о_результатах_то, ЕФРСБ_ББ, Должник_текст,
                        вид_торгов, Дата_публикации_сообщения_ДКП, Дата_публикации_сообщения_о_резул
                    ) VALUES (
                        :ИНН_Должника, :Дата_публикации, :Дата_начала_торгов, :Дата_окончания, :Номер_дела,
                        :Действующий_номер_сообщения, :Номер_лота, :Ссылка_на_сообщение_ЕФРСБ, :Имущество,
                        :Классификация_имущества, :Цена, :Предыдущий_номер_сообщения_по_лот,
                        :Дата_публикации_предыдущего_сообщ, :Организатор_торгов, :Торговая_площадка,
                        :Статус_ДКП, :Статус_сообщения_о_результатах_то, :ЕФРСБ_ББ, :Должник_текст,
                        :вид_торгов, :Дата_публикации_сообщения_ДКП, :Дата_публикации_сообщения_о_резул
                    )
                """)
                await session.execute(insert_error_table_query, {k: str(v) if v is not None else None for k, v in data.items()})
                await session.commit()
                logger.info("Запись добавлена в error_table и пропущена.")
                continue

            # Логика проверки критериев и обновления данных (Критерии 1, 2, 3)
            # Критерий 1
            logger.info(f"Проверяем критерий 1: Предыдущий_номер_сообщения_по_лот='{previous_message_number}', Номер_лота='{lot_number}'.")
            query_criteria_1 = text("""
                        SELECT * FROM lots 
                        WHERE Действующий_номер_сообщения = :previous_message_number 
                          AND Номер_лота = :lot_number
                    """)
            result_criteria_1 = await session.execute(query_criteria_1, {
                'previous_message_number': previous_message_number,
                'lot_number': lot_number
            })
            matching_rows_criteria_1 = result_criteria_1.fetchall()
            logger.info(f"Критерий 1 найдено совпадений: {len(matching_rows_criteria_1)}.")

            for row in matching_rows_criteria_1:
                row_data = row._mapping
                logger.info(f"Переносим строку в delete_lots: {row_data}.")
                insert_delete_query = text("""
                            INSERT INTO delete_lots 
                            SELECT * FROM lots 
                            WHERE Действующий_номер_сообщения = :previous_message_number 
                              AND Номер_лота = :lot_number
                        """)
                await session.execute(insert_delete_query, {
                    'previous_message_number': row_data['Действующий_номер_сообщения'],
                    'lot_number': row_data['Номер_лота']
                })

                delete_query = text("""
                            DELETE FROM lots 
                            WHERE Действующий_номер_сообщения = :previous_message_number 
                              AND Номер_лота = :lot_number
                        """)
                await session.execute(delete_query, {
                    'previous_message_number': row_data['Действующий_номер_сообщения'],
                    'lot_number': row_data['Номер_лота']
                })
            await session.commit()
            logger.info("Критерий 1 обработан.")

            # Критерий 2
            logger.info(
                f"Проверяем критерий 2: Предыдущий_номер_сообщения_по_лот='{previous_message_number}', Номер_лота='{lot_number}'.")
            query_criteria_2 = text("""
                        SELECT * FROM lots 
                        WHERE Предыдущий_номер_сообщения_по_лот = :previous_message_number 
                          AND Номер_лота = :lot_number
                    """)
            result_criteria_2 = await session.execute(query_criteria_2, {
                'previous_message_number': previous_message_number,
                'lot_number': lot_number
            })
            matching_rows_criteria_2 = result_criteria_2.fetchall()
            logger.info(f"Критерий 2 найдено совпадений: {len(matching_rows_criteria_2)}.")

            for row in matching_rows_criteria_2:
                row_data = row._mapping
                logger.info(f"Переносим строку в delete_lots: {row_data}.")
                insert_delete_query = text("""
                            INSERT INTO delete_lots 
                            SELECT * FROM lots 
                            WHERE Предыдущий_номер_сообщения_по_лот = :previous_message_number 
                              AND Номер_лота = :lot_number
                        """)
                await session.execute(insert_delete_query, {
                    'previous_message_number': row_data['Предыдущий_номер_сообщения_по_лот'],
                    'lot_number': row_data['Номер_лота']
                })

                delete_query = text("""
                            DELETE FROM lots 
                            WHERE Предыдущий_номер_сообщения_по_лот = :previous_message_number 
                              AND Номер_лота = :lot_number
                        """)
                await session.execute(delete_query, {
                    'previous_message_number': row_data['Предыдущий_номер_сообщения_по_лот'],
                    'lot_number': row_data['Номер_лота']
                })
            await session.commit()
            logger.info("Критерий 2 обработан.")

            # Критерий 3
            logger.info(f"Проверяем критерий 3: Номер_дела='{case_number}', Номер_лота='{lot_number}'.")
            query_criteria_3 = text("""
                        SELECT * FROM lots 
                        WHERE Номер_дела = :case_number 
                          AND Номер_лота = :lot_number
                    """)
            result_criteria_3 = await session.execute(query_criteria_3, {
                'case_number': case_number,
                'lot_number': lot_number
            })
            matching_rows_criteria_3 = result_criteria_3.fetchall()
            logger.info(f"Критерий 3 найдено совпадений: {len(matching_rows_criteria_3)}.")

            for row in matching_rows_criteria_3:
                row_data = row._mapping
                logger.info(f"Переносим строку в delete_lots: {row_data}.")
                insert_delete_query = text("""
                            INSERT INTO delete_lots 
                            SELECT * FROM lots 
                            WHERE Номер_дела = :case_number 
                              AND Номер_лота = :lot_number
                        """)
                await session.execute(insert_delete_query, {
                    'case_number': row_data['Номер_дела'],
                    'lot_number': row_data['Номер_лота']
                })

                delete_query = text("""
                            DELETE FROM lots 
                            WHERE Номер_дела = :case_number 
                              AND Номер_лота = :lot_number
                        """)
                await session.execute(delete_query, {
                    'case_number': row_data['Номер_дела'],
                    'lot_number': row_data['Номер_лота']
                })
            await session.commit()
            logger.info("Критерий 3 обработан.")

            # Добавление новой строки в lots
            logger.info(f"Добавляем новую строку в lots: {data}.")
            insert_query = text("""
                        INSERT INTO lots (
                            ИНН_Должника, Дата_публикации, Дата_начала_торгов, Дата_окончания, Номер_дела,
                            Действующий_номер_сообщения, Номер_лота, Ссылка_на_сообщение_ЕФРСБ, Имущество,
                            Классификация_имущества, Цена, Предыдущий_номер_сообщения_по_лот,
                            Дата_публикации_предыдущего_сообщ, Организатор_торгов, Торговая_площадка,
                            Статус_ДКП, Статус_сообщения_о_результатах_то, ЕФРСБ_ББ, Должник_текст,
                            вид_торгов, Дата_публикации_сообщения_ДКП, Дата_публикации_сообщения_о_резул
                        ) VALUES (
                            :ИНН_Должника, :Дата_публикации, :Дата_начала_торгов, :Дата_окончания, :Номер_дела,
                            :Действующий_номер_сообщения, :Номер_лота, :Ссылка_на_сообщение_ЕФРСБ, :Имущество,
                            :Классификация_имущества, :Цена, :Предыдущий_номер_сообщения_по_лот,
                            :Дата_публикации_предыдущего_сообщ, :Организатор_торгов, :Торговая_площадка,
                            :Статус_ДКП, :Статус_сообщения_о_результатах_то, :ЕФРСБ_ББ, :Должник_текст,
                            :вид_торгов, :Дата_публикации_сообщения_ДКП, :Дата_публикации_сообщения_о_резул
                        )
                    """)
            await session.execute(insert_query, {k: str(v) if v is not None else None for k, v in data.items()})
            await session.commit()
            logger.info("Новая строка успешно добавлена в lots.")

# Логика второй программы (Аукцион или Публичка)
async def process_data_auction_or_public(data_list, session_maker):
    async with session_maker() as session:
        for record in data_list:
            logger.info(f"Обрабатываем запись: {record}")
            try:
                current_message_id = record['Действующий_номер_сообщения']
                case_number = record['Номер_дела']
                lot_number = record['Номер_лота']

                # Логика проверки критериев и обновления данных (Критерии 1, 2)
                # Критерий 1: Действующий_номер_сообщения и Номер_лота
                logger.info(
                    f"Проверяем критерий 1: совпадение по 'Действующий_номер_сообщения' ({current_message_id}) и 'Номер_лота' ({lot_number}).")
                query_criteria_1 = text("""
                                    SELECT * FROM lots 
                                    WHERE Действующий_номер_сообщения = :current_message_id 
                                      AND Номер_лота = :lot_number
                                """)
                result_criteria_1 = await session.execute(query_criteria_1, {
                    'current_message_id': current_message_id,
                    'lot_number': lot_number
                })
                matching_rows_criteria_1 = result_criteria_1.fetchall()
                logger.info(
                    f"Найдено записей по критерию 1 (совпадение по Действующий_номер_сообщения и Номер_лота): {len(matching_rows_criteria_1)}.")

                for row in matching_rows_criteria_1:
                    logger.info(f"Переносим запись в delete_lots: {row._mapping}")
                    insert_delete_query = text("""
                                        INSERT INTO delete_lots 
                                        SELECT * FROM lots 
                                        WHERE Действующий_номер_сообщения = :current_message_id 
                                          AND Номер_лота = :lot_number
                                    """)
                    await session.execute(insert_delete_query, {
                        'current_message_id': row._mapping['Действующий_номер_сообщения'],
                        'lot_number': row._mapping['Номер_лота']
                    })
                    delete_query = text("""
                                        DELETE FROM lots 
                                        WHERE Действующий_номер_сообщения = :current_message_id 
                                          AND Номер_лота = :lot_number
                                    """)
                    await session.execute(delete_query, {
                        'current_message_id': row._mapping['Действующий_номер_сообщения'],
                        'lot_number': row._mapping['Номер_лота']
                    })

                # Критерий 2: Номер_дела и Номер_лота
                logger.info(f"Проверяем критерий 2: совпадение по 'Номер_дела' ({case_number}) и 'Номер_лота' ({lot_number}).")
                query_criteria_2 = text("""
                                    SELECT * FROM lots 
                                    WHERE Номер_дела = :case_number 
                                      AND Номер_лота = :lot_number
                                """)
                result_criteria_2 = await session.execute(query_criteria_2, {
                    'case_number': case_number,
                    'lot_number': lot_number
                })
                matching_rows_criteria_2 = result_criteria_2.fetchall()
                logger.info(
                    f"Найдено записей по критерию 2 (совпадение по Номер_дела и Номер_лота): {len(matching_rows_criteria_2)}.")

                for row in matching_rows_criteria_2:
                    logger.info(f"Переносим запись в delete_lots: {row._mapping}")
                    insert_delete_query = text("""
                                        INSERT INTO delete_lots 
                                        SELECT * FROM lots 
                                        WHERE Номер_дела = :case_number 
                                          AND Номер_лота = :lot_number
                                    """)
                    await session.execute(insert_delete_query, {
                        'case_number': row._mapping['Номер_дела'],
                        'lot_number': row._mapping['Номер_лота']
                    })
                    delete_query = text("""
                                        DELETE FROM lots 
                                        WHERE Номер_дела = :case_number 
                                          AND Номер_лота = :lot_number
                                    """)
                    await session.execute(delete_query, {
                        'case_number': row._mapping['Номер_дела'],
                        'lot_number': row._mapping['Номер_лота']
                    })

                # Добавление новой строки в lots
                logger.info(f"Добавляем новую строку в lots: {record}.")
                insert_query = text("""
                                    INSERT INTO lots (
                                        ИНН_Должника, Дата_публикации, Дата_начала_торгов, Дата_окончания, Номер_дела,
                                        Действующий_номер_сообщения, Номер_лота, Ссылка_на_сообщение_ЕФРСБ, Имущество,
                                        Классификация_имущества, Цена, Предыдущий_номер_сообщения_по_лот,
                                        Дата_публикации_предыдущего_сообщ, Организатор_торгов, Торговая_площадка,
                                        Статус_ДКП, Статус_сообщения_о_результатах_то, ЕФРСБ_ББ, Должник_текст,
                                        вид_торгов, Дата_публикации_сообщения_ДКП, Дата_публикации_сообщения_о_резул
                                    ) VALUES (
                                        :ИНН_Должника, :Дата_публикации, :Дата_начала_торгов, :Дата_окончания, :Номер_дела,
                                        :Действующий_номер_сообщения, :Номер_лота, :Ссылка_на_сообщение_ЕФРСБ, :Имущество,
                                        :Классификация_имущества, :Цена, :Предыдущий_номер_сообщения_по_лот,
                                        :Дата_публикации_предыдущего_сообщ, :Организатор_торгов, :Торговая_площадка,
                                        :Статус_ДКП, :Статус_сообщения_о_результатах_то, :ЕФРСБ_ББ, :Должник_текст,
                                        :вид_торгов, :Дата_публикации_сообщения_ДКП, :Дата_публикации_сообщения_о_резул
                                    )
                                """)

                # Выполняем запрос
                await session.execute(insert_query, record)
                await session.commit()

            except Exception as e:
                logger.error(f"Ошибка при обработке записи: {e}")
                await session.rollback()


# Логика третьей программы (Оценка)
async def process_data_evaluation(data_list, session_maker):
    async with session_maker() as session:
        for data in data_list:
            logger.info(f"Обрабатываем данные для Оценки: {data}")
            try:
                current_message_id = data['Действующий_номер_сообщения']
                lot_property = data['Имущество']
                case_number = data['Номер_дела']
                вид_торгов = data['вид_торгов']

                # Критерий 1: Действующий_номер_сообщения и Имущество
                query_criteria_1 = text("""
                    SELECT * FROM lots
                    WHERE Действующий_номер_сообщения = :current_message_id
                      AND Имущество = :lot_property
                """)
                result_criteria_1 = await session.execute(query_criteria_1, {
                    'current_message_id': current_message_id,
                    'lot_property': lot_property
                })
                matching_rows_criteria_1 = result_criteria_1.fetchall()
                logger.info(f"Найдено записей по критерию 1 (Действующий_номер_сообщения и Имущество): {len(matching_rows_criteria_1)}.")

                for row in matching_rows_criteria_1:
                    logger.info(f"Переносим запись в delete_lots: {row._mapping}")
                    insert_delete_query = text("""
                        INSERT INTO delete_lots
                        SELECT * FROM lots
                        WHERE Действующий_номер_сообщения = :current_message_id
                          AND Имущество = :lot_property
                    """)
                    await session.execute(insert_delete_query, {
                        'current_message_id': row._mapping['Действующий_номер_сообщения'],
                        'lot_property': row._mapping['Имущество']
                    })
                    delete_query = text("""
                        DELETE FROM lots
                        WHERE Действующий_номер_сообщения = :current_message_id
                          AND Имущество = :lot_property
                    """)
                    await session.execute(delete_query, {
                        'current_message_id': row._mapping['Действующий_номер_сообщения'],
                        'lot_property': row._mapping['Имущество']
                    })

                # Критерий 2: вид_торгов, Номер_дела и Имущество
                query_criteria_2 = text("""
                    SELECT * FROM lots
                    WHERE вид_торгов = :вид_торгов
                      AND Номер_дела = :case_number
                      AND Имущество = :lot_property
                """)
                result_criteria_2 = await session.execute(query_criteria_2, {
                    'вид_торгов': вид_торгов,
                    'case_number': case_number,
                    'lot_property': lot_property
                })
                matching_rows_criteria_2 = result_criteria_2.fetchall()
                logger.info(f"Найдено записей по критерию 2 (вид_торгов, Номер_дела и Имущество): {len(matching_rows_criteria_2)}.")

                for row in matching_rows_criteria_2:
                    logger.info(f"Переносим запись в delete_lots: {row._mapping}")
                    insert_delete_query = text("""
                        INSERT INTO delete_lots
                        SELECT * FROM lots
                        WHERE вид_торгов = :вид_торгов
                          AND Номер_дела = :case_number
                          AND Имущество = :lot_property
                    """)
                    await session.execute(insert_delete_query, {
                        'вид_торгов': row._mapping['вид_торгов'],
                        'case_number': row._mapping['Номер_дела'],
                        'lot_property': row._mapping['Имущество']
                    })
                    delete_query = text("""
                        DELETE FROM lots
                        WHERE вид_торгов = :вид_торгов
                          AND Номер_дела = :case_number
                          AND Имущество = :lot_property
                    """)
                    await session.execute(delete_query, {
                        'вид_торгов': row._mapping['вид_торгов'],
                        'case_number': row._mapping['Номер_дела'],
                        'lot_property': row._mapping['Имущество']
                    })

                # Добавление новой строки в lots
                logger.info(f"Добавляем новую строку в lots: {data}.")
                insert_query = text("""
                    INSERT INTO lots (
                        ИНН_Должника, Дата_публикации, Дата_начала_торгов, Дата_окончания, Номер_дела,
                        Действующий_номер_сообщения, Номер_лота, Ссылка_на_сообщение_ЕФРСБ, Имущество,
                        Классификация_имущества, Цена, Предыдущий_номер_сообщения_по_лот,
                        Дата_публикации_предыдущего_сообщ, Организатор_торгов, Торговая_площадка,
                        Статус_ДКП, Статус_сообщения_о_результатах_то, ЕФРСБ_ББ, Должник_текст,
                        вид_торгов, Дата_публикации_сообщения_ДКП, Дата_публикации_сообщения_о_резул
                    ) VALUES (
                        :ИНН_Должника, :Дата_публикации, :Дата_начала_торгов, :Дата_окончания, :Номер_дела,
                        :Действующий_номер_сообщения, :Номер_лота, :Ссылка_на_сообщение_ЕФРСБ, :Имущество,
                        :Классификация_имущества, :Цена, :Предыдущий_номер_сообщения_по_лот,
                        :Дата_публикации_предыдущего_сообщ, :Организатор_торгов, :Торговая_площадка,
                        :Статус_ДКП, :Статус_сообщения_о_результатах_то, :ЕФРСБ_ББ, :Должник_текст,
                        :вид_торгов, :Дата_публикации_сообщения_ДКП, :Дата_публикации_сообщения_о_резул
                    )
                """)
                await session.execute(insert_query, data)
                await session.commit()

            except Exception as e:
                logger.error(f"Ошибка при обработке записи: {e}")
                await session.rollback()


# Основная функция выбора логики обработки данных
async def main(data_list):
    engine = get_engine()
    session_maker = get_session_maker(engine)

    for data in data_list:
        ckecking_dolzhnic = await check_debtor(data_list, session_maker)

        if ckecking_dolzhnic is None:
            continue

        massage_type = data.get('вид_торгов')
        if massage_type in ['ДКП', 'Результаты торгов']:
            logger.info(f"Вид торгов: {massage_type}")
            await process_data_dkp_or_results([data], session_maker)
        elif massage_type in ['Аукцион', 'Публичка']:
            logger.info(f"Вид торгов: {massage_type}")
            await process_data_auction_or_public([data], session_maker)
        elif massage_type == "Оценка":
            logger.info(f"Вид торгов: {massage_type}")
            await process_data_evaluation([data], session_maker)
        else:
            logger.warning(f"Неизвестное значение 'вид_торгов': {massage_type}. Пропускаем запись.")



def lots_analyze(data):
   logger.info("Начинаем сравнение лотов.")
   asyncio.run(main(data))
   logger.info("Лоты сравнены.")

