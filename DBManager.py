import psycopg2
from datetime import datetime

# Функция для подключения к базе данных
def get_db_connection():
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="ourcrm",
        user="postgres",
        password="belek12"
    )
    return connection

# Функция для очистки текста
def clean_text(text):
    """Удаляет лишние символы из текста и приводит его в читаемый вид"""
    return text.replace('\xa0', ' ').strip()

# Функция для подготовки данных для вставки в базу данных
def prepare_data_for_db(raw_data):
    """Приводит данные к нужному формату для вставки в базу данных"""

    # Общие данные для всех сообщений
    date = datetime.strptime(raw_data['дата'], "%d.%m.%Y %H:%M:%S") if 'date' in raw_data else None
    message_type = clean_text(raw_data.get('тип_сообщения', ''))
    debtor = clean_text(raw_data.get('должник', ''))
    debtor_link = raw_data.get('должник_ссылка', '')
    arbiter = clean_text(raw_data.get('арбитр', ''))
    arbiter_link = raw_data.get('арбитр_ссылка', '')
    message_link = raw_data.get('должник_ссылка', '')

    # Данные из содержимого сообщения
    message_content = raw_data.get('message_content', {})
    message_number = clean_text(message_content.get('№ сообщения', ''))
    publication_date = message_content.get('Дата публикации', '')
    if publication_date:
        publication_date = datetime.strptime(publication_date, "%d.%m.%Y")
    else:
        publication_date = None

    # Данные о должнике
    debtor_name = clean_text(message_content.get('Наименование должника', '') or message_content.get('ФИО должника', ''))
    address = clean_text(message_content.get('Адрес', ''))
    ogrn = clean_text(message_content.get('ОГРН', ''))
    inn = clean_text(message_content.get('ИНН', ''))
    case_number = clean_text(message_content.get('№ дела', ''))
    birth_date = message_content.get('Дата рождения', '')
    birth_date = datetime.strptime(birth_date, "%d.%m.%Y") if birth_date else None
    birth_place = clean_text(message_content.get('Место рождения', ''))
    residence = clean_text(message_content.get('Место жительства', ''))
    snils = clean_text(message_content.get('СНИЛС', ''))

    # Данные об арбитраже
    arbiter_name = clean_text(message_content.get('Арбитражный управляющий', ''))
    correspondence_address = clean_text(message_content.get('Адрес для корреспонденции', ''))
    email = clean_text(message_content.get('E-mail', ''))
    sro_au = clean_text(message_content.get('СРО АУ', ''))
    sro_address = clean_text(message_content.get('Адрес СРО АУ', ''))
    auction_announcement = clean_text(message_content.get('Объявление о проведении торгов', ''))

    # Данные о торгах
    trading_platform = clean_text(message_content.get('Торговая площадка', ''))
    trading_number = clean_text(message_content.get('Номер торгов', ''))
    lot_number = message_content.get('Номер лота', None)
    description = clean_text(message_content.get('Описание', ''))
    contract_info = clean_text(message_content.get('Сведения о заключении договора', ''))
    contract_number = clean_text(message_content.get('Номер договора', ''))
    contract_date = message_content.get('Дата заключения договора', '')
    contract_date = datetime.strptime(contract_date,"%d.%m.%Y") if contract_date else None

    purchase_price = message_content.get('Цена', None)
    buyer_name = clean_text(message_content.get('Наименование покупателя', ''))
    text = clean_text(message_content.get('текст', ''))

    # Классификация и результаты
    classification = clean_text(message_content.get('Классификация имущества', ''))
    dkp = clean_text(message_content.get('Сведения о заключении договора купли-продажи', ''))
    auction_type = clean_text(message_content.get('Вид торгов', ''))
    application_start_date = message_content.get('Дата и время начала подачи заявок', '')
    application_start_date = datetime.strptime(application_start_date,
                                               "%d.%m.%Y %H:%M:%S") if application_start_date else None
    application_end_date = message_content.get('Дата и время окончания подачи заявок', '')
    application_end_date = datetime.strptime(application_end_date, "%d.%m.%Y %H:%M:%S") if application_end_date else None
    application_rules = clean_text(message_content.get('Правила подачи заявок', ''))
    auction_date = message_content.get('Дата и время торгов', '')
    price_submission_form = clean_text(message_content.get('Форма подачи предложения о цене', ''))
    auction_location = clean_text(message_content.get('Место проведения', ''))
    auction_result = clean_text(message_content.get('Результат', ''))

    # Данные по оценке
    evaluation_type = clean_text(message_content.get('Тип', ''))
    evaluation_date = message_content.get('Дата определения стоимости', '')
    evaluation_date = datetime.strptime(evaluation_date, "%d.%m.%Y") if evaluation_date else None
    balance_value = message_content.get('Балансовая стоимость', None)

    # Подготовленные данные для вставки
    prepared_data = {
        'дата': date,
        'тип_сообщения': message_type,
        'должник': debtor,
        'должник_ссылка': debtor_link,
        'арбитр': arbiter,
        'арбитр_ссылка': arbiter_link,
        'сообщение_ссылка': message_link,

        'номер_сообщения': message_number,
        'дата_публикации': publication_date,
        'наименование_должника': debtor_name,
        'адрес ': address,
        'ОГРН': ogrn,
        'ИНН': inn,
        'номер_дела': case_number,
        'дата_рождения': birth_date,
        'место_рождения': birth_place,
        'место_жительства': residence,
        'СНИЛС': snils,

        'ФИО_АУ': arbiter_name,
        'адрес_корреспонденции': correspondence_address,
        'почта': email,
        'СРО_АУ': sro_au,
        'адрес_СРО_АУ': sro_address,
        'объявление_о_проведении_торгов': auction_announcement,

        'торгова_площадка': trading_platform,
        'номер_торгов': trading_number,

        'номер_лота': lot_number,
        'описание': description,
        'сведения_о_заключении_договора': contract_info,
        'номер_договора': contract_number,
        'дата_заключения_договора': contract_date,
        'цена': purchase_price,
        'наименование_покупателя': buyer_name,
        'текст': text,

        'классификация': classification,

        'ДКП': dkp,
        'вид_торгов': auction_type,
        'дата_начала_подачи_заявок': application_start_date,
        'дата_окончания_подачи_заявок': application_end_date,
        'правила_подачи_заявок': application_rules,
        'дата_время_торгов': auction_date,
        'форма_подачи_предложения_о_цене': price_submission_form,
        'место_проведения': auction_location,
        'результат': auction_result,

        'тип_оценки': evaluation_type,
        'дата_определения_стоимости': evaluation_date,
        'балансовая_стоимость': balance_value
    }

    return prepared_data


# Функция для вставки данных в базу данных
def insert_message_to_db(data, connection):
    cursor = connection.cursor()
    insert_query = '''
    INSERT INTO messages (
        дата, тип_сообщения, должник, должник_ссылка, арбитр, арбитр_ссылка, сообщение_ссылка,
        номер_сообщения, дата_публикации,
        наименование_должника, адрес, ОГРН, ИНН, номер_дела, дата_рождения, место_рождения,
        место_жительства, СНИЛС,
        ФИО_АУ, адрес_корреспонденции, почта, СРО_АУ, адрес_СРО_АУ, объявление_о_проведении_торгов,
        торгова_площадка, номер_торгов,
        номер_лота, описание, сведения_о_заключении_договора, номер_договора,
        дата_заключения_договора, цена, наименование_покупателя, текст,
        классификация,
        ДКП, вид_торгов, дата_начала_подачи_заявок, дата_окончания_подачи_заявок, правила_подачи_заявок,
        дата_время_торгов, форма_подачи_предложения_о_цене, место_проведения,
        тип_оценки, дата_определения_стоимости, балансовая_стоимость
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
              %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    '''
    try:
        params = (
            data.get('дата'),
            data.get('тип_сообщения'),
            data.get('должник'),
            data.get('должник_ссылка'),
            data.get('арбитр'),
            data.get('арбитр_ссылка'),
            data.get('сообщение_ссылка'),
            data.get('номер_сообщения'),
            data.get('дата_публикации'),
            data.get('наименование_должника'),
            data.get('адрес'),
            data.get('ОГРН'),
            data.get('ИНН'),
            data.get('номер_дела'),
            data.get('дата_рождения'),
            data.get('место_рождения'),
            data.get('место_жительства'),
            data.get('СНИЛС'),
            data.get('ФИО_АУ'),
            data.get('адрес_корреспонденции'),
            data.get('почта'),
            data.get('СРО_АУ'),
            data.get('адрес_СРО_АУ'),
            data.get('объявление_о_проведении_торгов'),
            data.get('торгова_площадка'),
            data.get('номер_торгов'),
            data.get('номер_лота'),
            data.get('описание'),
            data.get('сведения_о_заключении_договора'),
            data.get('номер_договора'),
            data.get('дата_заключения_договора'),
            data.get('цена'),
            data.get('наименование_покупателя'),
            data.get('текст'),
            data.get('классификация'),
            data.get('ДКП'),
            data.get('вид_торгов'),
            data.get('дата_начала_подачи_заявок'),
            data.get('дата_окончания_подачи_заявок'),
            data.get('правила_подачи_заявок'),
            data.get('дата_время_торгов'),
            data.get('форма_подачи_предложения_о_цене'),
            data.get('место_проведения'),
            data.get('тип_оценки'),
            data.get('дата_определения_стоимости'),
            data.get('балансовая_стоимость')
        )

        # Печать параметров для отладки
        print("Параметры запроса:", params)

        cursor.execute(insert_query, params)
        connection.commit()
        new_id = cursor.fetchone()[0]
        print(f"Данные успешно вставлены с ID: {new_id}")
    except Exception as e:
        print("Ошибка при выполнении запроса:", e)
    finally:
        cursor.close()