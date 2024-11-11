import psycopg2
from datetime import datetime

# Функция для подключения к базе данных
def get_db_connection():
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="test1",
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
    date = datetime.strptime(raw_data['date'], "%d.%m.%Y %H:%M:%S") if 'date' in raw_data else None
    message_type = clean_text(raw_data.get('message_type', ''))
    debtor_name = clean_text(raw_data.get('debtor_name', ''))
    address = clean_text(raw_data.get('address', ''))
    arbiter_name = clean_text(raw_data.get('arbiter_name', ''))
    message_link = raw_data.get('message_link', '')

    # Данные из содержимого сообщения (дополнительные поля)
    message_content = raw_data.get('message_content', {})
    message_title = clean_text(message_content.get('title', ''))
    message_number = clean_text(message_content.get('№ сообщения', ''))
    publication_date = message_content.get('Дата публикации', '')
    if publication_date:
        publication_date = datetime.strptime(publication_date, "%d.%m.%Y")
    else:
        publication_date = None
    case_number = clean_text(message_content.get('№ дела', ''))
    inn = clean_text(message_content.get('ИНН', ''))
    snils = clean_text(message_content.get('СНИЛС', ''))
    email = clean_text(message_content.get('E-mail', ''))

    # Данные о торгах (новые поля)
    lot_number = clean_text(message_content.get('Номер лота', ''))
    description = clean_text(message_content.get('Описание', ''))
    buyer_name = clean_text(message_content.get('Наименование покупателя', ''))
    best_price = clean_text(message_content.get('Лучшая цена', ''))
    classification = clean_text(message_content.get('Классификация', ''))

    # Сведения о заключении договора купли-продажи и результатах торгов
    sale_agreements = clean_text(message_content.get('Сведения о заключении договора купли-продажи', ''))
    auction_results = clean_text(message_content.get('Сообщение о результатах торгов', ''))

    # Подготовленные данные для вставки
    prepared_data = {
        'date': date,
        'message_type': message_type,
        'debtor_name': debtor_name,
        'address': address,
        'arbiter_name': arbiter_name,
        'message_link': message_link,
        'message_title': message_title,
        'message_number': message_number,
        'publication_date': publication_date,
        'case_number': case_number,
        'inn': inn,
        'snils': snils,
        'email': email
    }
    return prepared_data

# Функция для вставки данных в базу данных
def insert_message_to_db(data, connection):
    cursor = connection.cursor()
    insert_query = '''
    INSERT INTO messages (
        date, message_type, debtor_name, address, arbiter_name, message_link,
        message_title, message_number, publication_date, case_number, inn, snils, email
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    '''
    cursor.execute(insert_query, (
        data.get('date'),
        data.get('message_type'),
        data.get('debtor_name'),
        data.get('address'),
        data.get('arbiter_name'),
        data.get('message_link'),
        data.get('message_title'),
        data.get('message_number'),
        data.get('publication_date'),
        data.get('case_number'),
        data.get('inn'),
        data.get('snils'),
        data.get('email')
    ))
    connection.commit()
    new_id = cursor.fetchone()[0]
    cursor.close()
    return new_id
