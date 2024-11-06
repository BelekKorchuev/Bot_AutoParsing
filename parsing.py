from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

# Настройка Selenium (установка драйвера Chrome)
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Функция для получения и парсинга HTML-кода по предоставленной ссылке
def parse_message_page(url, driver):
    # Переход по ссылке
    driver.get(url)

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
    rows = table_main.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if len(cells) == 2:
            field = cells[0].text.strip()
            value = cells[1].text.strip()
            data[field] = value

    # Данные о должнике
    debtor_section = soup.find('div', string="Должник").find_next('table')
    debtor_rows = debtor_section.find_all('tr')
    for row in debtor_rows:
        cells = row.find_all('td')
        if len(cells) == 2:
            field = cells[0].text.strip()
            value = cells[1].text.strip()
            data[field] = value

    # Информация об арбитражном управляющем
    arbiter_section = soup.find('div', string="Кем опубликовано").find_next('table')
    arbiter_rows = arbiter_section.find_all('tr')
    for row in arbiter_rows:
        cells = row.find_all('td')
        if len(cells) == 2:
            field = cells[0].text.strip()
            value = cells[1].text.strip()
            data[field] = value

    return data


# Открытие страницы с сообщениями
url = "https://old.bankrot.fedresurs.ru/Messages.aspx"  # Замените на URL вашей страницы
driver.get(url)
time.sleep(3)

# Получение HTML-кода страницы
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')
table = soup.find('table', class_='bank')

# Находим нужные строки с сообщениями
rows = table.find_all('tr', class_='row')  # Убедитесь, что 'class=row' совпадает

for row in rows:
    cells = row.find_all('td')
    if len(cells) >= 5:
        date = cells[0].text.strip()
        message_type = cells[1].text.strip()
        debtor_name = cells[2].text.strip()
        address = cells[3].text.strip()
        arbiter_name = cells[4].text.strip()

        print(f"Дата: {date}")
        print(f"Тип сообщения: {message_type}")
        print(f"Должник: {debtor_name}")
        print(f"Адрес: {address}")
        print(f"Арбитражный управляющий: {arbiter_name}")

    # Извлечение ссылки на сообщение
    message_link_tag = row.find('a', href=True)
    if message_link_tag:
        message_link = "https://old.bankrot.fedresurs.ru" + message_link_tag['href']
        print("Ссылка на сообщение:", message_link)

        # Переход по ссылке и извлечение данных сообщения
        message_data = parse_message_page(message_link, driver)
        print("Содержимое сообщения:", message_data)
    else:
        print("Ссылка не найдена в текущей строке.")

# Закрытие браузера после завершения работы
driver.quit()
