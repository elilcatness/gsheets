import json
import os
from datetime import date, timedelta
import time
from csv import DictWriter

# noinspection PyUnresolvedReferences
from apiclient.discovery import build
from dotenv import load_dotenv
from gspread import service_account_from_dict, Client, Worksheet
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials

from constants import DIGITS, MAX_ROWS_COUNT

from src.db import db_session
from src.db.models.last_date import LastDate


def get_spreadsheet(creds: dict, spread_id: str):
    account = service_account_from_dict(creds)
    return account.open_by_key(spread_id)


def extract_urls(spread_url: str, creds: dict):
    if (not spread_url.startswith('http://docs.google.com/spreadsheets/d/') and
            not spread_url.startswith('https://docs.google.com/spreadsheets/d/')):
        return print('Введён неверный URL таблицы')
    sub = spread_url.split('/')
    try:
        spread_id = sub[sub.index('d') + 1]
    except IndexError:
        return print('В URL отсутствует ID таблицы')
    spread = get_spreadsheet(creds, spread_id)
    sheet = spread.worksheets()[0]
    urls = []
    for col in range(1, sheet.col_count + 1):
        values = sheet.col_values(col)
        if not values:
            break
        for val in values:
            if isinstance(val, str) and val.startswith('http') and val not in urls:
                urls.append(val)
    return urls


def get_console(creds: dict):
    scope = 'https://www.googleapis.com/auth/webmasters.readonly'
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scopes=[scope])
    http = credentials.authorize(Http())
    return build('searchconsole', 'v1', http=http)


def _execute_request(service, url: str, request: dict):
    return service.searchanalytics().query(siteUrl=url, body=request).execute()


def process_url(url: str, date: str) -> list[dict]:
    # with open('../creds.json', encoding='utf-8') as f:
    #     creds = json.loads(f.read())
    # service = get_console(creds)
    # request = {'startDate': date,
    #            'endDate': date,
    #            'dimensions': ['date']}
    # response = _execute_request(service, url, request)
    # return []
    with open('../input.json', encoding='utf-8') as f:
        return json.loads(f.read())


def process_table(data: list[dict], service, table_name: str, dt: str, email: str):
    filename = f'{"".join(str(time.time()).split("."))}.csv'
    headers = list(data[0].keys())
    with open(filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = DictWriter(csv_file, headers, delimiter=',')
        writer.writeheader()
        writer.writerows(data)
    spread = service.create(table_name)
    service: Client
    print(f'{spread=}')
    with open(filename, encoding='utf-8') as f:
        service.import_csv(spread.id, f.read().encode('utf-8'))
    spread.worksheets()[0].update_title(dt)
    spread.share(email, 'user', 'owner')
    try:
        os.remove(filename)
    except Exception as e:
        print(f'[FILE DELETE] {e}')
    print(spread.url, end='\n\n')


def fill_url_spread(url: str, service: Client, dt: str, email: str):
    url = url.split('//')[1].rstrip('/').replace('.', '_')
    dt = dt.replace('-', '')
    table_name = f'Search_console_{url}_{dt}'
    data = process_url(url, date)
    n = 1
    set_number = len(data) > MAX_ROWS_COUNT
    while len(data) > MAX_ROWS_COUNT or n == 1:
        if set_number:
            sub_data = data[:MAX_ROWS_COUNT]
            data = data[MAX_ROWS_COUNT:]
            sub_table_name = f'{table_name}_{n}'
        else:
            sub_data = data[:]
            sub_table_name = table_name
        process_table(sub_data, service, sub_table_name, dt, email)
        n += 1


def main():
    # with db_session.create_session() as session:
    #     date = session.query(LastDate).first()
    #     if not date:
    #         date = LastDate(last_date=date.today().toisoformat())
    #         session.add(date)
    #         session.commit()
    #     last_date = date.last_date
    with open('creds.json', encoding='utf-8') as f:
        creds = json.loads(f.read())
    last_date = date.today() - timedelta(days=1)
    _date = date.today()
    dates = []
    while last_date <= _date:
        dates.append(_date.isoformat())
        _date -= timedelta(days=1)
    service = service_account_from_dict(creds)
    email = os.getenv('email')
    for url in extract_urls(os.getenv('spread_url'), creds):
        for dt in dates[::-1]:
            fill_url_spread(url, service, dt, email)


if __name__ == '__main__':
    load_dotenv()
    # db_session.global_init(os.getenv('DATABASE_URL'))
    main()
