import json
import os
from datetime import date, timedelta
import time
from csv import DictWriter

# noinspection PyUnresolvedReferences
from typing import Union

from apiclient.discovery import build
from dotenv import load_dotenv
from googleapiclient.errors import HttpError
from gspread import service_account_from_dict, Client, Worksheet
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from telegram.error import BadRequest
from telegram.ext import CallbackContext

from constants import MAX_ROWS_COUNT

from src.db import db_session
from src.db.models.config import Config
from src.db.models.state import State


def delete_last_message(func):
    def wrapper(update, context: CallbackContext, **kwargs):
        if context.user_data.get('message_id'):
            try:
                context.bot.deleteMessage(context.user_data['id'], context.user_data.pop('message_id'))
            except BadRequest:
                pass
        while context.user_data.get('messages_to_delete'):
            try:
                context.bot.deleteMessage(context.user_data['id'],
                                          context.user_data['messages_to_delete'].pop(0))
            except BadRequest:
                pass
        output = func(update, context, **kwargs)
        if isinstance(output, tuple):
            msg, callback = output
            context.user_data['message_id'] = msg.message_id
        else:
            callback = output
        save_state(context.user_data['id'], callback, context.user_data)
        return callback

    return wrapper


def save_state(user_id: int, callback: str, data: dict):
    with db_session.create_session() as session:
        state = session.query(State).get(user_id)
        str_data = json.dumps(data)
        if state:
            state.user_id = user_id
            state.callback = callback
            state.data = str_data
        else:
            state = State(user_id=user_id, callback=callback, data=str_data)
        session.add(state)
        session.commit()


def get_current_state(user_id: int):
    with db_session.create_session() as session:
        return session.query(State).get(user_id)


def get_config():
    with db_session.create_session() as session:
        try:
            cfg = json.loads(session.query(Config).first().text)
        except AttributeError:
            with open(os.path.join('data', 'config.json'), encoding='utf-8') as f:
                data = f.read()
                session.add(Config(text=data))
                session.commit()
                cfg = json.loads(data)
    return cfg


def save_config(cfg):
    with db_session.create_session() as session:
        config = session.query(Config).first()
        if not config:
            session.add(Config(text=json.dumps(cfg)))
        else:
            config.text = json.dumps(cfg)
            session.merge(config)
        session.commit()


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
    urls, dates = [], []
    today = date.today().isoformat()
    for row in sheet.get_all_values():
        if len(row) == 1:
            row += [today]
        elif len(row) > 2:
            row = row[:2]
        url, dt = row
        try:
            dt = date.fromisoformat(dt).isoformat()
        except (ValueError, TypeError):
            dt = today
        if isinstance(url, str) and url.startswith('http') and url not in urls:
            urls.append(url)
            dates.append(dt)
    return [list(x) for x in zip(urls, dates)]


def get_console(creds: dict):
    scope = 'https://www.googleapis.com/auth/webmasters.readonly'
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scopes=[scope])
    http = credentials.authorize(Http())
    return build('searchconsole', 'v1', http=http)


def _execute_request(service, url: str, request: dict):
    return service.searchanalytics().query(siteUrl=url, body=request).execute()


def process_url(url: str, date: str) -> list[dict]:
    with open('../creds.json', encoding='utf-8') as f:
        creds = json.loads(f.read())
    service = get_console(creds)
    print(f'{date=}')
    headers = ['Page', 'Query', 'Device', 'Country']
    request = {'startDate': '2022-01-01',
               'endDate': '2022-03-18',
               'dimensions': headers}
    response = _execute_request(service, url, request)
    output = []
    for row in response.get('rows', []):
        ...
    print(response)
    exit()
    # return []
    # with open('../input.json', encoding='utf-8') as f:
    #     return json.loads(f.read())


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
    data = process_url(url, dt)
    url = url.split('//')[1].rstrip('/').replace('.', '_')
    dt = dt.replace('-', '')
    table_name = f'Search_console_{url}_{dt}'
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


def get_iso_dates_interval(first_date: Union[str, date], last_date: Union[str, date]) -> list[str]:
    if isinstance(first_date, str):
        first_date = date.fromisoformat(first_date)
    if isinstance(last_date, str):
        last_date = date.fromisoformat(last_date)
    dates = []
    while first_date <= last_date:
        dates.append(first_date.isoformat())
        first_date += timedelta(days=1)
    return dates


def main():
    with open('../creds.json', encoding='utf-8') as f:
        creds = json.loads(f.read())
    input_data = extract_urls(os.getenv('spread_url'), creds)
    new_data = []
    today = date.today().isoformat()
    service = service_account_from_dict(creds)
    email = os.getenv('email')
    for url, last_date in input_data:
        dates = get_iso_dates_interval(last_date, today)
        print(f'{dates=}')
        for dt in dates:
            print(url)
            try:
                fill_url_spread(url, service, dt, email)
            except HttpError as e:
                if e.status_code == 400:
                    print(f'\n[ERROR] {e}\n')
                    break
                raise e
        else:
            new_data.append([url, today])
    print(f'{new_data=}')

    # print(process_url('', '2022-02-12'))

    # with open('creds.json', encoding='utf-8') as f:
    #     creds = json.loads(f.read())
    # last_date = date.today() - timedelta(days=1)
    # _date = date.today()
    # dates = []
    # while last_date <= _date:
    #     dates.append(_date.isoformat())
    #     _date -= timedelta(days=1)
    # service = service_account_from_dict(creds)
    # email = os.getenv('email')
    # for url in extract_urls(os.getenv('spread_url'), creds):
    #     for dt in dates[::-1]:
    #         fill_url_spread(url, service, dt, email)


if __name__ == '__main__':
    load_dotenv()
    # db_session.global_init(os.getenv('DATABASE_URL'))
    main()
