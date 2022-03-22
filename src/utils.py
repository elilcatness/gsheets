import json
import os
from datetime import date, timedelta
import time
from csv import DictWriter

from typing import Union

# noinspection PyUnresolvedReferences
from apiclient.discovery import build
from gspread import service_account_from_dict, Client
from gspread.exceptions import APIError
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from telegram.error import BadRequest
from telegram.ext import CallbackContext

from src.constants import MAX_ROWS_COUNT, API_ROWS_LIMIT

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
            with open(os.path.join('src', 'config.json'), encoding='utf-8') as f:
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


def extract_urls(spread_url: str, creds: dict, return_spread: bool = False):
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
    return ([list(x) for x in zip(urls, dates)] if not return_spread else
            [list(x) for x in zip(urls, dates)], spread)


def get_console(creds: dict):
    scope = 'https://www.googleapis.com/auth/webmasters.readonly'
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds, scopes=[scope])
    http = credentials.authorize(Http())
    return build('searchconsole', 'v1', http=http)


def _execute_request(service, url: str, request: dict):
    return service.searchanalytics().query(siteUrl=url, body=request).execute()


def process_url(url: str, dt: str, creds: dict) -> list[dict]:
    service = get_console(creds)
    print(f'{dt=}')
    headers = ['page', 'query', 'device', 'country']
    start_row = 0
    raw_output, current_rows = [], []
    while len(current_rows) == API_ROWS_LIMIT or start_row == 0:
        request = {'startDate': dt,
                   'endDate': dt,
                   'dimensions': headers,
                   'rowLimit': API_ROWS_LIMIT,
                   'startRow': start_row}
        current_rows = _execute_request(service, url, request).get('rows', [])
        raw_output.extend(current_rows)
        start_row += API_ROWS_LIMIT
    output = []
    for row in raw_output:
        keys = row.pop('keys')
        data = {headers[i]: keys[i] for i in range(len(keys))}
        output.append({**data, **row})
    return output


def generate_csv(data: list[dict]) -> str:
    filename = f'{generate_timestamp()}.csv'
    headers = list(data[0].keys())
    with open(filename, 'w', newline='', encoding='utf-8') as csv_file:
        writer = DictWriter(csv_file, headers, delimiter=',')
        writer.writeheader()
        writer.writerows(data)
    return filename


def process_table(context: CallbackContext, data: list[dict], service, table_name: str, dt: str, email: str):
    context.job.context.bot_data['step'] = 'Формирование CSV файла'
    context.job.context.bot_data['k'] = 0
    filename = generate_csv(data)
    context.job.context.bot_data['step'] = 'Создание таблицы Google'
    context.job.context.bot_data['k'] = 0
    spread = service.create(table_name)
    service: Client
    print(f'{spread=}')
    context.job.context.bot_data['step'] = 'Наполнение таблицы данными из CSV файла'
    context.job.context.bot_data['k'] = 0
    with open(filename, encoding='utf-8') as f:
        service.import_csv(spread.id, f.read().encode('utf-8'))
    spread.worksheets()[0].update_title(dt)
    unshared_tables = []
    context.job.context.bot_data['step'] = f'Передача таблицы пользователю {email}'
    context.job.context.bot_data['k'] = 0
    if not share_spread(spread, email, 'user', 'owner'):
        unshared_tables.append(spread.url)
    try:
        os.remove(filename)
    except Exception as e:
        print(f'[FILE DELETE] {e}')
    return unshared_tables


def share_spread(spread, *args, **kwargs):
    try:
        spread.share(*args, **kwargs)
    except APIError as e:
        errors = e.args[0].get('errors', [])
        if not errors:
            raise e
        if errors[0].get('reason', '' == 'userRateLimitExceeded'):
            print(f'[ERROR] {e.response}: {e} on {spread}')
        return False
    return True


def fill_url_spread(context: CallbackContext, url: str, service: Client, dt: str, email: str, creds: dict):
    context.job.context.bot_data['step'] = 'Получение данных из API'
    context.job.context.bot_data['k'] = 0
    data = process_url(url, dt, creds)
    unshared_tables = []
    if not data:
        print(f'[WARNING] Empty data for {url} on {dt}')
        return 0, unshared_tables
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
        unshared_tables.extend(process_table(context, sub_data, service, sub_table_name, dt, email))
        n += 1
    return n - 1, unshared_tables


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


def generate_timestamp():
    return ''.join(str(time.time()).split('.'))
