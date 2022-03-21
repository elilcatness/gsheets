import os
import json
import time
from datetime import date

from googleapiclient.errors import HttpError
from telegram import Update, ParseMode, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import CallbackContext
from gspread import service_account_from_dict

from src.utils import (delete_last_message, extract_urls, get_iso_dates_interval,
                       fill_url_spread, generate_csv, get_config, generate_timestamp)
from src.db import db_session
from src.db.models.state import State


def serve(context: CallbackContext):
    users_ids = []
    with db_session.create_session() as session:
        for state in session.query(State).all():
            context.bot.send_message(state.user_id, 'Работа началась')
            users_ids.append(state.user_id)
    config = get_config()
    creds = json.loads(config['Сервисный аккаунт (JSON)'])
    input_data, spread = extract_urls(config['URL таблицы'], creds, return_spread=True)
    for user_id in users_ids:
        context.bot.send_message(user_id, 'Считывание данных завершено.\n'
                                          f'Ссылок, отправленных в обработку: <b>{len(input_data)}</b>',
                                 parse_mode=ParseMode.HTML)
    context.job.context.user_data['messages'] = {user_id: None for user_id in users_ids}
    context.job.context.user_data['completed_count'] = 0
    context.job.context.user_data['total_count'] = len(input_data)
    context.job.context.user_data['k'] = 0
    new_data = []
    today = date.today().isoformat()
    service = service_account_from_dict(creds)
    email = config['Email']
    count = 0
    unshared_tables = []
    for i in range(len(input_data)):
        url, last_date = input_data[i]
        context.job.context.user_data['current_url'] = url
        context.job.context.user_data['current_date'] = ''
        if i == 0:
            for job in context.job_queue.get_jobs_by_name('visual_process'):
                job.schedule_removal()
            context.job_queue.run_repeating(process_status, 1, 0,
                                            context=context.job.context,
                                            name='visual_process')
        dates = get_iso_dates_interval(last_date, today)
        for dt in dates:
            context.job.context.user_data['current_date'] = dt
            print(url)
            try:
                k, unshared_sub_tables = fill_url_spread(context, url, service, dt, email, creds)
                count += k
                if unshared_sub_tables:
                    unshared_tables.extend(unshared_sub_tables)
                print()
            except HttpError as e:
                if e.status_code == 400:
                    print(f'\n[ERROR] {e}\n')
                    break
                raise e
        else:
            new_data.append({'URL': url, 'Last date': today})
        context.job.context.user_data['completed_count'] = i + 1
        context.job.context.user_data['step'] = 'Готово'
        context.job.context.user_data['k'] = 0
    context.job.context.user_data['step'] = 'Формирование CSV файла изменённой исходной таблицы'
    context.job.context.user_data['k'] = 0
    filename = generate_csv(new_data)
    context.job.context.user_data['step'] = 'Наполнение исходной таблицы изменёнными данными'
    context.job.context.user_data['k'] = 0
    with open(filename, encoding='utf-8') as f:
        service.import_csv(spread.id, f.read().encode('utf-8'))
    try:
        os.remove(filename)
    except Exception as e:
        print(f'[FILE DELETE] {e}')
    context.job.context.user_data['completed_count'] = len(input_data)
    context.job.context.user_data['step'] = 'Готово'
    context.job.context.user_data['k'] = 0
    time.sleep(1)
    for job in context.job_queue.get_jobs_by_name('visual_process'):
        job.schedule_removal()
    for key in 'messages', 'completed_count', 'total_count', 'k':
        if context.job.context.user_data.get(key):
            context.job.context.user_data.pop(key)
    with db_session.create_session() as session:
        for state in session.query(State).all():
            text = ('Работа была выполнена успешно!\n\n'
                    f'<b>Изначальное число URL:</b> {len(input_data)}\n'
                    f'<b>Из них обработано:</b> {len(new_data)}\n'
                    f'<b>Таблиц создано:</b> {count}')
            if unshared_tables:
                text += f'\n<b>Таблиц, не получивших права:</b> {len(unshared_tables)}'
                filename = f'{generate_timestamp()}.txt'
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(unshared_tables))
                with open(filename, 'rb') as f:
                    context.bot.send_document(state.user_id, f, filename, text, parse_mode=ParseMode.HTML)
                try:
                    os.remove(filename)
                except Exception as e:
                    print(f'[ERROR] Delete file: {e}')
            else:
                context.bot.send_message(state.user_id, text, parse_mode=ParseMode.HTML)


def process_status(context: CallbackContext):
    k = context.job.context.user_data['k']
    context.job.context.user_data['k'] = (k + 1) % 4
    text = (f'<b>Текущий URL:</b> {context.job.context.user_data["current_url"]}\n'
            f'<b>Текущая дата:</b> {context.job.context.user_data["current_date"]}\n\n'
            f'{context.job.context.user_data.get("step", "")}{"." * k}\n\n'
            f'<b>Прогресс:</b> '
            f'{context.job.context.user_data["completed_count"]}/{context.job.context.user_data["total_count"]}')
    for user_id, msg_id in context.job.context.user_data['messages'].items():
        if not msg_id:
            context.job.context.user_data['messages'][user_id] = context.bot.send_message(
                user_id, text, parse_mode=ParseMode.HTML).message_id
        else:
            context.bot.edit_message_text(text, user_id, msg_id, parse_mode=ParseMode.HTML)


@delete_last_message
def start(update: Update, context: CallbackContext):
    if not context.user_data.get('id') and update.message:
        context.user_data['id'] = update.message.from_user.id
    with db_session.create_session() as session:
        if not session.query(State).get(context.user_data['id']):
            session.add(State(user_id=context.user_data['id']))
            session.commit()
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton('Запустить работу вручную', callback_data='manual')],
         [InlineKeyboardButton('Настроить переменные', callback_data='admin')],
         [InlineKeyboardButton('Разблокировать таблицы', callback_data='unlock_spreads')]])
    return context.bot.send_message(context.user_data['id'], 'Меню', reply_markup=markup), 'menu'
