import json
import os
from datetime import time
import time as _time

from gspread import service_account_from_dict
from pytz import UTC
from telegram import Update, ParseMode, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (Updater, CallbackContext, CommandHandler, ConversationHandler,
                          CallbackQueryHandler, MessageHandler, Filters)
from telegram.error import Unauthorized

from src.admin import show_data, reset_data, request_changing_data, change_data, ask_resetting_data
from src.db import db_session
from src.db.models.state import State
from src.general import serve, start
from src.utils import get_config, share_spread, generate_timestamp, delete_last_message


def start_jobs(dispatcher, bot):
    context = CallbackContext(dispatcher)
    context._bot = bot
    t = time(hour=get_config()['Час запуска (UTC)'], tzinfo=UTC)
    context.job_queue.run_daily(serve, t, context=context, name='serve')


def manual_start(_, context: CallbackContext):
    jobs = context.job_queue.get_jobs_by_name('serve')
    if any(job.enabled for job in jobs):
        return context.bot.send_message(context.user_data['id'], 'Работа уже ведётся')
    try:
        context.job_queue.run_once(serve, 0, context=context, name='serve')
    except TypeError as e:
        print(f'[WARNING] {e}')


def load_states(updater: Updater, conv_handler: ConversationHandler):
    with db_session.create_session() as session:
        for state in session.query(State).all():
            conv_handler._conversations[(state.user_id, state.user_id)] = state.callback
            updater.dispatcher.user_data[state.user_id] = json.loads(state.data)


def error_handler(_, context: CallbackContext):
    e = context.error
    with db_session.create_session() as session:
        for state in session.query(State).all():
            try:
                context.bot.send_message(state.user_id, f'An exception occurred!\n\n'
                                                        f'{e.__class__}: {e}\n')
            except Unauthorized as e:
                print(f'[{state.user_id}]: {e}')
    for job in context.job_queue.get_jobs_by_name('visual_process'):
        job.schedule_removal()


@delete_last_message
def ask_spreads(_, context: CallbackContext):
    markup = InlineKeyboardMarkup([[InlineKeyboardButton('Вернуться назад', callback_data='back')]])
    context.bot.send_message(context.user_data['id'],
                             'Отправьте список URL таблиц для разблокировки (каждая на новой строке)\n'
                             'Или прикрепите текстовый файл (.txt) в таком же формате',
                             reply_markup=markup)
    return 'ask_spreads'


def unlock_spreads(update: Update, context: CallbackContext):
    if update.message.document:
        spreads_urls = [x.strip() for x
                        in update.message.document.get_file().download_as_bytearray().decode('utf-8').split('\n')]
    else:
        spreads_urls = [x.strip() for x in update.message.text.split('\n')]
    cfg = get_config()
    creds = json.loads(cfg['Сервисный аккаунт (JSON)'])
    service = service_account_from_dict(creds)
    unshared_tables = []
    # msg = update.message.reply_text(f'Начинаем разблокировку. <b>Количество таблиц:</b> {len(spreads_urls)}',
    #                                 parse_mode=ParseMode.HTML)
    # template = (f'<b>Текущая таблица</b>: %s'
    #             f'\n<b>Таблиц обработано:</b> %d/{len(spreads_urls)}'
    #             f'\n<b>Таблиц разблокировано:</b> %d/{len(spreads_urls)}')
    for i, spread_url in enumerate(spreads_urls):
        # msg.edit_text(template % (spread_url, i, len()))
        spread = service.open_by_url(spread_url)
        # try:
        #     spread = service.open_by_url(spread_url)
        # except APIError as e:
        #     reason = get_api_error_reason(e)
        #     print(f'{reason=}')
        #     if not reason:
        #         raise e
        #     if reason == 'RATE_LIMIT_EXCEEDED':
        #         _time.sleep(61)
        #         try:
        #             spread = service.open_by_url(spread_url)
        #         except APIError:
        #             continue
        #     else:
        #         raise e
        if share_spread(spread, cfg['Email'], 'user', 'owner'):
            unshared_tables.append(spread.url)
        _time.sleep(1.5)
    text = (f'<b>Таблиц разблокировано:</b> <b>{len(spreads_urls) - len(unshared_tables)}</b> '
            f'из <b>{len(spreads_urls)}</b>')
    if unshared_tables:
        filename = f'{generate_timestamp()}.txt'
        with open(filename, 'w', encoding='utf-8') as f:
            f.write('\n'.join(unshared_tables))
        with open(filename, 'rb') as f:
            try:
                context.bot.send_document(context.user_data['id'], f, filename, text,
                                          parse_mode=ParseMode.HTML)
            except Unauthorized as e:
                print(f'[{context.user_data["id"]}]: {e}')
        try:
            os.remove(filename)
        except Exception as e:
            print(f'[ERROR] On delete: {e}')
    else:
        context.bot.send_message(context.user_data['id'], text, parse_mode=ParseMode.HTML)
    return start(update, context)


def main():
    updater = Updater(os.getenv('token'))
    conv_handler = ConversationHandler(
        allow_reentry=True,
        per_message=False,
        entry_points=[CommandHandler('start', start)],
        states={'menu': [CallbackQueryHandler(manual_start, pattern='manual'),
                         CallbackQueryHandler(show_data, pattern='admin'),
                         CallbackQueryHandler(ask_spreads, pattern='unlock_spreads')],
                'data': [CallbackQueryHandler(show_data, pattern='data'),
                         MessageHandler((~Filters.text('Вернуться назад')) & Filters.text, change_data)],
                'data_resetting': [CallbackQueryHandler(reset_data, pattern='change_yes'),
                                   CallbackQueryHandler(start, pattern='change_no')],
                'data_requesting': [CallbackQueryHandler(start, pattern='menu'),
                                    CallbackQueryHandler(request_changing_data, pattern=''),
                                    CallbackQueryHandler(ask_resetting_data, pattern='ask')],
                'ask_spreads': [MessageHandler(Filters.text | Filters.document, unlock_spreads),
                                CallbackQueryHandler(start, pattern='back')]},
        fallbacks=[CommandHandler('start', start)])
    updater.dispatcher.add_handler(conv_handler)
    # updater.dispatcher.add_error_handler(error_handler)
    load_states(updater, conv_handler)
    start_jobs(updater.dispatcher, updater.bot)
    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    db_session.global_init(os.getenv('DATABASE_URL'))
    main()

