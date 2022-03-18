import json
import os
from dotenv import load_dotenv
from datetime import time, tzinfo

from telegram import Update
from telegram.ext import Updater, CallbackContext, CommandHandler

from src.db import db_session
from src.utils import extract_urls

from src.db.models.state import State
from src.constants import START_HOUR_UTC


def serve(context: CallbackContext):
    pass


def start(update: Update, context: CallbackContext):
    if not context.user_data['id'] and update.message:
        context.user_data['id'] = update.message.from_user.id
    with db_session.create_session() as session:
        if not session.query(State).get(context.user_data['id']):
            session.add(State(id=context.user_data['id']))
            session.commit()
    if not context.job_queue.get_jobs_by_name('serve'):
        tz = tzinfo('UTC')
        hour = START_HOUR_UTC if isinstance(START_HOUR_UTC, int) and (8 <= START_HOUR_UTC < 24) else 10
        t = time(hour=START_HOUR_UTC, tzinfo=tz)
        print(t)
        context.job_queue.run_daily(serve, t, context=context, name='serve')
        return context.bot.send_message(context.user_data['id'], f'Обработка начнётся в {hour}:00 (UTC)')
    return context.bot.send_message(context.user_data['id'], 'Обработка уже ведётся')


def stop(_, context: CallbackContext):
    for job in context.job_queue.get_jobs_by_name('serve'):
        job.schedule_removal()
    return context.bot.send_message(context.user_data['id'],
                                    'Работа была приостановлена, для перезапуска введите /start')


def main():
    updater = Updater(os.getenv('token'))
    updater.dispatcher.add_handler(CommandHandler())


if __name__ == '__main__':
    load_dotenv()
    db_session.global_init(os.getenv('DATABASE_URL'))
    main()