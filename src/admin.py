import json
import os

from telegram import InlineKeyboardMarkup, InlineKeyboardButton, ParseMode, Update

from src.general import start
from src.utils import delete_last_message, get_config, save_config


@delete_last_message
def show_data(_, context):
    cfg = get_config()
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton(text=key, callback_data=key)] for key in cfg.keys()] +
        [[InlineKeyboardButton('Сбросить настройки до серверных', callback_data='ask')]] +
        [[InlineKeyboardButton(text='Вернуться назад', callback_data='menu')]])
    return (context.bot.send_message(context.user_data['id'], 'Выберите переменную', reply_markup=markup),
            'data_requesting')


@delete_last_message
def request_changing_data(_, context):
    context.user_data['key_to_change'] = context.match.string
    val = get_config()[context.match.string]
    if isinstance(val, list):
        val = ';'.join(map(str, val))
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton(text='Вернуться назад', callback_data='data')]])
    text = (f'На что вы хотите заменить <b>{context.match.string}</b>?\n'
            f'\n<b>Текущее значение:</b> {val}')
    msg = context.bot.send_message(context.user_data['id'], text, reply_markup=markup,
                                   parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    return msg, 'data'


@delete_last_message
def change_data(update: Update, context):
    cfg = get_config()
    key = context.user_data['key_to_change']
    if key == 'Час запуска (UTC)':
        try:
            val = int(update.message.text)
            assert 8 <= val < 24
        except (ValueError, AssertionError):
            update.message.reply_text('Час должен быть в виде натурального числа от 8 до 23 включительно')
            return show_data(update, context)
    else:
        val = update.message.text
    cfg[key] = val
    save_config(cfg)
    update.message.reply_text(f'Переменная <b>{context.user_data["key_to_change"]}</b> была обновлена',
                              parse_mode=ParseMode.HTML)
    return show_data(update, context)


@delete_last_message
def ask_resetting_data(_, context):
    markup = InlineKeyboardMarkup([[InlineKeyboardButton('Да', callback_data='change_yes')],
                                   [InlineKeyboardButton('Нет', callback_data='change_no')]])
    return context.bot.send_message(context.user_data['id'],
                                    'Вы уверены, что хотите сбросить настройки до серверных?',
                                    reply_markup=markup, ), 'data_resetting'


@delete_last_message
def reset_data(update, context):
    if context.match and context.match.string == 'change_yes':
        with open(os.path.join('data', 'config.json'), encoding='utf-8') as f:
            data = json.loads(f.read())
            save_config(data)
            context.bot.send_message(context.user_data['id'], 'Настройки были успешно сброшены')
    return start(update, context)