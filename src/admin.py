import json
import os

from telegram import InlineKeyboardMarkup, InlineKeyboardButton, ParseMode, Update
from telegram.error import BadRequest

from data.general import start
from data.utils import delete_last_message, get_config, save_config, upload_img, delete_img


@delete_last_message
def show_data(_, context):
    cfg = get_config()
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton(text=key, callback_data=key)] for key in cfg.keys()] +
        [[InlineKeyboardButton(text='Вернуться назад', callback_data='menu')]])
    return (context.bot.send_message(context.user_data['id'], 'Выберите переменную', reply_markup=markup),
            'admin.data_requesting')


@delete_last_message
def request_changing_data(_, context):
    context.user_data['key_to_change'] = context.match.string
    current_item = get_config()[context.match.string]
    val = current_item['val']
    if isinstance(val, list):
        val = ';'.join(map(str, val))
    markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton(text='Вернуться назад', callback_data='data')]])
    if 'фото' not in context.match.string.lower() or not val:
        if current_item['can_be_list']:
            text = (f'На что вы хотите заменить <b>{context.match.string}</b>?\n'
                    f'\n<b>Текущее значение:</b> {val}'
                    f'\n<b>Может ли быть списком:</b> Да\n\n'
                    'Если это список, то введите элементы через ;')
        else:
            text = (f'На что вы хотите заменить <b>{context.match.string}</b>?\n'
                    f'\n<b>Текущее значение:</b> {val}'
                    f'\n<b>Может ли быть списком:</b> Нет')
        msg = context.bot.send_message(context.user_data['id'], text, reply_markup=markup,
                                       parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    else:
        try:
            msg = context.bot.send_photo(context.user_data['id'], val,
                                         f'На что вы хотите заменить <b>{context.match.string}</b>?'
                                         f'\n<b>Текущее значение:</b>', reply_markup=markup,
                                         parse_mode=ParseMode.HTML)
        except BadRequest:
            msg = context.bot.send_message(context.user_data['id'],
                                           f'На что вы хотите заменить <b>{context.match.string}</b>?'
                                           f'\n<b>Текущее значение: None</b>', reply_markup=markup,
                                           parse_mode=ParseMode.HTML)
    return msg, 'admin.data'


@delete_last_message
def change_data(update: Update, context):
    cfg = get_config()
    key = context.user_data['key_to_change']
    if key == 'Шаг пагинации':
        try:
            val = int(update.message.text)
            assert val > 0
            cfg[key]['val'] = val
        except (ValueError, AssertionError):
            update.message.reply_text('Неверный формат шага пагинации')
    else:
        if key == 'admins' and str(update.message.from_user.id) not in update.message.text.split(';'):
            update.message.reply_text('Вы не можете удалить сами себя из admins')
            return show_data(update, context)
        if 'фото' in key.lower():
            photo = update.message.photo
            if not photo:
                update.message.reply_text('Должно быть прикреплено фото')
                return request_changing_data(update, context)
            stream = (update.message.photo[-1].get_file().download_as_bytearray() if update.message.photo
                      else update.message.document.get_file().download_as_bytearray())
            try:
                prev_url = cfg[key]['val']
                url = upload_img(stream)
                if not url:
                    update.message.reply_text(
                        'Не удалось загрузить изображение. '
                        f'Возможно, был превышен лимит размера фотографии ({IMG_FILE_SIZE_LIMIT} МБ')
                    return request_changing_data(update, context)
                cfg[key]['val'] = url
                with open(os.path.join('data', 'config.json'), encoding='utf-8') as f:
                    default_cfg = json.loads(f.read())
                    if prev_url != default_cfg[key] and 'res.cloudinary.com' in prev_url:
                        delete_img(prev_url)
            except Exception as e:
                update.message.reply_text(f'Выпало следующее исключение: {str(e)}')
                return request_changing_data(update, context)
        else:
            val = update.message.text.strip().split(';')
            cfg[key]['val'] = val[0] if len(val) == 1 or not cfg[key]['can_be_list'] else [v.strip() for v in val]
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
                                    reply_markup=markup, ), 'admin.data_resetting'


@delete_last_message
def reset_data(update, context):
    if context.match and context.match.string == 'change_yes':
        with open(os.path.join('data', 'config.json'), encoding='utf-8') as f:
            cfg = get_config()
            admins = cfg['admins']
            data = json.loads(f.read())
            data['admins'] = admins
            save_config(data)
            context.bot.send_message(context.user_data['id'], 'Настройки были успешно сброшены')
    return start(update, context)