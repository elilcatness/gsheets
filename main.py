import json

from src.utils import get_spreadsheet
import gspread


def main():
    # spread_url = input('Вставьте ссылку на таблицу: ').strip()
    spread_url = 'https://docs.google.com/spreadsheets/d/1Yao_N7aX7MVluTHOUQm4qIkho2TYmbWc64DGT1L2gJ0/edit#gid=0'
    if (not spread_url.startswith('http://docs.google.com/spreadsheets/d/') and
        not spread_url.startswith('https://docs.google.com/spreadsheets/d/')):
        return print('Введён неверный URL таблицы')
    sub = spread_url.split('/')
    try:
        spread_id = sub[sub.index('d') + 1]
    except IndexError:
        return print('В URL отсутствует ID таблицы')
    print('spread_id: %s' % spread_id)
    with open('creds.json', encoding='utf-8') as f:
        creds = json.loads(f.read())
    spread = get_spreadsheet(creds, spread_id)
    sheet = spread.worksheets()[0]
    print(sheet.cell(2, 1))


if __name__ == '__main__':
    main()