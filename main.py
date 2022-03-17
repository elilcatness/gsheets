import json
import os

from dotenv import load_dotenv

from src.utils import extract_urls


def main():
    with open('creds.json', encoding='utf-8') as f:
        creds = json.loads(f.read())
    spread_url = os.getenv('spread_url')
    if not spread_url:
        return print('spread_url var is missed in env')
    urls = extract_urls(spread_url, creds)


if __name__ == '__main__':
    load_dotenv()
    main()