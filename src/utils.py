from gspread import service_account_from_dict


def get_spreadsheet(creds: dict, spread_id: str):
    account = service_account_from_dict(creds)
    return account.open_by_key(spread_id)