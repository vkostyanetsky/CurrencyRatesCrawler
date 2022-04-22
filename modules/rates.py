import datetime


def get_rate_date(update_date, config) -> datetime.datetime:

    date_delta = datetime.timedelta(days=config['number_of_days_to_add'])

    return datetime.datetime(update_date.year, update_date.month, update_date.day) + date_delta


def get_currency_code(currency_presentation: str, config) -> str:

    result = config['currency_codes'].get(currency_presentation)

    if result is None:
        # TODO Telegram alert required
        print('Unknown currency discovered: {}' .format(currency_presentation))

    return result


def is_currency_code_allowed(currency_code: str, config: dict) -> bool:

    result = True

    if len(config['currency_codes_filter']) > 0:
        result = currency_code in config['currency_codes_filter']

    return result
