import os
import yaml
import time
import platform
import datetime

import requests
from requests import Response
from requests.structures import CaseInsensitiveDict


def get_date_format_string():

    return '%#d-%#m-%Y' if platform.system() == 'Windows' else '%-d-%-m-%Y'


def get_date_as_string(date: datetime.datetime) -> str:

    return date.strftime('%Y-%m-%d')


def get_current_date() -> datetime.datetime:

    today = datetime.date.today()
    return get_datetime_from_date(today)


def get_current_datetime() -> datetime.datetime:

    return datetime.datetime.now()


def get_currency_code(currency_presentation: str, config) -> str:

    result = config['currency_codes'].get(currency_presentation)

    if result is None:
        # TODO Telegram alert required
        print('Unknown currency discovered: {}' .format(currency_presentation))

    return result


def get_config(directory) -> dict:

    def get_yaml_data(yaml_filepath) -> dict:

        try:

            with open(yaml_filepath, encoding='utf-8-sig') as yaml_file:
                yaml_data = yaml.safe_load(yaml_file)

        except EnvironmentError:

            yaml_data = []

        return yaml_data

    def process_parameter_number_of_days_to_check():

        key = 'number_of_days_to_check'

        if config.get(key) is None:
            config[key] = 14

    def process_parameter_number_of_days_to_add():

        key = 'number_of_days_to_add'

        if config.get(key) is None:
            config[key] = 1

    def process_parameter_currency_codes_filter():

        key = 'currency_codes_filter'
        value = config.get(key)

        if value is None or type(value) != list:
            config[key] = []

    def process_parameter_mongodb_connection_string():

        key = 'mongodb_connection_string'

        if config.get(key) is None:
            config[key] = 'mongodb://localhost:27017'

    def process_parameter_mongodb_database_name():

        key = 'mongodb_database_name'

        if config.get(key) is None:
            config[key] = 'uae_currency_rates'

    def process_parameter_mongodb_max_delay():

        key = 'mongodb_max_delay'

        if config.get(key) is None:
            config[key] = 5

    def process_parameter_telegram_bot_token():

        key = 'telegram_bot_token'

        if config.get(key) is None:
            config[key] = ''

    def process_parameter_telegram_chat_id():

        key = 'telegram_chat_id'

        if config.get(key) is None:
            config[key] = 0

    def process_parameter_currency_codes():

        key = 'currency_codes'
        value = config.get(key)

        if value is None or type(value) != dict:
            config[key] = {}

    config_filepath = os.path.join(directory, 'config.yaml')

    config = get_yaml_data(config_filepath)

    process_parameter_number_of_days_to_check()
    process_parameter_number_of_days_to_add()

    process_parameter_currency_codes_filter()

    process_parameter_mongodb_connection_string()
    process_parameter_mongodb_database_name()
    process_parameter_mongodb_max_delay()

    process_parameter_telegram_bot_token()
    process_parameter_telegram_chat_id()

    process_parameter_currency_codes()

    return config


def get_response_for_request(request_url: str) -> Response:

    def get_request_headers() -> CaseInsensitiveDict:

        headers = CaseInsensitiveDict()
        headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        headers['Pragma'] = 'no-cache'
        headers['Expires'] = '0'

        return headers

    time.sleep(1)

    request_headers = get_request_headers()

    return requests.get(request_url, headers=request_headers)


def get_datetime_from_date(date: datetime.date) -> datetime.datetime:

    return datetime.datetime(date.year, date.month, date.day)


def is_currency_code_allowed(currency_code: str, config: dict) -> bool:

    result = True

    if len(config['currency_codes_filter']) > 0:
        result = currency_code in config['currency_codes_filter']

    return result
