import os
import time
import yaml
import datetime
import requests
import modules.db
import modules.logger

from logging import Logger
from requests import Response
from requests.structures import CaseInsensitiveDict


class Crawler:
    _CURRENT_DIRECTORY: str
    _CURRENT_DATETIME: datetime.datetime
    _CURRENT_DATE: datetime.datetime
    _CONFIG: dict
    _LOGGER: Logger
    _DB: modules.db.CrawlerDB

    def __init__(self, file):

        # Lets-a-go!

        self._CURRENT_DIRECTORY = os.path.abspath(os.path.dirname(file))
        self._CURRENT_DATETIME = datetime.datetime.now()
        self._CURRENT_DATE = Crawler.get_beginning_of_today()

        self._CONFIG = self.get_config()
        self._LOGGER = modules.logger.get_logger(
            os.path.basename(file),
            self._CONFIG,
            self._CURRENT_DIRECTORY
        )

        self._DB = modules.db.CrawlerDB(self._CONFIG)

    def get_config(self) -> dict:

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

        def process_parameter_telegram_bot_api_token():

            key = 'telegram_bot_api_token'

            if config.get(key) is None:
                config[key] = ''

        def process_parameter_telegram_chat_id():

            key = 'telegram_chat_id'

            if config.get(key) is None:
                config[key] = 0

        def process_parameter_api_endpoint_to_load_rates():

            key = 'api_endpoint_to_load_rates'
            value = config.get(key)

            if value is None or type(value) != str:
                config[key] = ''

        def process_parameter_currency_codes():

            key = 'currency_codes'
            value = config.get(key)

            if value is None or type(value) != dict:
                config[key] = {}

        config_filepath = os.path.join(self._CURRENT_DIRECTORY, 'config.yaml')

        config = get_yaml_data(config_filepath)

        process_parameter_number_of_days_to_check()
        process_parameter_number_of_days_to_add()

        process_parameter_currency_codes_filter()

        process_parameter_mongodb_connection_string()
        process_parameter_mongodb_database_name()
        process_parameter_mongodb_max_delay()

        process_parameter_telegram_bot_api_token()
        process_parameter_telegram_chat_id()

        process_parameter_api_endpoint_to_load_rates()

        process_parameter_currency_codes()

        return config

    def is_currency_code_allowed(self, currency_code: str) -> bool:

        result = True

        if len(self._CONFIG['currency_codes_filter']) > 0:
            result = currency_code in self._CONFIG['currency_codes_filter']

        return result

    def get_currency_code(self, currency_presentation: str) -> str:

        return self._CONFIG['currency_codes'].get(currency_presentation)

    def get_rate_date(self, source_date) -> datetime.datetime:

        date_delta = datetime.timedelta(days=self._CONFIG['number_of_days_to_add'])

        return datetime.datetime(source_date.year, source_date.month, source_date.day) + date_delta

    def unknown_currencies_warning(self, unknown_currencies):

        if len(unknown_currencies) > 0:
            unknown_currencies = list(set(unknown_currencies))

            currencies_string = ", ".join(unknown_currencies)

            self._LOGGER.warning(
                "Unknown currencies have been skipped: {}".format(currencies_string)
            )

    def changed_currency_rates_warning(self, changed_currency_rates):

        if len(changed_currency_rates) > 0:

            details = []

            for rate in changed_currency_rates:
                details.append(
                    "{} on {}".format(
                        rate['currency_code'],
                        self.get_date_as_string(rate['rate_date'])
                    )
                )

            details = ", ".join(details)

            self._LOGGER.warning(
                "Changed currency rates have been detected: {}".format(details)
            )

    def historical_currency_rates_warning(self, historical_currency_rates):

        if len(historical_currency_rates) > 0:

            details = []

            for rate in historical_currency_rates:
                details.append(
                    "{} on {}".format(
                        rate['currency_code'],
                        self.get_date_as_string(rate['rate_date'])
                    )
                )

            details = ", ".join(details)

            self._LOGGER.warning(
                "Historical currency rates have been added: {}".format(details)
            )

    @staticmethod
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

    @staticmethod
    def get_beginning_of_today() -> datetime:

        today = datetime.date.today()
        return Crawler.get_datetime_from_date(today)

    @staticmethod
    def get_datetime_from_date(date: datetime.date) -> datetime.datetime:

        return datetime.datetime(date.year, date.month, date.day)

    @staticmethod
    def get_date_as_string(date: datetime.datetime) -> str:

        return date.strftime('%Y-%m-%d')
