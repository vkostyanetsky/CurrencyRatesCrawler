import os
import time
import yaml
import datetime
import requests
import modules.db
import modules.logger

from itertools import groupby

from logging import Logger
from requests import Response
from requests.structures import CaseInsensitiveDict


class Crawler:
    _current_directory: str
    _current_datetime: datetime.datetime
    _current_date: datetime.datetime
    _config: dict
    _logger: Logger
    _db: modules.db.CrawlerDB

    _title: str = ''
    _user_interface_url: str = "https://www.centralbank.ae/en/fx-rates"

    _session: requests.sessions.Session = requests.session()

    def __init__(self, file):

        # Lets-a-go!

        self._current_directory = os.path.abspath(os.path.dirname(file))
        self._current_datetime = Crawler.get_beginning_of_this_second()
        self._current_date = Crawler.get_beginning_of_this_day()

        self._config = self.get_config()
        self._db = modules.db.CrawlerDB(self._config)

        self._logger = modules.logger.get_logger(
            os.path.basename(file),
            self._config,
            self._current_datetime,
            self._db
        )

        self._logger.debug("Crawler initialized.")

    @staticmethod
    def rate_value_presentation(value: float):
        return format(value, ".6f")

    def _process_currency_rates_to_import(self, currency_rates_to_import: list) -> tuple:

        self._logger.debug("Process obtained rates...")

        retroactive_rates = []
        changed_rates = []

        for currency_rate_to_import in currency_rates_to_import:

            rate_presentation = "{} on {} is {}".format(
                currency_rate_to_import["currency_code"],
                datetime.datetime.strftime(currency_rate_to_import["rate_date"], "%d-%m-%Y"),
                self.rate_value_presentation(currency_rate_to_import["rate"])
            )

            if not self._db.rate_is_new_or_changed(currency_rate_to_import):
                self._logger.debug("{}: skipped (already imported)".format(rate_presentation))
                continue

            currency_rate_on_date = self._db.currency_rate_on_date(
                currency_rate_to_import['currency_code'],
                currency_rate_to_import['rate_date']
            )

            if currency_rate_to_import['rate_date'] < self.get_rate_date(self._current_date):
                retroactive_rates.append((currency_rate_on_date, currency_rate_to_import))

            changed_rates.append((currency_rate_on_date, currency_rate_to_import))

            self._db.insert_currency_rate(currency_rate_to_import)
            self._logger.debug("{}: imported".format(rate_presentation))

        self._logger.debug("Obtained rates have been processed.")

        self._logger.debug(
            self.__description_of_rates_changed(
                len(changed_rates), len(retroactive_rates)
            )
        )

        self.__write_log_event_currency_rates_change_description("changed rates", changed_rates)
        self.__write_log_event_currency_rates_change_description("retroactive rates", retroactive_rates)

        return len(changed_rates), len(retroactive_rates)

    @staticmethod
    def __description_of_rates_changed(number_of_changed_rates: int, number_of_retroactive_rates: int) -> str:
        if number_of_changed_rates > 0:
            description = "Rates changed: {} (retroactively: {})".format(
                number_of_changed_rates,
                number_of_retroactive_rates
            )
        else:
            description = "No changes found."

        return description

    def get_import_date_as_string(self) -> str:
        return self._current_datetime.strftime('%Y%m%d%H%M%S')

    def get_config_value(self, key: str) -> any:
        return self._config.get(key)

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

        def process_parameter_user_agent():

            key = 'user_agent'

            if config.get(key) is None:
                config[key] = ''

        def process_parameter_api_endpoint_to_get_logs():

            key = 'api_endpoint_to_get_logs'
            value = config.get(key)

            if value is None or type(value) != str:
                config[key] = ''

        def process_parameter_big_ip_cookies():

            key = 'big_ip_cookies'
            value = config.get(key)

            if value is None or type(value) != list:
                config[key] = []

        def process_parameter_currency_codes():

            key = 'currency_codes'
            value = config.get(key)

            if value is None or type(value) != dict:
                config[key] = {}

        config_filepath = os.path.join(self._current_directory, 'config.yaml')

        config = get_yaml_data(config_filepath)

        process_parameter_number_of_days_to_check()
        process_parameter_number_of_days_to_add()

        process_parameter_currency_codes_filter()

        process_parameter_mongodb_connection_string()
        process_parameter_mongodb_database_name()
        process_parameter_mongodb_max_delay()

        process_parameter_telegram_bot_api_token()
        process_parameter_telegram_chat_id()

        process_parameter_user_agent()

        process_parameter_api_endpoint_to_get_logs()

        process_parameter_big_ip_cookies()

        process_parameter_currency_codes()

        return config

    def is_currency_code_allowed(self, currency_code: str) -> bool:

        result = True

        if len(self._config['currency_codes_filter']) > 0:
            result = currency_code in self._config['currency_codes_filter']

        return result

    def get_currency_code(self, currency_presentation: str) -> str:

        return self._config['currency_codes'].get(currency_presentation)

    def get_rate_date(self, source_date) -> datetime.datetime:

        date_delta = datetime.timedelta(days=self._config['number_of_days_to_add'])

        return datetime.datetime(source_date.year, source_date.month, source_date.day) + date_delta

    def unknown_currencies_warning(self, unknown_currencies):

        if len(unknown_currencies) > 0:
            unknown_currencies = list(set(unknown_currencies))

            currencies_string = ", ".join(unknown_currencies)

            self._logger.warning(
                "Unknown currencies have been skipped: {}".format(currencies_string)
            )

    def get_response_for_request(self, request_url: str) -> Response:

        def get_request_headers() -> CaseInsensitiveDict:
            headers = CaseInsensitiveDict()
            headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            headers['Pragma'] = 'no-cache'
            headers['Expires'] = '0'

            if self._config['user_agent'] != '':
                headers['User-Agent'] = self._config['user_agent']

            return headers

        time.sleep(1)

        request_headers = get_request_headers()

        response = self._session.get(request_url, headers=request_headers)

        self._logger.debug(
            "Response received. Status code: {}, text:\n{}".format(response.status_code, response.text)
        )

        return response

    def get_current_date_presentation(self) -> str:
        return self.get_date_as_string(self._current_date)

    @staticmethod
    def get_beginning_of_this_second() -> datetime.datetime:

        now = datetime.datetime.now()

        return now - datetime.timedelta(microseconds=now.microsecond)

    @staticmethod
    def get_beginning_of_this_day() -> datetime:

        today = datetime.date.today()
        return Crawler.get_datetime_from_date(today)

    @staticmethod
    def get_datetime_from_date(date: datetime.date) -> datetime.datetime:

        return datetime.datetime(date.year, date.month, date.day)

    @staticmethod
    def get_date_as_string(date: datetime.datetime) -> str:

        return date.strftime("%Y-%m-%d")

    @staticmethod
    def get_time_as_string(date: datetime.datetime) -> str:

        return date.strftime("%H:%M:%S")

    def _write_log_event_import_started(self) -> None:
        time_as_string = self.get_time_as_string(self._current_datetime)
        import_date_as_string = self.get_import_date_as_string()

        message = "{} started at {} ({}).".format(self._title, time_as_string, import_date_as_string)

        self._logger.debug(message)

    def _write_log_event_import_completed(self, number_of_changed_rates, number_of_retroactive_rates) -> None:
        self._logger.info("{} started at {} ({}) is completed. {}".format(
            self._title,
            self.get_time_as_string(self._current_datetime),
            self.get_import_date_as_string(),
            self.__description_of_rates_changed(number_of_changed_rates, number_of_retroactive_rates)
        ))

    def __write_log_event_currency_rates_change_description(self, title: str, rates: list) -> None:

        if len(rates) == 0:
            return

        for group in groupby(sorted(rates, key=lambda x: x[0]['rate_date']), key=lambda x: x[0]['rate_date']):
            presentations = []

            for group_item in group[1]:
                presentation = "{} ({} → {})".format(
                    group_item[1]['currency_code'],
                    self.rate_value_presentation(group_item[0]['rate']),
                    self.rate_value_presentation(group_item[1]['rate']),
                )
                presentations.append(presentation)

            self._logger.warning(
                "Summary of {} on {}: {}".format(
                    title,
                    self.get_date_as_string(group[0]),
                    ", ".join(presentations)
                )
            )

    @staticmethod
    def date_with_time_as_string(date_with_time: datetime.datetime) -> str:
        return date_with_time.strftime('%Y-%m-%d %H:%M:%S')
