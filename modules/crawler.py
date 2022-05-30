import os
import time
import yaml
import pickle
import datetime
import requests
import modules.db
import modules.logger

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

    __session_file_path: str = ""
    __session: requests.sessions.Session = None

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

    def _load_session(self) -> None:

        self.__session = requests.session()

        try:

            self.__session_file_path = os.path.join(self._current_directory, "session.bin")

            with open(self.__session_file_path, 'rb') as file:
                self.__session.cookies.update(pickle.load(file))

            for cookie in self.__session.cookies:
                self._logger.debug("Cookie restored: " + cookie.name + " = " + cookie.value)

        except FileNotFoundError:

            self._logger.warning(
                f"The session dump file ({self.__session_file_path}) is not found."
            )

        except OSError:

            self._logger.warning(
                f"OS error occurred trying to open session dump file ({self.__session_file_path})."
            )

        except Exception as exception:

            self._logger.warning(
                f"Unexpected error opening session dump file ({self.__session_file_path}): ", repr(exception)
            )

    def _save_session(self) -> None:

        with open(self.__session_file_path, 'wb') as file:
            pickle.dump(self.__session.cookies, file)

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

        def process_parameter_api_endpoint_to_get_logs():

            key = 'api_endpoint_to_get_logs'
            value = config.get(key)

            if value is None or type(value) != str:
                config[key] = ''

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

        process_parameter_api_endpoint_to_get_logs()

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

            self._logger.warning(
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

            self._logger.warning(
                "Historical currency rates have been added: {}".format(details)
            )

    def get_response_for_request(self, request_url: str) -> Response:

        def get_request_headers() -> CaseInsensitiveDict:
            headers = CaseInsensitiveDict()
            headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            headers['Pragma'] = 'no-cache'
            headers['Expires'] = '0'
            headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/100.0' # TODO

            return headers

        time.sleep(1)

        request_headers = get_request_headers()

        response = self.__session.get(request_url, headers=request_headers)

        self._save_session()

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

    def _write_import_started_log_event(self) -> None:
        time_as_string = self.get_time_as_string(self._current_datetime)
        import_date_as_string = self.get_import_date_as_string()

        message = "{} started at {} ({}).".format(self._title, time_as_string, import_date_as_string)

        self._logger.debug(message)

    def _write_import_completed_log_event(self, number_of_added_rates) -> None:
        time_as_string = self.get_time_as_string(self._current_datetime)
        import_date_as_string = self.get_import_date_as_string()

        final_message = "{} started at {} ({}) is completed.".format(self._title, time_as_string, import_date_as_string)

        if number_of_added_rates > 0:
            final_message_suffix = "Number of imported rates: {}.".format(number_of_added_rates)
        else:
            final_message_suffix = "No changes found."

        message = "{} {}".format(final_message, final_message_suffix)

        self._logger.debug(message)

    @staticmethod
    def date_with_time_as_string(date_with_time: datetime.datetime) -> str:
        return date_with_time.strftime('%Y-%m-%d %H:%M:%S')
