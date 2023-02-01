#!/usr/bin/env python3

"""
Basic crawler class. Contains core functionality for a working prototype:
- functions to work with currencies
- functions to make HTTP requests
- functions to write various logs
- configuration loading
- database connection
- logger instance
"""

import datetime
import logging
import logging.config
import os
from itertools import groupby

import requests
import yaml
from requests import Response
from requests.structures import CaseInsensitiveDict

from modules.db import Event, UAExchangeRatesCrawlerDB


class UAExchangeRatesCrawler:
    _current_directory: str
    _current_datetime: datetime.datetime
    _current_date: datetime.datetime
    _config: dict
    _db: UAExchangeRatesCrawlerDB
    _session: requests.sessions.Session = requests.session()
    _updating_event: Event

    def __init__(self, file, updating_event: Event) -> None:
        self._current_directory = os.path.abspath(os.path.dirname(file))
        self._current_datetime = UAExchangeRatesCrawler.get_beginning_of_this_second()
        self._current_date = UAExchangeRatesCrawler._get_beginning_of_this_day()

        self._config = self._get_config()
        self._db = UAExchangeRatesCrawlerDB(self._config)

        self.setup_logging(file)

        self._updating_event = updating_event

        self.review_currency_codes()

        logging.debug("Crawler initialized.")

    def send_to_telegram_chat(self, text: str) -> None:

        bot_api_token = self._config.get("telegram_bot_api_token")
        chat_id = self._config.get("telegram_chat_id")

        try:

            url = f"https://api.telegram.org/bot{bot_api_token}/sendMessage"

            data = {
                "parse_mode": "HTML",
                "chat_id": chat_id,
                "text": text,
            }

            requests.post(url, params=data)

        except Exception as error:
            print(error)
            print("Error while sending a message to Telegram.")

    def setup_logging(self, file: str) -> None:
        """
        Applies logging configuration (or basic one in case of failure).

        :return: nothing
        """

        current_file = os.path.split(file)[1]

        if current_file == "load_current.py":
            logging_config_name = "load_current_logging"
        elif current_file == "load_history.py":
            logging_config_name = "load_history_logging"
        elif current_file == "api.py":
            logging_config_name = "api_logging"
        else:
            logging_config_name = None

        if logging_config_name is not None:

            try:

                logging.config.dictConfig(self._config.get(logging_config_name))

            except Exception as error:

                print(error)
                print(
                    "Error in logging configuration. Basic configuration will be applied."
                )

                logging.basicConfig(level=logging.DEBUG)

    @staticmethod
    def get_beginning_of_this_second() -> datetime.datetime:

        now = datetime.datetime.now()

        return now - datetime.timedelta(microseconds=now.microsecond)

    @staticmethod
    def _get_beginning_of_this_day() -> datetime.datetime:

        today = datetime.date.today()
        return UAExchangeRatesCrawler.get_datetime_from_date(today)

    def get_import_date_as_string(self) -> str:
        return self._current_datetime.strftime("%Y%m%d%H%M%S")

    def _import_started(self, title: str, event: Event) -> None:
        time = self.get_time_as_string(self._current_datetime)
        import_date = self.get_import_date_as_string()

        message = f"{title.capitalize()} started at {time} ({import_date})."

        logging.debug(message)

        self._db.insert_event_rates_loading(event)

    def _log_import_failed(self, title: str):

        event_title = title.capitalize()
        event_datetime = self.get_time_as_string(self._current_datetime)
        event_import_date = self.get_import_date_as_string()

        logs_url = self._get_logs_url(event_import_date)

        if logs_url != "":
            logging.info(
                '{} started at {} (<a href="{}">{}</a>) is failed.'.format(
                    event_title, event_datetime, logs_url, event_import_date
                )
            )
        else:
            logging.info(
                "{} started at {} ({}) is failed.".format(
                    event_title, event_datetime, event_import_date
                )
            )

    def _log_import_completed(self, title: str, changed_rates_number: int) -> None:

        event_title = title.capitalize()
        event_datetime = self.get_time_as_string(self._current_datetime)
        event_description = self._description_of_rates_changed(changed_rates_number)

        logging.info(
            f"{event_title} started at {event_datetime} is completed. {event_description}"
        )

    def _get_config(self) -> dict:
        def get_yaml_data(yaml_filepath: str) -> dict:

            try:

                with open(yaml_filepath, encoding="utf-8-sig") as yaml_file:
                    yaml_data = yaml.safe_load(yaml_file)

            except EnvironmentError:

                yaml_data = []

            return yaml_data

        def check_parameter(
            parameter_key: str,
            parameter_type: type,
            default_value: int | str | list | dict,
        ):

            value = config.get(parameter_key)

            if type(value) != parameter_type:
                config[parameter_key] = default_value

        config_filepath = os.path.join(self._current_directory, "config.yaml")

        config = get_yaml_data(config_filepath)

        check_parameter("currency_codes_filter", list, [])
        check_parameter("mongodb_connection_string", str, "mongodb://localhost:27017")
        check_parameter("mongodb_database_name", str, "uae_currency_rates")
        check_parameter("mongodb_max_delay", int, 5)
        check_parameter("telegram_bot_api_token", str, "")
        check_parameter("telegram_chat_id", int, 0)
        check_parameter("api_url", str, "")
        check_parameter("api_endpoint_to_get_logs", str, "")
        check_parameter("user_agent", str, "")
        check_parameter("currency_codes", dict, {})

        return config

    def _get_logs_url(self, import_date: str):
        if (
            self._config["api_url"] == ""
            or self._config["api_endpoint_to_get_logs"] == ""
        ):
            return ""

        return "{}/{}/{}/".format(
            self._config["api_url"],
            self._config["api_endpoint_to_get_logs"],
            import_date,
        )

    @staticmethod
    def rate_value_presentation(value: float) -> str:
        return format(value, ".6f")

    def _process_currency_rates_to_import(self, currency_rates_to_import: list) -> int:

        logging.debug("Process obtained rates...")

        changed_rates = []

        for currency_rate_to_import in currency_rates_to_import:

            rate_presentation = "{} on {} is {}".format(
                currency_rate_to_import["currency_code"],
                datetime.datetime.strftime(
                    currency_rate_to_import["rate_date"], "%d-%m-%Y"
                ),
                self.rate_value_presentation(currency_rate_to_import["rate"]),
            )

            if not self._db.rate_is_new_or_changed(currency_rate_to_import):
                logging.debug(
                    "{}: skipped (already imported)".format(rate_presentation)
                )
                continue

            currency_rate_on_date = self._db.currency_rate_on_date(
                currency_rate_to_import["currency_code"],
                currency_rate_to_import["rate_date"],
            )

            changed_rates.append((currency_rate_on_date, currency_rate_to_import))

            self._db.insert_currency_rate(currency_rate_to_import)
            logging.debug("{}: imported".format(rate_presentation))

        logging.debug("Obtained rates have been processed.")

        logging.debug(self._description_of_rates_changed(len(changed_rates)))

        self._write_log_event_currency_rates_change_description(changed_rates)

        return len(changed_rates)

    @staticmethod
    def _description_of_rates_changed(changed_rates_number: int) -> str:
        return f"Number of changed rates: {changed_rates_number}."

    def get_config_value(self, key: str) -> any:
        return self._config.get(key)

    def _is_currency_code_allowed(self, currency_code: str) -> bool:

        result = True

        if len(self._config["currency_codes_filter"]) > 0:
            result = currency_code in self._config["currency_codes_filter"]

        return result

    def get_currency_code(self, currency_presentation: str) -> str:

        return self._config["currency_codes"].get(currency_presentation)

    def review_currency_codes(self) -> None:
        """
        Outputs information about currency codes & their presentations.
        """

        codes = {}

        for currency_presentation in self._config["currency_codes"]:

            currency_code = self._config["currency_codes"][currency_presentation]

            if codes.get(currency_code) is None:
                codes[currency_code] = []

            codes[currency_code].append(currency_presentation)

        logging.debug(f"{len(codes)} currency codes discovered:")

        for code in codes:

            currency_presentations = ", ".join(codes[code])

            logging.debug(f"- {code} ({currency_presentations})")

    def _unknown_currencies_warning(self, unknown_currencies: list) -> None:

        if len(unknown_currencies) > 0:
            unknown_currencies = list(set(unknown_currencies))

            currencies_string = ", ".join(unknown_currencies)

            logging.warning(
                "Unknown currencies have been skipped: {}".format(currencies_string)
            )

    def _get_request_headers(self) -> CaseInsensitiveDict:

        headers = CaseInsensitiveDict()
        headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        headers["Pragma"] = "no-cache"
        headers["Expires"] = "0"

        if self._config["user_agent"] != "":
            headers["User-Agent"] = self._config["user_agent"]

        return headers

    def _get_response_for_request(self, request_url: str) -> Response | None:

        response = None

        headers = self._get_request_headers()
        success = False
        attempt = 0

        logging.debug(f"URL to get: {request_url}")

        while not success:

            attempt += 1

            if attempt > 3:
                logging.debug(
                    "The maximum number of attempts to get a response is reached."
                )
                break

            logging.debug(f"Attempt {attempt} to get a response...")

            try:

                response = self._session.get(request_url, headers=headers)

                logging.debug(f"Response status code: {response.status_code}")

                break

            except requests.exceptions.RequestException as exception:

                logging.error(exception)

        return response

    def get_current_date_presentation(self) -> str:
        return self._get_date_as_string(self._current_date)

    @staticmethod
    def get_datetime_from_date(date: datetime.date) -> datetime.datetime:

        return datetime.datetime(date.year, date.month, date.day)

    @staticmethod
    def _get_date_as_string(date: datetime.datetime) -> str:

        return date.strftime("%Y-%m-%d")

    @staticmethod
    def _get_datetime_as_string(date: datetime.datetime) -> str:

        return date.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_time_as_string(date: datetime.datetime) -> str:

        return date.strftime("%H:%M:%S")

    def _write_log_event_currency_rates_change_description(self, rates: list) -> None:

        if len(rates) == 0:
            return

        rates_by_dates = groupby(
            sorted(rates, key=lambda x: x[0]["rate_date"]),
            key=lambda x: x[0]["rate_date"],
        )

        for rates_by_date in rates_by_dates:

            date = rates_by_date[0]
            rates = rates_by_date[1]

            presentations = []

            for rate in rates:

                currency_code = rate[1]["currency_code"]
                rate_initial = self.rate_value_presentation(rate[0]["rate"])
                rate_current = self.rate_value_presentation(rate[1]["rate"])

                presentation = f"{currency_code}: {rate_initial} → {rate_current}"
                presentations.append(presentation)

                self._db.insert_event_rates_updating(
                    self._updating_event,
                    currency_code,
                    date,
                    rate_initial,
                    rate_current,
                )

            date_presentation = self._get_date_as_string(date)
            data_presentation = "\n".join(presentations)

            log = f"Summary of changed rates on {date_presentation}:\n<pre>\n{data_presentation}\n</pre>"

            self.send_to_telegram_chat(log)

    @staticmethod
    def date_with_time_as_string(date_with_time: datetime.datetime) -> str:
        return date_with_time.strftime("%Y-%m-%d %H:%M:%S")
