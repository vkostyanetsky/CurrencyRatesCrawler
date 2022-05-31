#!/usr/bin/env python3

"""Loader of Currency Rates

This script finds currency rates published at the UAE Central Bank, and then writes them
to a database. It has no arguments, but can be customized via the file config.yaml
in the same directory.

"""

import time
import datetime
import platform
import requests
import modules.crawler

from bs4 import BeautifulSoup


class CurrentRatesCrawler(modules.crawler.Crawler):
    _title: str = "Import of current exchanges rates"
    __big_ip_cookie_name: str = "BIGipServer~CEN-BANK~Pool-Web-Prod"
    __request_date_format_string: str = "%#d-%#m-%Y" if platform.system() == "Windows" else "%-d-%-m-%Y"

    def __init__(self, file):

        super().__init__(file)

    def get_rates_from_soup(self, soup: BeautifulSoup, rate_date: datetime.datetime) -> tuple:

        currency_rates = []
        unknown_currencies = []

        tags = soup.find_all('td')

        for index in range(0, len(tags), 2):

            currency_name_tag = tags[index]
            currency_code = self.get_currency_code(currency_name_tag.text)

            if currency_code is None:
                unknown_currencies.append(currency_name_tag)

            if not self.is_currency_code_allowed(currency_code):
                continue

            currency_rate_tag = tags[index + 1]

            currency_rates.append({
                'currency_code': currency_code,
                'import_date': self._current_datetime,
                'rate_date': rate_date,
                'rate': float(currency_rate_tag.text),
            })

        return currency_rates, unknown_currencies

    @staticmethod
    def get_request_url(request_date: datetime.datetime) -> str:

        request_date_string = request_date.strftime(CurrentRatesCrawler.__request_date_format_string)
        url_string = "https://www.centralbank.ae/en/fx-rates-ajax?date={}&v=2".format(request_date_string)

        return url_string

    def get_update_date_and_currency_rates_from_soup(self, response):

        soup = BeautifulSoup(response.text, features="html.parser")

        try:
            update_date = soup.find("span", {"class": "dir-ltr"})
            update_date = update_date.text[:11]
            update_date = datetime.datetime.strptime(update_date, "%d %b %Y")

        except Exception:
            raise Exception

        currency_rates, unknown_currencies = self.get_rates_from_soup(soup, self.get_rate_date(update_date))

        return update_date, currency_rates, unknown_currencies

    def set_big_ip_cookie(self, cookie_value):

        self._logger.debug(
            "Cookie is set: {} = {}".format(self.__big_ip_cookie_name, cookie_value)
        )

        self._session.cookies.set(
            name=self.__big_ip_cookie_name,
            value=cookie_value,
            domain="www.centralbank.ae",
            path="/"
        )

    def get_data_from_bank_for_today_with_big_ip_list(self):

        self._logger.debug("The list of values for the Big-IP cookie is provided.")
        self._logger.debug("Finding the most relevant value for the Big-IP cookie...")

        results_for_cookie_values = {}

        for cookie_value in self._config['big_ip_cookies']:

            self._logger.debug("Checking a Big-IP cookie: {}".format(cookie_value))

            self.set_big_ip_cookie(cookie_value)

            response = self.get_response_for_request(self._user_interface_url)

            for cookie in response.cookies:

                if cookie.name == self.__big_ip_cookie_name:
                    continue

                self._session.cookies.set_cookie(cookie)
                self._logger.debug(
                    "Cookie is set: {} = {}".format(cookie.name, cookie.value)
                )

            results_for_cookie_values[cookie_value] = self.get_update_date_and_currency_rates_from_soup(response)

        cookie_value = sorted(results_for_cookie_values, key=lambda data_item: data_item[0])[0]
        cookie_value_update_date = results_for_cookie_values[cookie_value][0]

        self._logger.debug("The most relevant value for the Big-IP cookie is {} (update date is {}).".format(
            cookie_value, cookie_value_update_date
        ))

        self.set_big_ip_cookie(cookie_value)

        return results_for_cookie_values[cookie_value]

    def get_data_from_bank_for_today_naturally(self):

        self._logger.debug("The list of values for the Big-IP cookie is not provided.")

        response = self.get_response_for_request(self._user_interface_url)

        if response.status_code != 200:
            raise Exception

        for cookie in response.cookies:
            self._session.cookies.set_cookie(cookie)
            self._logger.debug(
                "Cookie is set: {} = {}".format(cookie.name, cookie.value)
            )

        return self.get_update_date_and_currency_rates_from_soup(response)

    def get_data_from_bank_for_today(self):

        if len(self._config['big_ip_cookies']) > 0:
            return self.get_data_from_bank_for_today_with_big_ip_list()
        else:
            return self.get_data_from_bank_for_today_naturally()

    def get_data_from_bank(self, request_url) -> tuple:

        def get_response_json() -> dict:

            response = self.get_response_for_request(request_url)

            return response.json()

        def get_update_date_from_response() -> datetime.date:

            string = response_json["last_updated"].split(" ")
            string = "{} {} {}".format(string[0], string[1], string[2])

            return datetime.datetime.strptime(string, "%d %b %Y")

        def get_currency_rates_from_response() -> tuple:

            rate_date = self.get_rate_date(update_date_from_response)

            table = BeautifulSoup(response_json["table"], features="html.parser")

            return self.get_rates_from_soup(table, rate_date)

        response_json = get_response_json()

        time.sleep(1)

        update_date_from_response = get_update_date_from_response()
        currency_rates_from_response, unknown_currencies_from_response = get_currency_rates_from_response()

        return update_date_from_response, currency_rates_from_response, unknown_currencies_from_response

    def run(self):

        self._write_log_event_import_started()

        total_number_of_changed_rates = 0
        total_number_of_retroactive_rates = 0

        minimal_date = self._current_date - datetime.timedelta(
            days=self._config['number_of_days_to_check']
        )

        request_date = self._current_date

        while request_date >= minimal_date:

            self._logger.debug(
                "REQUEST DATE: {}".format(self.get_date_as_string(request_date))
            )

            if request_date == self._current_date:

                self._logger.debug(
                    "HTML to parse: {}".format(self._user_interface_url)
                )

                update_date, currency_rates, unknown_currencies = self.get_data_from_bank_for_today()

            else:

                request_url = self.get_request_url(request_date)

                self._logger.debug(
                    "JSON to parse: {}".format(request_url)
                )

                update_date, currency_rates, unknown_currencies = self.get_data_from_bank(request_url)

            self.unknown_currencies_warning(unknown_currencies)

            if update_date == request_date:

                self._logger.debug("Update date is equal to the request date.")

                number_of_changed_rates, number_of_retroactive_rates = self._process_currency_rates_to_import(
                    currency_rates
                )

                total_number_of_changed_rates += number_of_changed_rates
                total_number_of_retroactive_rates += number_of_retroactive_rates

                request_date -= datetime.timedelta(days=1)

            else:

                self._logger.debug(
                    "Update date ({}) is not equal to the request date.".format(self.get_date_as_string(update_date))
                )

                if minimal_date <= update_date:

                    self._logger.debug("Switching to the update date.")

                    request_date = update_date

                else:

                    self._logger.debug(
                        "Unable to switch to the update date since it is less than the minimal one ({}).".format(
                            minimal_date)
                    )

                    break

        self._db.insert_import_date(self._current_datetime)

        self._write_log_event_import_completed(total_number_of_changed_rates, total_number_of_retroactive_rates)

        self._db.disconnect()


crawler = CurrentRatesCrawler(__file__)

if __name__ == '__main__':
    crawler.run()
