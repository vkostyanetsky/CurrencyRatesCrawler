#!/usr/bin/env python3

"""Loader of Currency Rates

This script finds currency rates published at the UAE Central Bank, and then writes them
to a database. It has no arguments, but can be customized via the file config.yaml
in the same directory.

"""

import time
import datetime
import platform
import modules.crawler

from bs4 import BeautifulSoup


class CurrentRatesCrawler(modules.crawler.Crawler):
    __REQUEST_DATE_FORMAT_STRING = "%#d-%#m-%Y" if platform.system() == "Windows" else "%-d-%-m-%Y"

    def __init__(self, file):

        super().__init__(file)

        self._load_session()

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
                'import_date': self._CURRENT_DATETIME,
                'rate_date': rate_date,
                'rate': float(currency_rate_tag.text),
            })

        return currency_rates, unknown_currencies

    @staticmethod
    def get_request_url_for_today():

        return "https://www.centralbank.ae/en/fx-rates"

    @staticmethod
    def get_request_url(request_date: datetime.datetime) -> str:

        request_date_string = request_date.strftime(CurrentRatesCrawler.__REQUEST_DATE_FORMAT_STRING)
        url_string = "https://www.centralbank.ae/en/fx-rates-ajax?date={}&v=2".format(request_date_string)

        return url_string

    def get_data_from_bank_for_today(self):

        response = self.get_response_for_request(self.get_request_url_for_today())

        if response.status_code != 200:
            raise Exception

        soup = BeautifulSoup(response.text, features="html.parser")

        try:
            update_date = soup.find("span", {"class": "dir-ltr"})
            update_date = update_date.text[:11]
            update_date = datetime.datetime.strptime(update_date, "%d %b %Y")

        except Exception:
            raise Exception

        currency_rates, unknown_currencies = self.get_rates_from_soup(soup, update_date)

        return update_date, currency_rates, unknown_currencies

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

    def get_current_date_presentation(self) -> str:

        return self.get_date_as_string(self._CURRENT_DATE)

    def get_start_message(self) -> str:
        current_date_presentation = self.get_current_date_presentation()

        return "REGULAR IMPORT IS STARTED.".format(current_date_presentation)

    def get_final_message(self, number_of_added_rates) -> str:

        current_date_presentation = self.get_current_date_presentation()

        final_message = "Regular import is done.".format(current_date_presentation)

        if number_of_added_rates > 0:
            final_message_suffix = "Number of added or changed rates: {}.".format(number_of_added_rates)
        else:
            final_message_suffix = "No changes found."

        return "{} {}".format(final_message, final_message_suffix)

    def run(self):

        self._LOGGER.debug(self.get_start_message())
        self._LOGGER.debug(self.get_log_message_about_import_date())

        number_of_added_rates = 0
        changed_currency_rates = []
        historical_currency_rates = []

        minimal_date = self._CURRENT_DATE - datetime.timedelta(
            days=self._CONFIG['number_of_days_to_check']
        )

        request_date = self._CURRENT_DATE

        while request_date >= minimal_date:

            self._LOGGER.debug(
                "REQUEST DATE: {}".format(self.get_date_as_string(request_date))
            )

            if request_date == self._CURRENT_DATE:

                self._LOGGER.debug(
                    "HTML to parse: {}".format(self.get_request_url_for_today())
                )

                update_date, currency_rates, unknown_currencies = self.get_data_from_bank_for_today()

            else:

                request_url = self.get_request_url(request_date)

                self._LOGGER.debug(
                    "JSON to parse: {}".format(request_url)
                )

                update_date, currency_rates, unknown_currencies = self.get_data_from_bank(request_url)

            self.unknown_currencies_warning(unknown_currencies)

            if update_date == request_date:

                self._LOGGER.debug("Update date is equal to the request date.")
                self._LOGGER.debug("Processing obtained rates...")

                number_of_historical = 0
                number_of_changed = 0
                number_of_added = 0

                for currency_rate in currency_rates:

                    rate_presentation = "- {} {} = {}".format(currency_rate['currency_code'], datetime.datetime.strftime(currency_rate['rate_date'], '%d-%m-%Y'), format(currency_rate['rate'], '.6f'))
                    is_historical = False
                    is_changed = False

                    if not self._DB.is_currency_rate_to_add(currency_rate):
                        self._LOGGER.debug("{} - skipped (already loaded)".format(rate_presentation))
                        continue

                    if currency_rate['rate_date'] < self.get_rate_date(self._CURRENT_DATE):
                        number_of_historical += 1
                        is_historical = True
                        historical_currency_rates.append({
                            'currency_code': currency_rate['currency_code'],
                            'rate_date': currency_rate['rate_date']
                        })

                    if self._DB.is_currency_rate_to_change(currency_rate):
                        number_of_changed += 1
                        is_changed = True
                        changed_currency_rates.append({
                            'currency_code': currency_rate['currency_code'],
                            'rate_date': currency_rate['rate_date']
                        })

                    number_of_added += 1
                    self._DB.add_currency_rate(currency_rate)

                    number_of_added_rates += 1

                    self._LOGGER.debug("{} - added".format(rate_presentation))

                self._LOGGER.debug("Obtained rates have been processed.")

                request_date -= datetime.timedelta(days=1)

                if number_of_added == 0:
                    self._LOGGER.debug("Rates added: 0")
                else:
                    self._LOGGER.debug(
                        "Rates added: {} (historical: {}, changed: {}).".format(
                            number_of_added, number_of_historical, number_of_changed
                        )
                    )

            else:

                self._LOGGER.debug(
                    "Update date ({}) is not equal to the request date.".format(self.get_date_as_string(update_date))
                )

                if minimal_date <= update_date:

                    self._LOGGER.debug("Switching to the update date.")

                    request_date = update_date

                else:

                    self._LOGGER.debug(
                        "Unable to switch to the update date since it is less than the minimal one ({}).".format(
                            minimal_date)
                    )

                    break

        self.changed_currency_rates_warning(changed_currency_rates)
        self.historical_currency_rates_warning(historical_currency_rates)

        self._DB.add_import_date(self._CURRENT_DATETIME)

        self._LOGGER.info(self.get_final_message(number_of_added_rates))

        self._DB.disconnect()


crawler = CurrentRatesCrawler(__file__)

if __name__ == '__main__':
    crawler.run()
