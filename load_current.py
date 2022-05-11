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

    def get_request_url(self, crawl_date: datetime.datetime) -> str:

        request_date_string = crawl_date.strftime(self.__REQUEST_DATE_FORMAT_STRING)

        return "https://www.centralbank.ae/en/fx-rates-ajax?date={}&v=1".format(request_date_string)

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

            currency_rates = []
            unknown_currencies = []

            tags = table.find_all('td')

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

        response_json = get_response_json()

        time.sleep(1)

        update_date_from_response = get_update_date_from_response()
        currency_rates_from_response, unknown_currencies_from_response = get_currency_rates_from_response()

        return update_date_from_response, currency_rates_from_response, unknown_currencies_from_response

    def get_current_date_presentation(self):

        return self.get_date_as_string(self._CURRENT_DATE)

    def get_start_message(self) -> str:
        current_date_presentation = self.get_current_date_presentation()

        return "Regular import for {} is started.".format(current_date_presentation)

    def get_final_message(self, number_of_added_rates) -> str:

        current_date_presentation = self.get_current_date_presentation()

        final_message = "Regular import for {} is done.".format(current_date_presentation)

        if number_of_added_rates > 0:
            final_message_suffix = "Number of added or changes rates: {}.".format(number_of_added_rates)
        else:
            final_message_suffix = "No changes found."

        return "{} {}".format(final_message, final_message_suffix)

    def run(self):

        self._LOGGER.debug(self.get_start_message())

        number_of_added_rates = 0
        changed_currency_rates = []
        historical_currency_rates = []

        minimal_date = self._CURRENT_DATE - datetime.timedelta(
            days=self._CONFIG['number_of_days_to_check']
        )

        crawl_date = self._CURRENT_DATE

        while crawl_date >= minimal_date:

            self._LOGGER.debug(
                "CRAWLING DATE: {}".format(self.get_date_as_string(crawl_date))
            )

            request_url = self.get_request_url(crawl_date)

            self._LOGGER.debug(
                "Link to crawl: {}".format(request_url)
            )

            update_date, currency_rates, unknown_currencies = self.get_data_from_bank(request_url)

            self.unknown_currencies_warning(unknown_currencies)

            if update_date == crawl_date:

                self._LOGGER.debug("Update date is equal to the crawling one.")
                self._LOGGER.debug("Rates obtained: {}".format(len(currency_rates)))

                number_of_historical = 0
                number_of_changed = 0
                number_of_added = 0

                for currency_rate in currency_rates:

                    if not self._DB.is_currency_rate_to_add(currency_rate):
                        continue

                    if currency_rate['rate_date'] < self.get_rate_date(self._CURRENT_DATE):
                        number_of_historical += 1
                        historical_currency_rates.append({
                            'currency_code': currency_rate['currency_code'],
                            'rate_date': currency_rate['rate_date']
                        })

                    if self._DB.is_currency_rate_to_change(currency_rate):
                        number_of_changed += 1
                        changed_currency_rates.append({
                            'currency_code': currency_rate['currency_code'],
                            'rate_date': currency_rate['rate_date']
                        })

                    number_of_added += 1
                    self._DB.add_currency_rate(currency_rate)

                    number_of_added_rates += 1

                crawl_date -= datetime.timedelta(days=1)

                self._LOGGER.debug(
                    "Rates added: {} (historical: {}, changed: {}).".format(
                        number_of_added, number_of_historical, number_of_changed
                    )
                )

            else:

                self._LOGGER.debug(
                    "Update date ({}) is not equal to crawling one.".format(self.get_date_as_string(update_date))
                )

                if minimal_date <= update_date:

                    self._LOGGER.debug("Switching to the update date.")

                    crawl_date = update_date

                else:

                    self._LOGGER.debug(
                        "Unable to switch to the update date since it is less than {} (the minimal one).".format(
                            minimal_date)
                    )

                    break

        self.changed_currency_rates_warning(changed_currency_rates)
        self.historical_currency_rates_warning(historical_currency_rates)

        self._LOGGER.info(self.get_final_message(number_of_added_rates))

        self._DB.disconnect()


crawler = CurrentRatesCrawler(__file__)

if __name__ == '__main__':
    crawler.run()
