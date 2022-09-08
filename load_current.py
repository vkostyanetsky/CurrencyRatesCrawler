#!/usr/bin/env python3

"""
Crawler for current exchange rates. It finds rates published
at the UAE Central Bank, and then writes them to a database.

It has no arguments, but can be customized via the config.yaml
file in the same directory.
"""

import datetime
import re

from bs4 import BeautifulSoup

from modules.crawler import UAExchangeRatesCrawler


class CurrentUAExchangeRatesCrawler(UAExchangeRatesCrawler):
    def _parse_bank_page_text(self, text: str, rate_date: datetime.datetime) -> tuple:

        soup = BeautifulSoup(text, features="html.parser")

        exchange_rates = []
        unknown_currencies = []

        tags = soup.find_all("td")
        currency_title = None

        for tag in tags:

            # <td class="font-r fs-small text-navy-custom"></td>

            if len(tag.text) == 0:
                continue

            # <td class="font-r fs-small text-navy-custom">US Dollar</td>

            if not tag.text[0].isdigit():
                currency_title = tag.text
                continue

            # <td class="font-r fs-small text-navy-custom">3.6725</td>

            currency_rate = tag.text
            currency_code = self.get_currency_code(currency_title)

            if currency_code is None:

                unknown_currencies.append(currency_title)

            elif self._is_currency_code_allowed(currency_code):

                exchange_rates.append(
                    {
                        "currency_code": currency_code,
                        "import_date": self._current_datetime,
                        "rate_date": rate_date,
                        "rate": float(currency_rate),
                    }
                )

            currency_title = None

        return exchange_rates, unknown_currencies

    def _parse_bank_page(self, rate_date: datetime.datetime) -> tuple | None:

        parsing_results = None

        page_url = self._config.get("current_exchange_rates_url")
        response = self._get_response_for_request(page_url)

        if response is not None:
            parsing_results = self._parse_bank_page_text(response.text, rate_date)

        return parsing_results

    def _get_day_end_time(self) -> datetime.datetime | None:

        end_time = self._config.get("day_end_time")

        if end_time is not None:
            hours, minutes = end_time.split(":")
            end_time_shift = datetime.timedelta(hours=int(hours), minutes=int(minutes))

            end_time = self._get_beginning_of_this_day() + end_time_shift

        return end_time

    def _log_day_end_time(self, day_end_time: datetime.datetime | None) -> None:
        if day_end_time is None:
            message = "Day end time is not set."
        else:
            message = f"Day end time is {self._get_datetime_as_string(day_end_time)}."

        self._logger.debug(message)

    def _get_new_rates_date(
        self, day_end_time: datetime.datetime | None
    ) -> datetime.datetime:

        return (
            self._current_date
            if day_end_time is None or self._current_datetime <= day_end_time
            else self._current_date + datetime.timedelta(days=1)
        )

    def _log_new_rates_date(self, new_rates_date: datetime.datetime) -> None:
        message = f"New rates date is {self._get_date_as_string(new_rates_date)}."
        self._logger.debug(message)

    def run(self):

        log_title = "import of current exchange rates"

        self._log_import_started(title=log_title)

        day_end_time = self._get_day_end_time()
        self._log_day_end_time(day_end_time)

        new_rates_date = self._get_new_rates_date(day_end_time)
        self._log_new_rates_date(new_rates_date)

        parsing_results = self._parse_bank_page(new_rates_date)

        if parsing_results is not None:

            exchange_rates, unknown_currencies = parsing_results

            self._unknown_currencies_warning(unknown_currencies)

            changed_rates_number = self._process_currency_rates_to_import(
                exchange_rates
            )

            self._db.insert_import_date(self._current_datetime)

            self._log_import_completed(
                title=log_title, changed_rates_number=changed_rates_number
            )

        else:

            self._log_import_failed(title=log_title)

        self._db.disconnect()


if __name__ == "__main__":
    CurrentUAExchangeRatesCrawler(__file__).run()
