#!/usr/bin/env python3

"""
Crawler for current exchange rates. It finds rates published
at the UAE Central Bank, and then writes them to a database.

It has no arguments, but can be customized via the config.yaml
file in the same directory.
"""

import datetime

from bs4 import BeautifulSoup

from modules.crawler import UAExchangeRatesCrawler


class CurrentUAExchangeRatesCrawler(UAExchangeRatesCrawler):
    def _parse_rates_text_for_date(
        self, text: str, rate_date: datetime.datetime
    ) -> tuple:

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
                        "rate_date": rate_date + datetime.timedelta(days=1),
                        "rate": float(currency_rate),
                    }
                )

            currency_title = None

        return exchange_rates, unknown_currencies

    def _parse_rates_for_date(self, rate_date: datetime.datetime) -> tuple | None:

        date_for_date = None

        page_url = "https://www.centralbank.ae/umbraco/Surface/Exchange/GetExchangeRateAllCurrencyDate"  # noqa: E501
        page_url = f"{page_url}?dateTime={rate_date:%Y-%m-%d}"

        response = self._get_response_for_request(page_url)

        if response is not None and response.status_code == 200:
            date_for_date = self._parse_rates_text_for_date(response.text, rate_date)

        return date_for_date

    def run(self):

        log_title = "import of current exchange rates"

        self._log_import_started(title=log_title)

        days_to_check = self._config.get("days_to_check")
        date_to_check = self._current_datetime.replace(hour=0, minute=0, second=0)

        changed_rates_number = 0

        while days_to_check > 0:

            self._logger.debug(f"DATE TO CHECK: {date_to_check:%Y-%m-%d}")
            self._logger.debug(f"DAYS TO CHECK: {days_to_check}")

            data_for_date = self._parse_rates_for_date(date_to_check)

            if data_for_date is not None:

                exchange_rates, unknown_currencies = data_for_date

                self._unknown_currencies_warning(unknown_currencies)

                changed_rates_number += self._process_currency_rates_to_import(
                    exchange_rates
                )

                self._db.insert_import_date(self._current_datetime)

            days_to_check -= 1
            date_to_check -= datetime.timedelta(days=1)

        self._log_import_completed(
            title=log_title, changed_rates_number=changed_rates_number
        )

        self._db.disconnect()


if __name__ == "__main__":
    CurrentUAExchangeRatesCrawler(__file__).run()
