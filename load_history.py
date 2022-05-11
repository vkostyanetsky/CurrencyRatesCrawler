#!/usr/bin/env python3

"""Loader of Historical Currency Rates

This script finds Excel files with currency rates published at the UAE Central Bank,
and then writes them to a database. It has no arguments, but can be customized via
the file config.yaml in the same directory.

"""

import re
import ssl
import pandas
import modules.crawler

from bs4 import BeautifulSoup


class HistoricalRatesCrawler(modules.crawler.Crawler):

    def __init__(self, file):

        super().__init__(file)

    def run(self):

        self._LOGGER.debug("Attempting to find links to Excel files...")

        """It seems like a not optimal way to avoid CERTIFICATE_VERIFY_FAILED, but it works. 

        Probably creating SSL context via create_default_context() is more appropriate:

            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE        

        However, I didn't find out how to apply this to read_excel().         
        """
        ssl._create_default_https_context = ssl._create_unverified_context

        links_to_files = self.get_links_to_files()
        number_of_links = len(links_to_files)

        self._LOGGER.debug("Search results: {} file(s).".format(number_of_links))

        currency_rates = []

        for link_to_file in links_to_files:
            self.load_currency_rates_from_file(link_to_file, currency_rates)

        self._LOGGER.debug("Crawling results: {} rate(s).".format(len(currency_rates)))
        self._LOGGER.debug("Inserting rates into the database...")

        changed_currency_rates = []

        for currency_rate in currency_rates:

            if not self._DB.is_currency_rate_to_add(currency_rate):
                continue

            if self._DB.is_currency_rate_to_change(currency_rate):
                changed_currency_rates.append({
                    'currency_code': currency_rate['currency_code'],
                    'rate_date': currency_rate['rate_date']
                })

            self._DB.add_currency_rate(currency_rate)

        self.changed_currency_rates_warning(changed_currency_rates)

        self._DB.disconnect()

        self._LOGGER.debug("Loading of historical exchange rates is done.")

    def get_links_to_files(self) -> list:

        def is_link_to_excel_file(href):
            return href and re.compile("/sites/.*[a-z0-9]\\.xlsx").search(href)

        response = self.get_response_for_request("https://www.centralbank.ae/en/fx-rates")

        page = BeautifulSoup(response.text, features="html.parser")
        tags = page.find_all("a", href=is_link_to_excel_file)

        links = []

        for tag in tags:
            link = tag.get("href")
            link = "https://www.centralbank.ae{}".format(link)

            links.append(link)

        return links

    def load_currency_rates_from_file(self, link, currency_rates):

        self._LOGGER.debug("In processing: {}".format(link))

        unknown_currencies = []

        excel_data = pandas.read_excel(link, sheet_name=0, header=2)
        excel_dict = excel_data.to_dict()

        currency_column = excel_dict["Currency"]
        rate_column = excel_dict["Rate"]
        date_column = excel_dict["Date"]

        max_index = len(currency_column) - 1

        for index in range(0, max_index):

            currency_name = currency_column[index]
            currency_code = self.get_currency_code(currency_name)

            if currency_code is None:
                unknown_currencies.append(currency_name)
                continue

            if not self.is_currency_code_allowed(currency_code):
                continue

            rate_date = self.get_datetime_from_date(date_column[index])
            rate_date = self.get_rate_date(rate_date)

            currency_rates.append({
                "currency_code": currency_code,
                "import_date": self._CURRENT_DATETIME,
                "rate_date": rate_date,
                "rate": float(rate_column[index]),
            })

        self.unknown_currencies_warning(unknown_currencies)


crawler = HistoricalRatesCrawler(__file__)

if __name__ == '__main__':
    crawler.run()
