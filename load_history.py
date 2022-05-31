#!/usr/bin/env python3

"""Loader of Historical Currency Rates

This script finds Excel files with currency rates published at the UAE Central Bank,
and then writes them to a database. It has no arguments, but can be customized via
the file config.yaml in the same directory.

"""

import re
import os
import ssl
import pandas
import shutil
import hashlib
import requests
import modules.crawler

from bs4 import BeautifulSoup


class HistoricalRatesCrawler(modules.crawler.Crawler):
    _title: str = "Import of historical exchange rates"
    __historical_files_directory: str = ''

    def __init__(self, file):

        super().__init__(file)

        self.__init_historical_files_directory()

    def run(self):

        self._write_log_event_import_started()

        self._logger.debug("Attempting to find links to Excel files...")

        """It seems like a not optimal way to avoid CERTIFICATE_VERIFY_FAILED, but it works. 

        Probably creating SSL context via create_default_context() is more appropriate:

            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE        

        However, I didn't find out how to apply this to read_excel().         
        """
        ssl._create_default_https_context = ssl._create_unverified_context

        links_to_files = self.__get_links_to_files()
        number_of_links = len(links_to_files)

        self._logger.debug("Search results: {} link(s).".format(number_of_links))

        total_number_of_changed_rates = 0
        total_number_of_retroactive_rates = 0

        for link_to_file in links_to_files:

            currency_rates = self.__currency_rates_from_file(link_to_file)
            self._logger.debug("Crawling results: {} rate(s).".format(len(currency_rates)))

            number_of_changed_rates, number_of_retroactive_rates = self._process_currency_rates_to_import(
                currency_rates
            )

            total_number_of_changed_rates += number_of_changed_rates
            total_number_of_retroactive_rates += number_of_retroactive_rates

        self._db.insert_import_date(self._current_datetime)

        self._write_log_event_import_completed(total_number_of_changed_rates, total_number_of_retroactive_rates)

        self._db.disconnect()

    def __init_historical_files_directory(self) -> None:

        self.__historical_files_directory = os.path.join(self._current_directory, "history")

        try:
            os.makedirs(self.__historical_files_directory, exist_ok=True)
        except OSError:
            pass  # TODO needs to be processed

    def __currency_rates_from_file(self, file_link: str) -> list:

        currency_rates = []

        self._logger.debug("LINK TO PROCESS: {}".format(file_link))

        file_path = self.__file_path_in_historical_files_directory(file_link)
        file_hash = self.__file_hash(file_path)

        self._logger.debug("Downloaded file hash: {}".format(file_hash))

        historical_file = self._db.historical_file(file_link)

        load = False

        if historical_file is None:

            self._logger.debug(
                "The file hasn't been processed before "
                "(unable to find a previous file hash in the database)."
            )

            load = True

        elif historical_file['hash'] != file_hash:

            self._logger.debug(
                "The file has been updated "
                "since the last processing ({}), "
                "because previous file hash ({}) "
                "is not equal to the current one.".format(
                    self.date_with_time_as_string(historical_file['import_date']),
                    historical_file['file_hash']
                )
            )

            load = True

        else:

            self._logger.debug(
                "The file hasn't been updated "
                "since the last processing ({}), "
                "because a previous file hash "
                "is equal to the current one.".format(
                    self.date_with_time_as_string(historical_file['import_date'])
                )
            )

        if load:

            self.__load_currency_rates_from_file(file_link, currency_rates)

            if historical_file is None:
                self._db.insert_historical_file(file_link, file_hash, import_date=self._current_datetime)
            else:
                self._db.update_historical_file(file_link, file_hash, import_date=self._current_datetime)

        return currency_rates

    def __get_links_to_files(self) -> list:

        def is_link_to_excel_file(href):
            return href and re.compile("/sites/.*[a-z0-9]\\.xlsx").search(href)

        response = self.get_response_for_request(self._user_interface_url)

        page = BeautifulSoup(response.text, features="html.parser")
        tags = page.find_all("a", href=is_link_to_excel_file)

        links = []

        for tag in tags:
            link = tag.get("href")
            link = "https://www.centralbank.ae{}".format(link)

            links.append(link)

        return links

    def __load_currency_rates_from_file(self, link, currency_rates):

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
                "import_date": self._current_datetime,
                "rate_date": rate_date,
                "rate": float(rate_column[index]),
            })

        self.unknown_currencies_warning(unknown_currencies)

    def __file_path_in_historical_files_directory(self, file_link: str) -> str:

        file_name = file_link.split('/')[-1]
        file_path = os.path.join(self.__historical_files_directory, file_name)

        with requests.get(file_link, stream=True) as response:
            with open(file_path, 'wb') as file:
                shutil.copyfileobj(response.raw, file)

        return file_path

    @staticmethod
    def __file_hash(file_path: str):

        md5 = hashlib.md5()

        with open(file_path, "rb") as file:
            md5.update(file.read())

        return md5.hexdigest()


crawler = HistoricalRatesCrawler(__file__)

if __name__ == '__main__':
    crawler.run()
