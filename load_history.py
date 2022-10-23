#!/usr/bin/env python3

"""
Crawler for historical exchange rates. Finds Excel files with currency rates
published at the UAE Central Bank, and then writes them to a database.
It has no arguments, but can be customized via the config.yaml file
in the same directory.
"""

import datetime
import hashlib
import os
import re
import shutil
import ssl

import pandas
import requests
from bs4 import BeautifulSoup

from modules.db import Event
from modules.crawler import UAExchangeRatesCrawler


class HistoricalUAExchangeRatesCrawler(UAExchangeRatesCrawler):
    __historical_files_directory: str = ""

    def __init__(self, file, updating_event):

        super().__init__(file, updating_event)

        self._init_historical_files_directory()

    def _init_historical_files_directory(self) -> None:

        self.__historical_files_directory = os.path.join(
            self._current_directory, "history"
        )

        try:
            os.makedirs(self.__historical_files_directory, exist_ok=True)
        except OSError:
            pass  # TODO needs to be processed

    def _get_links_to_files(self) -> list | None:
        def is_link_to_excel_file(href):
            return href and re.compile("/media/.*[a-z0-9]\\.xlsx").search(href)

        links = []

        self._logger.debug("Attempting to find links to Excel files...")

        page_url = "https://www.centralbank.ae/en/forex-eibor/exchange-rates/"
        response = self._get_response_for_request(page_url)

        if response is not None:

            page = BeautifulSoup(response.text, features="html.parser")
            tags = page.find_all("a", href=is_link_to_excel_file)

            for tag in tags:
                links.append(f'https://www.centralbank.ae{tag.get("href")}')

            self._logger.debug("Search results: %d link(s).", len(links))

        return links

    def _load_currency_rates_from_file(self, link, currency_rates):

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

            if not self._is_currency_code_allowed(currency_code):
                continue

            rate_date = self.get_datetime_from_date(date_column[index])

            currency_rates.append(
                {
                    "currency_code": currency_code,
                    "import_date": self._current_datetime,
                    "rate_date": rate_date + datetime.timedelta(days=1),
                    "rate": float(rate_column[index]),
                }
            )

        self._unknown_currencies_warning(unknown_currencies)

    def _currency_rates_from_file(self, file_link: str) -> list | None:

        currency_rates = []

        self._logger.debug("LINK TO PROCESS: %s", file_link)

        file_path = self.__file_path_in_historical_files_directory(file_link)

        if file_path is not None:

            file_hash = self.__file_hash(file_path)

            self._logger.debug("Downloaded file hash: %s", file_hash)

            historical_file = self._db.historical_file(file_link)

            load = False

            if historical_file is None:

                self._logger.debug(
                    "The file hasn't been processed before "
                    "(unable to find a previous file hash in the database)."
                )

                load = True

            elif historical_file["hash"] != file_hash:

                self._logger.debug(
                    "The file has been updated "
                    "since the last processing ({}), "
                    "because previous file hash ({}) "
                    "is not equal to the current one.".format(
                        self.date_with_time_as_string(historical_file["import_date"]),
                        historical_file["file_hash"],
                    )
                )

                load = True

            else:

                self._logger.debug(
                    "The file hasn't been updated "
                    "since the last processing (%s), "
                    "because a previous file hash "
                    "is equal to the current one.",
                    self.date_with_time_as_string(historical_file["import_date"]),
                )

            if load:

                self._load_currency_rates_from_file(file_link, currency_rates)

                if historical_file is None:
                    self._db.insert_historical_file(
                        file_link, file_hash, import_date=self._current_datetime
                    )
                else:
                    self._db.update_historical_file(
                        file_link, file_hash, import_date=self._current_datetime
                    )

        return currency_rates

    def run(self):

        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        log_title = "import of historical exchange rates"

        self._import_started(log_title, event=Event.HISTORICAL_RATES_LOADING)

        links_to_files = self._get_links_to_files()

        if links_to_files is not None:

            changed_rates_number = 0

            for link_to_file in links_to_files:

                currency_rates = self._currency_rates_from_file(link_to_file)

                self._logger.debug("Crawling results: %d rate(s).", len(currency_rates))

                changed_rates_number += self._process_currency_rates_to_import(
                    currency_rates
                )

            self._db.insert_import_date(self._current_datetime)

            self._log_import_completed(
                title=log_title, changed_rates_number=changed_rates_number
            )

        else:

            self._log_import_failed(title=log_title)

        self._db.disconnect()

    def __file_path_in_historical_files_directory(self, file_link: str) -> str | None:

        file_name = file_link.split("/")[-1]
        file_path = os.path.join(self.__historical_files_directory, file_name)

        file_is_downloaded = False
        attempt_number = 0

        while not file_is_downloaded:

            if attempt_number == 3:
                break

            attempt_number += 1

            self._logger.debug("Attempt #%d to download the file...", attempt_number)

            try:

                with requests.get(file_link, stream=True) as response:
                    with open(file_path, "wb") as file:
                        shutil.copyfileobj(response.raw, file)
                        file_is_downloaded = True
            except (requests.exceptions.RequestException, shutil.Error) as exception:
                self._logger.error(exception)

        if not file_is_downloaded:
            self._logger.debug("Unable to download the file!")
            file_path = None

        return file_path

    @staticmethod
    def __file_hash(file_path: str):

        md5 = hashlib.md5()

        with open(file_path, "rb") as file:
            md5.update(file.read())

        return md5.hexdigest()


if __name__ == "__main__":
    HistoricalUAExchangeRatesCrawler(file=__file__, updating_event=Event.HISTORICAL_RATES_UPDATING).run()
