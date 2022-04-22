#!/usr/bin/env python3

"""Loader of Historical Currency Rates

This script finds Excel files with currency rates published at the UAE Central Bank,
and then writes them to a database. It has no arguments, but can be customized via
the file config.yaml in the same directory.

"""

import os
import re
import pandas

from bs4 import BeautifulSoup

import modules.common as common_module
import modules.logger as logger_module
import modules.rates as rates_module
import modules.db as db_module


def get_links_to_files() -> list:

    def is_link_to_excel_file(href):
        return href and re.compile("/sites/.*[a-z0-9]\\.xlsx").search(href)

    response = common_module.get_response_for_request("https://www.centralbank.ae/en/fx-rates")

    page = BeautifulSoup(response.text, features="html.parser")
    tags = page.find_all("a", href=is_link_to_excel_file)

    links = []

    for tag in tags:
        link = tag.get("href")
        link = "https://www.centralbank.ae{}".format(link)

        links.append(link)

    return links


def load_currency_rates_from_file(link):

    logger.debug("In processing: {}".format(link))

    excel_data = pandas.read_excel(link, sheet_name=0, header=2)
    excel_dict = excel_data.to_dict()

    currency_column = excel_dict["Currency"]
    rate_column = excel_dict["Rate"]
    date_column = excel_dict["Date"]

    max_index = len(currency_column) - 1

    rates = []

    for index in range(0, max_index):

        currency_code = rates_module.get_currency_code(currency_column[index], config)

        if currency_code is None:
            continue

        if not rates_module.is_currency_code_allowed(currency_code, config):
            continue

        rate_date = common_module.get_datetime_from_date(date_column[index])
        rate_date = rates_module.get_rate_date(rate_date, config)

        rates.append({
            'currency_code':    currency_code,
            'import_date':      current_datetime,
            'rate_date':        rate_date,
            'rate':             float(rate_column[index]),
        })

    db.add_currency_rates(rates)


# Initialization

current_directory = os.path.abspath(os.path.dirname(__file__))
current_datetime = common_module.get_current_datetime()
current_date = common_module.get_beginning_of_today()

config = common_module.get_config(current_directory)
logger = logger_module.get_logger(os.path.basename(__file__), config, current_directory)

db = db_module.CrawlerDB(config)

# Lets-a-go!

logger.debug("Attempting to find links to Excel files...")

links_to_files = get_links_to_files()
number_of_links = len(links_to_files)

logger.debug("Search results: {} file(s).".format(number_of_links))

for link_to_file in links_to_files:
    load_currency_rates_from_file(link_to_file)

db.disconnect()

logger.debug("Loading of historical exchange rates is done.")
