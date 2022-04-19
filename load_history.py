#!/usr/bin/env python3

"""Loader of Historical Currency Rates

This script finds Excel files with currency rates published at the UAE Central Bank,
and then writes them to a database. It has no arguments, but can be customized via
the file config.yaml in the same directory.

"""

# TODO: pytest

import os
import re
import pandas

from bs4 import BeautifulSoup

import modules.common as common
import modules.db as db


def get_links_to_files() -> list:

    def is_link_to_excel_file(href):
        return href and re.compile("\/sites\/.*[a-z0-9]\.xlsx").search(href)

    response = common.get_response_for_request('https://www.centralbank.ae/en/fx-rates')

    page = BeautifulSoup(response.text, features='html.parser')
    tags = page.find_all('a', href=is_link_to_excel_file)

    links = []

    for tag in tags:
        link = tag.get('href')
        link = 'https://www.centralbank.ae{}'.format(link)

        links.append(link)

    return links


def load_currency_rates_from_file(link):

    print('In processing:', link)

    excel_data = pandas.read_excel(link, header=2)
    excel_dict = excel_data.to_dict()

    currency_column = excel_dict['Currency']
    rate_column = excel_dict['Rate']
    date_column = excel_dict['Date']

    max_index = len(currency_column) - 1

    rates = []

    for index in range(0, max_index):

        currency_code = common.get_currency_code(currency_column[index], CONFIG)

        if currency_code is None:
            continue

        if not common.is_currency_code_allowed(currency_code, CONFIG):
            continue

        rates.append({
            'currency_code': currency_code,
            'currency_rate': float(rate_column[index]),
            'valid_from':    common.get_datetime_from_date(date_column[index]),
            'written_at':    CURRENT_DATE,
            'version':       CURRENCY_RATES_VERSION,
        })

    DB.add_currency_rates(rates)


# Initialization

CURRENCY_RATES_VERSION = common.get_currency_rates_version()

CURRENT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
CONFIG = common.get_config(CURRENT_DIRECTORY)

CURRENT_DATE = common.get_current_date()

DB = db.CrawlerDB(CONFIG)

# Main process

links_to_files = get_links_to_files()
number_of_links = len(links_to_files)

print('Search results: {} file(s)'.format(number_of_links))

for link_to_file in links_to_files:
    load_currency_rates_from_file(link_to_file)