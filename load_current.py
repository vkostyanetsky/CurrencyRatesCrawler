#!/usr/bin/env python3

"""Loader of Currency Rates

This script finds currency rates published at the UAE Central Bank, and then writes them
to a database. It has no arguments, but can be customized via the file config.yaml
in the same directory.

"""

# TODO: pytest

import os
import time
import datetime

from bs4 import BeautifulSoup

import modules.common as common
import modules.db as db


def get_data_from_bank(request_date) -> tuple:

    def get_response_json() -> dict:

        def get_request_url() -> str:

            request_date_string = request_date.strftime(REQUEST_DATE_FORMAT_STRING)

            return 'https://www.centralbank.ae/en/fx-rates-ajax?date={}&v=5'.format(request_date_string)

        request_url = get_request_url()
        response = common.get_response_for_request(request_url)

        return response.json()

    def get_update_date_from_response() -> datetime.date:

        string = response_json['last_updated'].split(' ')
        string = '{} {} {}'.format(string[0], string[1], string[2])

        return datetime.datetime.strptime(string, '%d %b %Y')

    def get_currency_rates_from_response() -> list:

        def get_valid_from_parameter() -> datetime.datetime:

            update_date_converted = datetime.datetime(update_date.year, update_date.month, update_date.day)
            date_delta = datetime.timedelta(days=CONFIG['number_of_days_to_add'])

            return update_date_converted + date_delta

        valid_from = get_valid_from_parameter()

        table = BeautifulSoup(response_json['table'], features='html.parser')
        rates = []

        tags = table.find_all('td')

        for index in range(0, len(tags), 2):

            currency_name_tag = tags[index]
            currency_code = common.get_currency_code(currency_name_tag.text, CONFIG)

            if currency_code is None:
                continue

            if not common.is_currency_code_allowed(currency_code, CONFIG):
                continue

            currency_rate_tag = tags[index + 1]

            rates.append({
                'currency_code':    currency_code,
                'import_date':      CURRENT_DATETIME,
                'rate_date':        valid_from,
                'rate':             float(currency_rate_tag.text),
            })

        return rates

    response_json = get_response_json()

    time.sleep(1)

    update_date_from_response = get_update_date_from_response()
    currency_rates_from_response = get_currency_rates_from_response()

    return update_date_from_response, currency_rates_from_response


# Initialization

CURRENT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
CONFIG = common.get_config(CURRENT_DIRECTORY)

CURRENT_DATETIME = common.get_current_datetime()

CURRENT_DATE = common.get_current_date()
MINIMAL_DATE = CURRENT_DATE - datetime.timedelta(days=CONFIG['number_of_days_to_check'])

REQUEST_DATE_FORMAT_STRING = common.get_date_format_string()

DB = db.CrawlerDB(CONFIG)

# Main process

crawl_date = CURRENT_DATE

while crawl_date >= MINIMAL_DATE:

    print(
        '\nCrawling date: {}...'
        .format(common.get_date_as_string(crawl_date)),
        end=''
    )

    update_date, currency_rates = get_data_from_bank(crawl_date)

    if update_date == crawl_date:

        print(' Update date is equal.')

        DB.add_currency_rates(currency_rates)

        for rate in currency_rates:
            DB.check_for_ambiguous_currency_rate(rate)

        crawl_date -= datetime.timedelta(days=1)

    else:

        print(
            ' Update date is different ({}).'
            .format(common.get_date_as_string(update_date))
        )
        print('There are no rates for the crawl date.')

        if MINIMAL_DATE <= update_date:

            print('Switching to the update date...')
            crawl_date = update_date

        else:

            print(
                'Unable to switch to the update date since it is less than {} (the minimal one).'
                .format(MINIMAL_DATE)
            )
            break
