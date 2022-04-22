#!/usr/bin/env python3

"""Loader of Currency Rates

This script finds currency rates published at the UAE Central Bank, and then writes them
to a database. It has no arguments, but can be customized via the file config.yaml
in the same directory.

"""

import os
import time
import datetime
import platform

from bs4 import BeautifulSoup

import modules.common as common_module
import modules.logger as logger_module
import modules.rates as rates_module
import modules.db as db_module


def get_data_from_bank(request_date) -> tuple:

    def get_response_json() -> dict:

        def get_request_url() -> str:

            request_date_string = request_date.strftime(request_date_format_string)

            return "https://www.centralbank.ae/en/fx-rates-ajax?date={}&v=5".format(request_date_string)

        request_url = get_request_url()
        response = common_module.get_response_for_request(request_url)

        return response.json()

    def get_update_date_from_response() -> datetime.date:

        string = response_json["last_updated"].split(" ")
        string = "{} {} {}".format(string[0], string[1], string[2])

        return datetime.datetime.strptime(string, "%d %b %Y")

    def get_currency_rates_from_response() -> list:

        rate_date = rates_module.get_rate_date(update_date_from_response, config)

        table = BeautifulSoup(response_json["table"], features="html.parser")
        rates = []

        tags = table.find_all('td')

        for index in range(0, len(tags), 2):

            currency_name_tag = tags[index]
            currency_code = rates_module.get_currency_code(currency_name_tag.text, config)

            if currency_code is None:
                continue

            if not rates_module.is_currency_code_allowed(currency_code, config):
                continue

            currency_rate_tag = tags[index + 1]

            rates.append({
                'currency_code':    currency_code,
                'import_date':      current_datetime,
                'rate_date':        rate_date,
                'rate':             float(currency_rate_tag.text),
            })

        return rates

    response_json = get_response_json()

    time.sleep(1)

    update_date_from_response = get_update_date_from_response()
    currency_rates_from_response = get_currency_rates_from_response()

    return update_date_from_response, currency_rates_from_response


# Initialization

current_directory = os.path.abspath(os.path.dirname(__file__))
current_datetime = common_module.get_current_datetime()
current_date = common_module.get_beginning_of_today()

config = common_module.get_config(current_directory)
logger = logger_module.get_logger(os.path.basename(__file__), config, current_directory)

request_date_format_string = "%#d-%#m-%Y" if platform.system() == "Windows" else "%-d-%-m-%Y"
minimal_date = current_date - datetime.timedelta(days=config['number_of_days_to_check'])

db = db_module.CrawlerDB(config)

# Let's get it started!

crawl_date = current_date

while crawl_date >= minimal_date:

    logger.debug(
        "Crawling date: {}...".format(common_module.get_date_as_string(crawl_date))
    )

    update_date, currency_rates = get_data_from_bank(crawl_date)

    if update_date == crawl_date:

        logger.debug("Update date is equal.")

        db.add_currency_rates(currency_rates)

        for rate in currency_rates:
            db.check_for_ambiguous_currency_rate(rate)

        crawl_date -= datetime.timedelta(days=1)

    else:

        logger.debug(
            "Update date is different ({}).".format(common_module.get_date_as_string(update_date))
        )
        logger.debug("There are no rates for the crawl date.")

        if minimal_date <= update_date:

            logger.debug("Switching to the update date...")

            crawl_date = update_date

        else:

            logger.debug(
                "Unable to switch to the update date since it is less than {} (the minimal one).".format(minimal_date)
            )

            break

db.disconnect()

logger.info(
    "Crawling for {} is done.".format(common_module.get_date_as_string(current_date))
)
