# UAE Exchange Rates

[![pylint](https://github.com/vkostyanetsky/UAExchangeRates/actions/workflows/pylint.yml/badge.svg)](https://github.com/vkostyanetsky/UAExchangeRates/actions/workflows/pylint.yml) [![black](https://github.com/vkostyanetsky/UAExchangeRates/actions/workflows/black.yml/badge.svg)](https://github.com/vkostyanetsky/UAExchangeRates/actions/workflows/black.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This is a set of scripts intended to effectively crawl currency exchange rates on the Central Bank of United Arabian Emirates, and then allow another app to get it via REST service.

## 🙃 How to set up it?

You can use any settings presented in [config.yaml]. All of them are commented, so it seems to me that I don't need to explain meaning of each setting key once again here.

## 🙃 How to start it?

You have to set up periodical running for both of Python scripts, [load_current.py] and [load_history.py].  

<!--

```ad-example
title: Current Rates
https://www.centralbank.ae/umbraco/Surface/Exchange/GetExchangeRateAllCurrency
```

```ad-example
title: Historical Rates (2023, 17 Feb)
https://www.centralbank.ae/umbraco/Surface/Exchange/GetExchangeRateAllCurrencyDate?dateTime=2023-02-17
```

```yaml
    file:
      class: logging.handlers.RotatingFileHandler
      level: DEBUG
      formatter: simple
      filename: /var/log/crawler.log
      maxBytes: 10485760
      backupCount: 10
      encoding: utf8
```

## How does it work?

Open [this page](https://www.centralbank.ae/en/fx-rates). There is a Javascript application which allows a user to pick a date within the current month and Excel / PDF files with historical data for previous periods (have a look at the very bottom). For instance, if today is April, the bank already published the Excel file for March for sure.

So, it is possible to get rates via two different ways:

1. Currency rates for this month can be received via REST service of the bank, which lies behind the Javascript application.
2. Currency rates for previous periods can be found in published Excel files.

## How to use it?

1. [Historical data loader](load_history.py) parse Excel files with historical currency rates and puts it into database. It is intended to be executed once, if you need all the exchange rates which are possible to get, not the current ones only.
2. [Current data loader](load_current.py) puts into database currency rates which are possible to crawl via REST service of the bank. The bank used to publish actual rates approximately at 6:00 PM, so you can execute this script every evening at 8:00, for instance.
3. [REST service](api.py) is a simple Flask app you may run via [gunicorn](https://github.com/benoitc/gunicorn), [uwsgi](https://github.com/unbit/uwsgi), or [unit](https://github.com/nginx/unit). It enables any application to get currency rates from MongoDB instance.

## How to set up?

Have a look at the [configuration file](config.yaml).

## What do I need to run it?

It's required to have Python 3 (written and tested on 3.10.4) and MongoDB (tested on 5.0.6) installed.

All Python dependencies listed [here](requirements.txt).

-->