# UAE Exchange Rates

[![pylint](https://github.com/vkostyanetsky/UAExchangeRates/actions/workflows/pylint.yml/badge.svg)](https://github.com/vkostyanetsky/UAExchangeRates/actions/workflows/pylint.yml) [![black](https://github.com/vkostyanetsky/UAExchangeRates/actions/workflows/black.yml/badge.svg)](https://github.com/vkostyanetsky/UAExchangeRates/actions/workflows/black.yml) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

It is a set of scripts intended to crawl the currency exchange rates at the Central Bank of the United Arab Emirates. The collected rates are being written to a MongoDB database and can be retrieved via a REST interface.

## 🤔 How to set it up?

All the settings presented in [config.yaml](config.yaml). They are pretty well commented, so it looks like I don't need to explain here the meaning of each setting.

## 😕 How to use it?

Well, long story short: 

1. Setup periodical running for [load_current.py](load_current.py)
2. Do the same for [load_history.py](load_history.py) 
3. Start the REST service using [api.py](api.py).    

More details are below.

## 🌇 Current exchange rates 

Current rates are the rates for the short period of time back from this moment. For instance, two weeks. 

The `load_current.py` script puts into the database currency rates, which are possible to crawl via the REST service of the bank. The bank used to publish actual rates approximately at 6:00 PM, so you can execute this script every evening at 8:00, for instance.

For instance, [this is an example](https://www.centralbank.ae/umbraco/Surface/Exchange/GetExchangeRateAllCurrencyDate?dateTime=2023-02-17
) of a URL the script can crawl to get currency rates for February 17, 2023.

## 🌆 Historical exchange rates

Historical rates are the rates that have been published on the bank website in Excel files.

This script tries to find the files on the [respective page](https://www.centralbank.ae/umbraco/Surface/Exchange/GetExchangeRateAllCurrency), then parses each one and puts the collected rates into a database.

You are supposed to start this script from time to time to be sure that if the bank changes something without warning, you will see the changes in your database. However, you can execute the script only once (for instance, if you just want to load all currency rates that are possible to get). 




 

<!--


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