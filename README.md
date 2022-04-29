# UAECurrencyRatesCrawler

This is a set of scripts intended to crawl currency exchange rates on the Central Bank of UAE, store booty in MondoDB instance, and them allow your app to get it via REST service.

## How it works?

Open [this page](https://www.centralbank.ae/en/fx-rates). There is Javascript application which allows a user to pick a date within current month and Excel / PDF files with historical data for previous periods (have a look at the very bottom). For instance, if today is April, the bank is already published Excel for March for sure.   

So it is possible to get rates via two different ways:

1. Currency rates for this month is possible to get via REST service of the bank which lies behind the Javascript application.
2. Currency rates for previous periods can be found in published files.

## How to use it?

1. [Historical data loader](load_history.py) parse Excel files with historical currency rates and puts it into database. It is intended to be executed once, if you need all the exchange rates is possible to get, not the current ones only.
2. [Current data loader](load_current.py) puts into database currency rates which are possible to crawl via REST service of the bank. The bank is used to publish actual rates approximately at 6:00 PM, so you can execute this script every evening at 8:00, for instance.
3. [REST service](api.py) is a simple Flask app you may run via [gunicorn](https://github.com/benoitc/gunicorn), [uwsgi](https://github.com/unbit/uwsgi), or [unit](https://github.com/nginx/unit). It enables any application to get currency rates from MongoDB instance.

## How to set up?

Have a look at [configuration file](config.yaml).

## What I need to run it?

You need Python 3 (written and tested on 3.10.4) and MongoDB (tested on 5.0.6).

All Python dependecies listed here.

