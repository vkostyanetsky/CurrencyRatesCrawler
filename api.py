import datetime
import os

from flask import Flask
from flask_restful import Api
from flask_restful import Resource

import modules.common as common
import modules.db as db


def get_error_response_using_exception(exception):

    error_message = f"{exception=}"
    return get_error_response(error_message)


def get_error_response(message):

    data = {'error': message}

    return data, 200


def get_rates_response(rates):

    datetime_format_string = "%Y%m%d%H%M%S"
    date_format_string = "%Y%m%d"

    import_dates = []

    for rate in rates:

        import_dates.append(rate['import_date'])

        rate.update({
            'import_date':  rate['import_date'].strftime(datetime_format_string),
            'rate_date':    rate['rate_date'].strftime(date_format_string),
        })

    max_import_date = max(import_dates) if len(import_dates) > 0 else datetime.datetime(1, 1, 1)
    max_import_date = max_import_date.strftime(datetime_format_string)

    data = {
        'rates':            rates,
        'max_import_date':  max_import_date
    }

    return data, 200


class Hello(Resource):

    @staticmethod
    def get():

        message = 'No action specified.'

        return get_error_response(message)


class Rates(Resource):

    @staticmethod
    def get():

        message = 'No currency specified.'

        return get_error_response(message)


class RatesWithCurrencyCode(Resource):

    @staticmethod
    def get(currency_code: str):

        rates = DB.get_currency_rates(currency_code)

        return get_rates_response(rates)


class RatesWithCurrencyCodeAndDate(Resource):

    @staticmethod
    def get(currency_code: str, date: str):

        def get_date():

            year = int(date[:4])
            month = int(date[4:6])
            day = int(date[6:8])

            hour = int(date[8:10])
            minute = int(date[10:12])
            second = int(date[12:])

            return datetime.datetime(year, month, day, hour, minute, second)

        try:

            date = get_date()

        except ValueError:

            error_message = "Unable to parse a date."
            return get_error_response(error_message)

        rates = DB.get_currency_rates(currency_code, date)

        return get_rates_response(rates)


app = Flask(__name__)
api = Api(app)

api.add_resource(
    Hello,
    '/'
)

api.add_resource(
    Rates,
    "/rates/", "/rates/"
)

api.add_resource(
    RatesWithCurrencyCode,
    "/rates/", "/rates/<currency_code>/"
)

api.add_resource(
    RatesWithCurrencyCodeAndDate,
    "/rates/<currency_code>/", "/rates/<currency_code>/<date>/"
)

CURRENT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
CONFIG = common.get_config(CURRENT_DIRECTORY)
DB = db.CrawlerDB(CONFIG)

if __name__ == '__main__':
    app.run()
