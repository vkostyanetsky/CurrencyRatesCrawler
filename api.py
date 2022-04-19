import os

from flask import Flask
from flask_restful import Api
from flask_restful import Resource

import modules.common as common
import modules.db as db


def get_error_response(message):

    data = {'error': message}

    return data, 200


def get_rates_response(rates):

    def get_max_version():

        versions = []

        for rate in rates:
            versions.append(rate['version'])

        return max(versions) if len(versions) > 0 else 0

    data = {
        'rates':        rates,
        'max_version':  get_max_version()
    }

    return data, 200


class Hello(Resource):

    @staticmethod
    def get():

        message = 'No action specified.'

        return get_error_response(message)


class Rates(Resource):

    @staticmethod
    def get(currency_code: str):

        rates = DB.get_currency_rates(currency_code)

        return get_rates_response(rates)


class RatesAfterVersion(Resource):

    @staticmethod
    def get(currency_code: str, version: int):

        rates = DB.get_currency_rates(currency_code, version)

        return get_rates_response(rates)


app = Flask(__name__)
api = Api(app)

api.add_resource(
    Hello,
    '/'
)

api.add_resource(
    Rates,
    "/rates/",
    "/rates/<currency_code>/"
)

api.add_resource(
    RatesAfterVersion,
    "/rates/<currency_code>/",
    "/rates/<currency_code>/<int:version>/"
)

CURRENT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
CONFIG = common.get_config(CURRENT_DIRECTORY)
DB = db.CrawlerDB(CONFIG)

if __name__ == '__main__':
    app.run()
