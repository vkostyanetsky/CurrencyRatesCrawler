import datetime
import modules.crawler

from flask import Flask
from flask_restful import Api
from flask_restful import Resource


class CrawlerHTTPService(modules.crawler.Crawler):

    def __init__(self, file):

        super().__init__(file)

    def get_error_response_using_exception(self, exception):

        error_message = f"{exception=}"

        return self.get_error_response(error_message)

    def get_error_response(self, message):

        data = {'error': message}

        return data, 200

    def get_rates_response(self, rates):

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

        return crawler.get_error_response(message)


class Currencies(Resource):

    @staticmethod
    def get():

        data = {
            'currencies': crawler._DB.get_currencies()
        }

        return data, 200


class Rates(Resource):

    @staticmethod
    def get():

        message = 'No currency specified.'

        return crawler.get_error_response(message)


class RatesWithCurrencyCode(Resource):

    @staticmethod
    def get(currency_code: str):

        rates = crawler._DB.get_currency_rates(currency_code)

        return crawler.get_rates_response(rates)


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
            return crawler.get_error_response(error_message)

        rates = crawler._DB.get_currency_rates(currency_code, import_date=date)

        return crawler.get_rates_response(rates)


crawler = CrawlerHTTPService(__file__)

app = Flask(__name__)
api = Api(app)

api.add_resource(
    Hello,
    '/'
)

api.add_resource(
    Currencies,
    "/currencies/", "/currencies/"
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

if __name__ == '__main__':
    app.run()
