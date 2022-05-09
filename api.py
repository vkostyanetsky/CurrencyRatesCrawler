import datetime
import modules.crawler

from flask import Flask
from flask_restful import Api
from flask_restful import Resource


def get_date(date_as_string):

    year = int(date_as_string[:4])
    month = int(date_as_string[4:6])
    day = int(date_as_string[6:8])

    if len(date_as_string) > 8:

        hour = int(date_as_string[8:10])
        minute = int(date_as_string[10:12])
        second = int(date_as_string[12:])

    else:

        hour = 0
        minute = 0
        second = 0

    return datetime.datetime(year, month, day, hour, minute, second)


class CrawlerHTTPService(modules.crawler.Crawler):

    def __init__(self, file):

        super().__init__(file)

    def get_error_response_using_exception(self, exception):

        error_message = f"{exception=}"

        return self.get_error_response(error_message)

    def get_error_response_using_date(self, date):

        error_message = "Unable to parse a date: {}".format(date)

        return self.get_error_response(error_message)

    def get_error_response(self, message):

        data = {'error': message}

        return data, 200

    def get_currency_rates(
            self,
            currency_code: str,
            import_date: datetime.datetime = None,
            start_date: datetime.datetime = None,
            end_date: datetime.datetime = None):

        datetime_format_string = "%Y%m%d%H%M%S"
        date_format_string = "%Y%m%d"

        import_dates = []

        rates = self._DB.get_currency_rates(currency_code, import_date, start_date, end_date)

        for rate in rates:

            import_dates.append(rate['import_date'])

            rate.update({
                'import_date': rate['import_date'].strftime(datetime_format_string),
                'rate_date': rate['rate_date'].strftime(date_format_string),
            })

        max_import_date = max(import_dates) if len(import_dates) > 0 else datetime.datetime(1, 1, 1)
        max_import_date = max_import_date.strftime(datetime_format_string)

        data = {
            'rates': rates,
            'max_import_date': max_import_date
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


class RatesUsingCurrencyCode(Resource):

    @staticmethod
    def get(currency_code: str):

        return crawler.get_currency_rates(currency_code)


class RatesUsingCurrencyCodeAndImportDate(Resource):

    @staticmethod
    def get(currency_code: str, import_date: str):

        try:
            import_date = get_date(import_date)
        except ValueError:
            return crawler.get_error_response_using_date(import_date)

        return crawler.get_currency_rates(currency_code, import_date)


class RatesUsingCurrencyCodeAndImportDateAndStartDate(Resource):

    @staticmethod
    def get(currency_code: str, import_date: str, start_date: str):

        try:
            import_date = get_date(import_date)
        except ValueError:
            return crawler.get_error_response_using_date(import_date)

        try:
            start_date = get_date(start_date)
        except ValueError:
            return crawler.get_error_response_using_date(start_date)

        return crawler.get_currency_rates(currency_code, import_date, start_date)


class RatesUsingCurrencyCodeAndImportDateAndStartDateAndEndDate(Resource):

    @staticmethod
    def get(currency_code: str, import_date: str, start_date: str, end_date: str):

        try:
            import_date = get_date(import_date)
        except ValueError:
            return crawler.get_error_response_using_date(import_date)

        try:
            start_date = get_date(start_date)
        except ValueError:
            return crawler.get_error_response_using_date(start_date)

        try:
            end_date = get_date(end_date)
        except ValueError:
            return crawler.get_error_response_using_date(end_date)

        return crawler.get_currency_rates(currency_code, import_date, start_date, end_date)


crawler = CrawlerHTTPService(__file__)

app = Flask(__name__)
api = Api(app)

api.add_resource(
    Hello,
    "/"
)
api.add_resource(
    Currencies,
    "/currencies/"
)
api.add_resource(
    Rates,
    "/rates/"
)
api.add_resource(
    RatesUsingCurrencyCode,
    "/rates/<currency_code>/"
)
api.add_resource(
    RatesUsingCurrencyCodeAndImportDate,
    "/rates/<currency_code>/<import_date>/"
)
api.add_resource(
    RatesUsingCurrencyCodeAndImportDateAndStartDate,
    "/rates/<currency_code>/<import_date>/<start_date>/"
)
api.add_resource(
    RatesUsingCurrencyCodeAndImportDateAndStartDateAndEndDate,
    "/rates/<currency_code>/<import_date>/<start_date>/<end_date>/"
)

if __name__ == '__main__':
    app.run()
