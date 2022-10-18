#!/usr/bin/env python3

import datetime

from flask import Flask
from flask_restful import Api, Resource

from modules.crawler import UAExchangeRatesCrawler

from version import __version__


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


class CrawlerHTTPService(UAExchangeRatesCrawler):
    def __init__(self, file):
        super().__init__(file)

    def get_error_response_using_date(self, date):
        return self.get_error_response(
            code=3, message=f"Unable to parse a date: {date}"
        )

    @staticmethod
    def get_error_response(code, message):
        data = {"error_message": message, "error_code": code}

        return data, 200

    def get_currency_codes(self) -> list:
        return list(self._config["currency_codes"].values())

    def get_currency_rates(
        self,
        currency_code: str,
        import_date: datetime.datetime = None,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
    ):

        currency_code = currency_code.upper()

        if currency_code not in self.get_currency_codes():

            message = f'Exchange rates for the currency code' \
                      f' "{currency_code}" cannot be found at UAE CB.'

            return self.get_error_response(code=4, message=message)

        else:

            datetime_format_string = "%Y%m%d%H%M%S"
            date_format_string = "%Y%m%d"

            import_dates = []

            rates = self._db.get_currency_rates(
                currency_code, import_date, start_date, end_date
            )

            for rate in rates:
                import_dates.append(rate["import_date"])

                rate.update(
                    {
                        "import_date": rate["import_date"].strftime(
                            datetime_format_string
                        ),
                        "rate_date": rate["rate_date"].strftime(date_format_string),
                    }
                )

            max_import_date = (
                max(import_dates)
                if len(import_dates) > 0
                else datetime.datetime(1, 1, 1)
            )
            max_import_date = max_import_date.strftime(datetime_format_string)

            data = {"rates": rates, "max_import_date": max_import_date}

            return data, 200

    def get_logs(self, import_date: datetime.datetime):

        data = {"logs": self._db.get_logs(import_date)}

        return data, 200


class Hello(Resource):
    @staticmethod
    def get():
        return crawler.get_error_response(code=1, message="No action specified.")


class Info(Resource):
    @staticmethod
    def get():
        return {"version": __version__}, 200


class Currencies(Resource):
    @staticmethod
    def get():
        data = {"currencies": crawler.get_currency_codes()}

        return data, 200


class Rates(Resource):
    @staticmethod
    def get():
        return crawler.get_error_response(code=2, message="No currency specified.")


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

        return crawler.get_currency_rates(
            currency_code, import_date, start_date, end_date
        )


class Logs(Resource):
    @staticmethod
    def get():
        return crawler.get_error_response(code=15, message="No import date specified.")


class LogsUsingImportDate(Resource):
    @staticmethod
    def get(import_date: str):
        try:
            import_date = get_date(import_date)
        except ValueError:
            return crawler.get_error_response_using_date(import_date)

        return crawler.get_logs(import_date)


crawler = CrawlerHTTPService(__file__)

app = Flask(__name__)
api = Api(app)

api.add_resource(Hello, "/")

api.add_resource(Info, "/info/")

api.add_resource(Currencies, "/currencies/")

api.add_resource(Rates, "/rates/")

api.add_resource(RatesUsingCurrencyCode, "/rates/<currency_code>/")

api.add_resource(
    RatesUsingCurrencyCodeAndImportDate, "/rates/<currency_code>/<import_date>/"
)

api.add_resource(
    RatesUsingCurrencyCodeAndImportDateAndStartDate,
    "/rates/<currency_code>/<import_date>/<start_date>/",
)

api.add_resource(
    RatesUsingCurrencyCodeAndImportDateAndStartDateAndEndDate,
    "/rates/<currency_code>/<import_date>/<start_date>/<end_date>/",
)

api_endpoint_to_get_logs = crawler.get_config_value("api_endpoint_to_get_logs")

if api_endpoint_to_get_logs != "":

    api.add_resource(Logs, f"/{api_endpoint_to_get_logs}/")

    api.add_resource(LogsUsingImportDate, f"/{api_endpoint_to_get_logs}/<import_date>/")

if __name__ == "__main__":
    app.run()
