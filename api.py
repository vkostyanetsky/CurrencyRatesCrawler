#!/usr/bin/env python3

import datetime

from flask import Flask
from flask_restful import Api, Resource

from modules.crawler import Event, UAExchangeRatesCrawler
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
        super().__init__(file, updating_event=Event.NONE)

    def get_error_response_using_date(self, date):
        return self.get_error_response(
            code=3, message=f"Unable to parse a date: {date}"
        )

    def _fill_current_rates_loading_heartbeat(self, heartbeat: dict):

        event_lifespan = self._config.get(
            "heartbeat_current_rates_loading_event_lifespan"
        )
        last_event = self._db.get_last_event(Event.CURRENT_RATES_LOADING)

        if last_event is not None:

            last_event_ttl = round(
                event_lifespan
                - (datetime.datetime.now() - last_event["event_date"]).total_seconds()
            )
            last_event_date = last_event["event_date"].strftime("%Y-%m-%dT%H:%M:%S")

            if last_event_ttl < 0:
                heartbeat["warnings"].append(
                    f"The last current rates loading triggered over {event_lifespan} seconds ago."
                )

        else:

            last_event_date = None
            last_event_ttl = None

            heartbeat["warnings"].append(
                "It is impossible to determine when the last current rates loading has happened."
            )

        heartbeat["last_current_rates_loading_event_date"] = last_event_date
        heartbeat["last_current_rates_loading_event_ttl"] = last_event_ttl

    def _fill_current_rates_updating_heartbeat(self, heartbeat: dict):
        def get_last_weekday():
            date = datetime.datetime.today()
            date -= datetime.timedelta(days=1)
            while date.weekday() > 4:
                date -= datetime.timedelta(days=1)
            return date

        currencies_without_current_rates = []
        currencies_with_current_rates = []

        last_weekday = get_last_weekday()
        currency_codes = self.get_currency_codes()

        for currency_code in currency_codes:

            event = self._db.get_last_rates_updating_event(
                event=Event.CURRENT_RATES_UPDATING, start_date=last_weekday, end_date=datetime.datetime.now(), currency_code=currency_code
            )
            if event is not None:
                currencies_with_current_rates.append(currency_code)
            else:
                currencies_without_current_rates.append(currency_code)

        if currencies_without_current_rates:
            heartbeat["warnings"].append(
                f"At least one currency did not receive current rate update from {last_weekday:%Y-%m-%d}."
            )

        heartbeat["currencies_with_current_rates"] = currencies_with_current_rates
        heartbeat["currencies_without_current_rates"] = currencies_without_current_rates

    def _fill_historical_rates_loading_heartbeat(self, heartbeat: dict):

        event_lifespan = self._config.get(
            "heartbeat_historical_rates_loading_event_lifespan"
        )
        last_event = self._db.get_last_event(Event.HISTORICAL_RATES_LOADING)

        if last_event is not None:

            last_event_ttl = round(
                event_lifespan
                - (datetime.datetime.now() - last_event["event_date"]).total_seconds()
            )
            last_event_date = last_event["event_date"].strftime("%Y-%m-%dT%H:%M:%S")

            if last_event_ttl < 0:
                heartbeat["warnings"].append(
                    f"The last historical rates loading triggered over {event_lifespan} seconds ago."
                )

        else:

            last_event_date = None
            last_event_ttl = None

            heartbeat["warnings"].append(
                "It is impossible to determine when the last historical rates loading has happened."
            )

        heartbeat["last_historical_rates_loading_event_date"] = last_event_date
        heartbeat["last_historical_rates_loading_event_ttl"] = last_event_ttl

    def get_heartbeat(self) -> tuple:

        heartbeat = {"warnings": []}

        self._fill_current_rates_loading_heartbeat(heartbeat)
        self._fill_current_rates_updating_heartbeat(heartbeat)

        self._fill_historical_rates_loading_heartbeat(heartbeat)

        return heartbeat, len(heartbeat["warnings"]) == 0

    @staticmethod
    def get_error_response(code, message):
        data = {"error_message": message, "error_code": code}

        return data, 200

    def get_currency_codes(self) -> list:
        return list(set(list(self._config["currency_codes"].values())))

    def get_currency_rates(
        self,
        currency_code: str,
        import_date: datetime.datetime = None,
        start_date: datetime.datetime = None,
        end_date: datetime.datetime = None,
    ):

        currency_code = currency_code.upper()

        if currency_code not in self.get_currency_codes():

            message = (
                f"Exchange rates for the currency code"
                f' "{currency_code}" cannot be found at UAE CB.'
            )

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


class Heartbeat(Resource):
    @staticmethod
    def get():

        details, success = crawler.get_heartbeat()

        return details, 200 if success else 500


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

api.add_resource(Heartbeat, "/heartbeat/")

api_endpoint_to_get_logs = crawler.get_config_value("api_endpoint_to_get_logs")

if api_endpoint_to_get_logs != "":

    api.add_resource(Logs, f"/{api_endpoint_to_get_logs}/")

    api.add_resource(LogsUsingImportDate, f"/{api_endpoint_to_get_logs}/<import_date>/")

if __name__ == "__main__":
    app.run()
