import datetime
import enum

import pymongo.database
import pymongo.mongo_client


class Event(enum.Enum):
    """Enumeration of application's events."""

    NONE = "NONE"
    CURRENT_RATES_LOADING = "CURRENT_RATES_LOADING"
    HISTORICAL_RATES_LOADING = "HISTORICAL_RATES_LOADING"
    CURRENT_RATES_UPDATING = "CURRENT_RATES_UPDATING"
    HISTORICAL_RATES_UPDATING = "HISTORICAL_RATES_UPDATING"


class UAExchangeRatesCrawlerDB:
    __CLIENT: pymongo.MongoClient = None
    __DATABASE: pymongo.database.Database = None
    __HISTORICAL_FILES_COLLECTION: pymongo.collection = None
    __CURRENCY_RATES_COLLECTION: pymongo.collection = None
    __IMPORT_DATES_COLLECTION: pymongo.collection = None
    __EVENTS_COLLECTION: pymongo.collection = None
    __LOGS_COLLECTION: pymongo.collection = None

    def __init__(self, config: dict):

        self.__CLIENT = pymongo.MongoClient(
            config["mongodb_connection_string"],
            serverSelectionTimeoutMS=config["mongodb_max_delay"],
        )

        self.__DATABASE = self.__CLIENT[config["mongodb_database_name"]]

        self.__HISTORICAL_FILES_COLLECTION = self.__DATABASE["historical_files"]
        self.__CURRENCY_RATES_COLLECTION = self.__DATABASE["currency_rates"]
        self.__IMPORT_DATES_COLLECTION = self.__DATABASE["import_dates"]
        self.__EVENTS_COLLECTION = self.__DATABASE["events"]
        self.__LOGS_COLLECTION = self.__DATABASE["logs"]

    def disconnect(self):

        self.__CLIENT.close()

    def get_last_import_date(self) -> datetime.datetime:

        grouping_stage = {"$group": {"_id": "$date"}}

        sorting_stage = {"$sort": {"_id": -1}}

        limiting_stage = {"$limit": 1}

        stages = [grouping_stage, sorting_stage, limiting_stage]
        result = None

        records = list(self.__IMPORT_DATES_COLLECTION.aggregate(stages))

        if len(records) > 0:
            result = records[0]["_id"]

        return result

    def get_logs(self, import_date: datetime.datetime):

        rows_filter = {"import_date": import_date}
        rows_fields = {"_id": 0}

        cursor = self.__LOGS_COLLECTION.find(rows_filter, rows_fields).sort(
            "timestamp", pymongo.ASCENDING
        )

        logs = []

        for log in cursor:
            logs.append(log["text"])

        return logs

    def insert_historical_file(
        self, file_link: str, file_hash: str, import_date: datetime.datetime
    ) -> None:
        query_values = {
            "link": file_link,
            "hash": file_hash,
            "import_date": import_date,
        }

        self.__HISTORICAL_FILES_COLLECTION.insert_one(query_values)

    def update_historical_file(
        self, file_link: str, file_hash: str, import_date: datetime.datetime
    ) -> None:
        query_filter = {"link": file_link}
        query_values = {"$set": {"hash": file_hash, "import_date": import_date}}

        self.__HISTORICAL_FILES_COLLECTION.update_one(query_filter, query_values)

    def historical_file(self, link) -> dict:
        query_filter = {"link": link}
        query_fields = {"_id": 0, "link": 0}

        return self.__HISTORICAL_FILES_COLLECTION.find_one(query_filter, query_fields)

    def get_currency_rates(
        self,
        currency_code: str,
        import_date: datetime.datetime | None,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ):

        matching_stage = {"$match": {"currency_code": {"$eq": currency_code.upper()}}}

        last_import_date = self.get_last_import_date()

        if last_import_date is not None or import_date is not None:
            matching_stage["$match"]["import_date"] = {}

        if last_import_date is not None:
            matching_stage["$match"]["import_date"].update({"$lte": last_import_date})

        if import_date is not None:
            matching_stage["$match"]["import_date"].update({"$gt": import_date})

        if start_date is not None or end_date is not None:

            matching_stage["$match"]["rate_date"] = {}

            if start_date is not None:
                matching_stage["$match"]["rate_date"]["$gte"] = start_date

            if end_date is not None:
                matching_stage["$match"]["rate_date"]["$lte"] = end_date

        grouping_stage = {
            "$group": {
                "_id": "$rate_date",
                "import_date": {
                    "$max": {"import_date": "$import_date", "rate": "$rate"}
                },
            }
        }
        sorting_stage = {"$sort": {"_id": 1}}

        stages = [matching_stage, grouping_stage, sorting_stage]

        rates = []

        cursor = self.__CURRENCY_RATES_COLLECTION.aggregate(stages)

        for rate in cursor:
            rates.append(
                {
                    "import_date": rate["import_date"]["import_date"],
                    "rate_date": rate["_id"],
                    "rate": rate["import_date"]["rate"],
                }
            )

        return rates

    def currency_rate_on_date(
        self, currency_code: str, date: datetime.datetime
    ) -> dict:
        rates = self.get_currency_rates(
            currency_code, import_date=None, start_date=date, end_date=date
        )

        if len(rates) == 0:
            return {
                "currency_code": currency_code,
                "import_date": None,
                "rate_date": date,
                "rate": 0,
            }
        else:
            return rates[0]

    def rate_is_new_or_changed(self, rate: dict) -> bool:

        query = {
            "$and": [
                {"currency_code": {"$eq": rate["currency_code"]}},
                {"rate_date": {"$eq": rate["rate_date"]}},
                {"rate": {"$eq": rate["rate"]}},
            ]
        }

        return self.__CURRENCY_RATES_COLLECTION.count_documents(query) == 0

    def insert_currency_rate(self, rate):
        self.__CURRENCY_RATES_COLLECTION.insert_one(rate)

    def add_logs_entry(self, import_date, timestamp, text):
        self.__LOGS_COLLECTION.insert_one(
            {"import_date": import_date, "timestamp": timestamp, "text": text}
        )

    def insert_import_date(self, date):
        self.__IMPORT_DATES_COLLECTION.insert_one({"date": date})

    def insert_event_rates_updating(
        self,
        event: Event,
        currency_code: str,
        rate_date: datetime.datetime,
        rate_initial: str,
        rate_current: str,
    ):

        self.__EVENTS_COLLECTION.insert_one(
            {
                "event_name": event.value,
                "event_date": datetime.datetime.now(),
                "currency_code": currency_code,
                "rate_date": rate_date,
                "rate_initial": rate_initial,
                "rate_current": rate_current,
            }
        )

    def insert_event_rates_loading(self, event: Event):

        self.__EVENTS_COLLECTION.insert_one(
            {
                "event_name": event.value,
                "event_date": datetime.datetime.now(),
            }
        )

    def get_last_event(self, event: Event):

        query_filter = {"event_name": event.value}
        query_fields = {"_id": 0, "event_name": 0}

        return self.__EVENTS_COLLECTION.find_one(
            query_filter, query_fields, sort=[("event_date", -1)]
        )

    def get_last_rates_updating_event(
        self,
        event: Event,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        currency_code: str,
    ):

        query_filter = {
            "event_name": {"$eq": event.value},
            "event_date": {"$gte": start_date, "$lt": end_date},
            "currency_code": {"$eq": currency_code},
        }

        query_fields = {"_id": 0, "event_name": 0}

        return self.__EVENTS_COLLECTION.find_one(
            query_filter, query_fields, sort=[("event_date", -1)]
        )
