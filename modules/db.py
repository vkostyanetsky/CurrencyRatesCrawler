import pymongo.mongo_client
import pymongo.database
import datetime


class CrawlerDB:
    __CLIENT: pymongo.MongoClient = None
    __DATABASE: pymongo.database.Database = None
    __HISTORICAL_FILES_COLLECTION: pymongo.collection = None
    __CURRENCY_RATES_COLLECTION: pymongo.collection = None
    __IMPORT_DATES_COLLECTION: pymongo.collection = None
    __LOGS_COLLECTION: pymongo.collection = None

    def __init__(self, config: dict):

        self.__CLIENT = pymongo.MongoClient(
            config['mongodb_connection_string'],
            serverSelectionTimeoutMS=config['mongodb_max_delay']
        )

        self.__DATABASE = self.__CLIENT[config['mongodb_database_name']]

        self.__HISTORICAL_FILES_COLLECTION = self.__DATABASE['historical_files']
        self.__CURRENCY_RATES_COLLECTION = self.__DATABASE['currency_rates']
        self.__IMPORT_DATES_COLLECTION = self.__DATABASE['import_dates']
        self.__LOGS_COLLECTION = self.__DATABASE['logs']

    def disconnect(self):

        self.__CLIENT.close()

    def get_last_import_date(self) -> datetime.datetime:

        grouping_stage = {
            '$group':
                {
                    '_id': '$date'
                }
        }

        sorting_stage = {
            '$sort':
                {
                    '_id': -1
                }
        }

        limiting_stage = {
            '$limit': 1
        }

        stages = [grouping_stage, sorting_stage, limiting_stage]
        result = None

        records = list(self.__IMPORT_DATES_COLLECTION.aggregate(stages))

        if len(records) > 0:
            result = records[0]['_id']

        return result

    def get_logs(self, import_date: datetime.datetime):

        logs = []

        cursor = self.__LOGS_COLLECTION.find({'import_date': import_date}, {'_id': 0}).sort("timestamp", pymongo.ASCENDING)

        for log in cursor:
            logs.append(log['text'])

        return logs

    def insert_historical_file(self, file_link: str, file_hash: str, import_date: datetime.datetime) -> None:
        query_values = {
            'link': file_link,
            'hash': file_hash,
            'import_date': import_date,
        }

        self.__HISTORICAL_FILES_COLLECTION.insert_one(query_values)

    def update_historical_file(self, file_link: str, file_hash: str, import_date: datetime.datetime) -> None:
        query_filter = {'link': file_link}
        query_values = {
            "$set": {
                'hash': file_hash,
                'import_date': import_date
            }
        }

        self.__HISTORICAL_FILES_COLLECTION.update_one(query_filter, query_values)

    def historical_file(self, link) -> dict:
        query_filter = {'link': link}
        query_fields = {'_id': 0, 'link': 0}

        return self.__HISTORICAL_FILES_COLLECTION.find_one(query_filter, query_fields)

    def get_currency_rates(
            self,
            currency_code: str,
            import_date: datetime.datetime,
            start_date: datetime.datetime,
            end_date: datetime.datetime):

        matching_stage = {
            '$match':
                {
                    'currency_code': {'$eq': currency_code.upper()}
                }
        }

        last_import_date = self.get_last_import_date()

        if last_import_date is not None or import_date is not None:
            matching_stage['$match']['import_date'] = {}

        if last_import_date is not None:
            matching_stage['$match']['import_date'].update({'$lte': last_import_date})

        if import_date is not None:
            matching_stage['$match']['import_date'].update({'$gt': import_date})

        if start_date is not None or end_date is not None:

            matching_stage['$match']['rate_date'] = {}

            if start_date is not None:
                matching_stage['$match']['rate_date']['$gte'] = start_date

            if end_date is not None:
                matching_stage['$match']['rate_date']['$lte'] = end_date

        grouping_stage = {
            '$group':
                {
                    '_id': '$rate_date',
                    'import_date':
                        {
                            '$max':
                                {
                                    'import_date': '$import_date',
                                    'rate': '$rate'
                                }
                        },
                }
        }
        sorting_stage = {
            '$sort':
                {
                    '_id': 1
                }
        }

        stages = [matching_stage, grouping_stage, sorting_stage]

        rates = []

        cursor = self.__CURRENCY_RATES_COLLECTION.aggregate(stages)

        for rate in cursor:
            rates.append({
                'import_date': rate['import_date']['import_date'],
                'rate_date': rate['_id'],
                'rate': rate['import_date']['rate'],
            })

        return rates

    def currency_rate_on_date(self, currency_code: str, date: datetime.datetime) -> dict:
        rates = self.get_currency_rates(currency_code, import_date=None, start_date=date, end_date=date)

        if len(rates) == 0:
            return {
                'currency_code': currency_code,
                'import_date': None,
                'rate_date': date,
                'rate': 0
            }
        else:
            return rates[0]

    def rate_is_new_or_changed(self, rate: dict) -> bool:

        query = {
            '$and': [
                {'currency_code': {'$eq': rate['currency_code']}},
                {'rate_date': {'$eq': rate['rate_date']}},
                {'rate': {'$eq': rate['rate']}}
            ]}

        return self.__CURRENCY_RATES_COLLECTION.count_documents(query) == 0

    def insert_currency_rate(self, rate):
        self.__CURRENCY_RATES_COLLECTION.insert_one(rate)

    def add_logs_entry(self, import_date, timestamp, text):
        self.__LOGS_COLLECTION.insert_one({
            'import_date': import_date,
            'timestamp': timestamp,
            'text': text
        })

    def insert_import_date(self, date):
        self.__IMPORT_DATES_COLLECTION.insert_one({'date': date})
