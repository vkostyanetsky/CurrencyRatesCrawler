import pymongo.mongo_client
import pymongo.database
import datetime


class CrawlerDB:
    __CLIENT: pymongo.MongoClient = None
    __DATABASE: pymongo.database.Database = None
    __RATES_COLLECTION: pymongo.collection = None

    def __init__(self, config: dict):

        self.__CLIENT = pymongo.MongoClient(
            config['mongodb_connection_string'],
            serverSelectionTimeoutMS=config['mongodb_max_delay']
        )

        self.__DATABASE = self.__CLIENT[config['mongodb_database_name']]

        self.__RATES_COLLECTION = self.__DATABASE['currency_rates']

    def disconnect(self):

        self.__CLIENT.close()

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

        if import_date is not None:
            """I know about $gt, but for some reason it works as $gte on my MongoDB instance.

            So I use $gte and add 1 seconds, just to make it looks a bit more logical.
            """

            matching_stage['$match']['import_date'] = {'$gte': import_date + datetime.timedelta(seconds=1)}

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

        cursor = self.__RATES_COLLECTION.aggregate(stages)

        for rate in cursor:
            rates.append({
                'import_date': rate['import_date']['import_date'],
                'rate_date': rate['_id'],
                'rate': rate['import_date']['rate'],
            })

        return rates

    def is_currency_rate_to_change(self, rate: dict) -> bool:

        current_rates = self.get_currency_rates(
            rate['currency_code'], import_date=None, start_date=rate['rate_date'], end_date=rate['rate_date']
        )
        rate_for_date = current_rates[0]['rate'] if len(current_rates) > 0 else 0

        return rate_for_date != rate['rate']

    def is_currency_rate_to_add(self, rate: dict) -> bool:

        query = {
            '$and': [
                {'currency_code': {'$eq': rate['currency_code']}},
                {'rate_date': {'$eq': rate['rate_date']}},
                {'rate': {'$eq': rate['rate']}}
            ]}

        return self.__RATES_COLLECTION.count_documents(query) == 0

    def add_currency_rate(self, rate):

        self.__RATES_COLLECTION.insert_one(rate)
