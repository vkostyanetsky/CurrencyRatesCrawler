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

    def get_currencies(self):

        cursor = self.__RATES_COLLECTION.aggregate([{
            '$group': {'_id': '$currency_code'}
        }])

        currencies = []

        for record in cursor:
            currencies.append(record['_id'])

        return currencies

    def get_currency_rates(
            self,
            currency_code: str,
            import_date: datetime.datetime = None,
            rate_date: datetime.datetime = None):

        def get_stage_1():

            stage = {
                '$match':
                    {
                        'currency_code': {'$eq': currency_code.upper()}
                    }
            }

            if rate_date is not None:

                stage['$match']['rate_date'] = {'$eq': rate_date}

            if import_date is not None:

                """I know about $gt, but for some reason it works as $gte on my MongoDB instance.
                
                So I use $gte and add 1 seconds, just to make it looks a bit more logical.
                """

                stage['$match']['import_date'] = {'$gte': import_date + datetime.timedelta(seconds=1)}

            return stage

        def get_stage_2():

            return {
                '$group':
                    {
                        '_id': '$rate_date',
                        'import_date':
                            {
                                '$max':
                                    {
                                        'import_date':  '$import_date',
                                        'rate':         '$rate'
                                    }
                            },
                    }
            }

        def get_stage_3():

            return {
                '$sort':
                    {
                        '_id': 1
                    }
            }

        stage_1 = get_stage_1()
        stage_2 = get_stage_2()
        stage_3 = get_stage_3()

        stages = [stage_1, stage_2, stage_3]

        rates = []

        cursor = self.__RATES_COLLECTION.aggregate(stages)

        for rate in cursor:

            rates.append({
                'import_date':  rate['import_date']['import_date'],
                'rate_date':    rate['_id'],
                'rate':         rate['import_date']['rate'],
            })

        return rates

    def is_currency_rate_to_change(self, rate: dict) -> bool:

        current_rates = self.get_currency_rates(rate['currency_code'], rate_date=rate['rate_date'])
        rate_for_date = current_rates[0]['rate'] if len(current_rates) > 0 else 0

        return rate_for_date != rate['rate']

    def is_currency_rate_to_add(self, rate: dict) -> bool:

        query = {
            '$and': [
                {'currency_code':   {'$eq': rate['currency_code']}},
                {'rate_date':       {'$eq': rate['rate_date']}},
                {'rate':            {'$eq': rate['rate']}}
            ]}

        return self.__RATES_COLLECTION.count_documents(query) == 0

    def add_currency_rate(self, rate):

        self.__RATES_COLLECTION.insert_one(rate)
