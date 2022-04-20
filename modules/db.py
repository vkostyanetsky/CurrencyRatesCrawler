import datetime

from pymongo import MongoClient
from pymongo import collection as collection


class CrawlerDB:

    DB: object = None
    RATES: collection = None

    def __init__(self, config: dict):

        client = MongoClient(config['mongodb_connection_string'], serverSelectionTimeoutMS=config['mongodb_max_delay'])

        CrawlerDB.DB = client[config['mongodb_database_name']]
        CrawlerDB.RATES = CrawlerDB.DB['currency_rates']

    @classmethod
    def get_currency_rates(cls, currency_code: str, import_date: datetime.datetime = None):

        def get_stage_1():

            stage = {
                '$match':
                    {
                        'currency_code': {'$eq': currency_code.upper()}
                    }
            }

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
        cursor = cls.RATES.aggregate(stages)

        rates = []

        for rate in cursor:

            rates.append({
                'import_date':  rate['import_date']['import_date'],
                'rate_date':    rate['_id'],
                'rate':         rate['import_date']['rate'],
            })

        return rates

    @classmethod
    def add_currency_rates(cls, rates):
        cls.RATES.insert_many(rates)

    @classmethod
    def check_for_ambiguous_currency_rate(cls, rate):

        query = {
            '$and': [
                {'currency_code':   {'$eq': rate['currency_code']}},
                {'rate_date':       {'$eq': rate['rate_date']}},
                {'rate':            {'$ne': rate['rate']}},
            ]}

        if cls.RATES.count_documents(query) > 0:
            # TODO Telegram alert required
            print('?')

