import datetime
import os
import requests

import logging
import logging.config

from modules.db import CrawlerDB

from logging.handlers import TimedRotatingFileHandler


class TelegramMessageHandler(logging.Handler):
    BOT_API_TOKEN: str = ""
    CHAT_ID = ""

    def __init__(self, telegram_bot_api_token, telegram_chat_id):

        super().__init__()

        self.BOT_API_TOKEN = telegram_bot_api_token
        self.CHAT_ID = telegram_chat_id

    def emit(self, record):

        try:

            url = "https://api.telegram.org/bot{}/sendMessage".format(self.BOT_API_TOKEN)

            data = {
                'chat_id': self.CHAT_ID,
                'text': self.format(record)
            }

            requests.post(url, data)

        except (KeyboardInterrupt, SystemExit):

            raise

        except Exception:

            self.handleError(record)


class MongoDBRecordHandler(logging.Handler):
    __IMPORT_DATE: datetime.datetime
    __DATABASE: CrawlerDB

    def __init__(self, import_date, database):

        super().__init__()

        self.__IMPORT_DATE = import_date
        self.__DATABASE = database

    def emit(self, record):

        try:

            self.__DATABASE.add_logs_entry(self.__IMPORT_DATE, record.created, self.format(record))

        except (KeyboardInterrupt, SystemExit):

            raise

        except Exception:

            self.handleError(record)


def get_logger(name, config, current_directory, import_date, database):
    def get_timed_rotating_file_handler():

        dir_path = os.path.join(current_directory, "logs")

        try:
            os.makedirs(dir_path, exist_ok=True)
        except OSError:
            pass

        file_path = os.path.join(dir_path, "{}.log".format(name))

        handler = TimedRotatingFileHandler(file_path, when="d", interval=1, backupCount=30)

        log_format = f"%(asctime)s [%(levelname)s] (%(filename)s).%(funcName)s(%(lineno)d) %(message)s"
        formatter = logging.Formatter(log_format)

        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)

        return handler

    def get_stream_handler():

        handler = logging.StreamHandler()

        log_format = f"%(asctime)s [%(levelname)s] %(funcName)s(%(lineno)d) %(message)s"
        formatter = logging.Formatter(log_format)

        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)

        return handler

    def get_telegram_message_handler():

        handler = TelegramMessageHandler(config['telegram_bot_api_token'], config['telegram_chat_id'])

        log_format = "%(message)s"
        formatter = logging.Formatter(log_format)

        handler.setFormatter(formatter)
        handler.setLevel(logging.INFO)

        return handler

    def get_mongodb_handler():

        handler = MongoDBRecordHandler(import_date, database)

        log_format = f"%(asctime)s [%(levelname)s] %(funcName)s(%(lineno)d) %(message)s"
        formatter = logging.Formatter(log_format)

        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)

        return handler

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(get_stream_handler())
    logger.addHandler(get_timed_rotating_file_handler())

    if config['telegram_bot_api_token'] != "" and config['telegram_chat_id'] != 0:
        logger.addHandler(get_telegram_message_handler())

    logger.addHandler(get_mongodb_handler())

    return logger
