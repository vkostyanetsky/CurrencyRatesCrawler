import logging
import logging.config
from logging.handlers import TimedRotatingFileHandler
import requests
import os


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


def get_logger(name, config, current_directory):

    def get_timed_rotating_file_handler():

        file_path = os.path.join(current_directory, "logs", "{}.log".format(name))

        handler = TimedRotatingFileHandler(file_path, when="d", interval=1, backupCount=30)

        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(log_format))

        return handler

    def get_stream_handler():

        stream_handler = logging.StreamHandler()

        stream_handler.setLevel(logging.DEBUG)
        stream_handler.setFormatter(logging.Formatter(log_format))

        return stream_handler

    def get_telegram_message_handler():

        telegram_message_handler = TelegramMessageHandler(config['telegram_bot_api_token'], config['telegram_chat_id'])

        telegram_message_handler.setLevel(logging.INFO)
        telegram_message_handler.setFormatter(logging.Formatter(log_format))

        return telegram_message_handler

    log_format = f"%(asctime)s [%(levelname)s] (%(filename)s).%(funcName)s(%(lineno)d) %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(get_stream_handler())
    logger.addHandler(get_telegram_message_handler())
    logger.addHandler(get_timed_rotating_file_handler())

    return logger
