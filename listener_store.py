import logging
import time
import json
from tweepy.streaming import StreamListener
from pymongo import MongoClient

# Define custom stream listener class, which appends individual tweets to JSON file
# TODO: rename this ListenerStoreFile; then add ListenerStoreMongo class
# TODO: add mode to config to select mode


class ListenerStore(StreamListener):

    def __init__(self, config):
        self.config = config
        self.reconnect_flag = False
        self.reset_reconnect_timer()

    def ramp_reconnect_timer(self):
        self.reconnect_flag = True
        self.reconnect_time *= 2
        self.reconnect_time = min(self.reconnect_time, self.config['twitterAPI']['max_reconnect_time'])
        logging.info("Ramping reconnect time: " + self.reconnect_time)

    def reset_reconnect_timer(self):
        self.reconnect_flag = False
        self.reconnect_time = self.config['twitterAPI']['default_reconnect_time']
        logging.info("Reset reconnect time: " + self.reconnect_time)


class ListenerStoreFile(ListenerStore):
    def __init__(self, config):
        ListenerStore.__init__(self, config)

    def on_data(self, data):
        try:
            # save data to output file specified in config
            save_file = open(self.config['output_file'], 'a')
            save_file.write(data)
            save_file.close()
            if self.reconnect_flag:
                self.reset_reconnect_timer()

            return True
        except Exception as e:
            logging.warn('Exception - failed on_data(): ' + str(e))
            time.sleep(self.reconnect_time)
            self.ramp_reconnect_timer()

            return False
        except BaseException as e:
            logging.warn('BaseException - failed on_data(): ' + str(e))
            time.sleep(self.reconnect_time)
            self.ramp_reconnect_timer()

            return False

    def on_error(self, status):
        logging.error('on_error(): ' + str(status))
        time.sleep(self.reconnect_time)
        self.ramp_reconnect_timer()

        return False


class ListenerStoreMongoDB(ListenerStore):
    def __init__(self, config):
        ListenerStore.__init__(self, config)
        self.client = MongoClient(self.config['mongoDB']['server'], int(self.config['mongoDB']['port']))
        self.db = self.client[self.config['mongoDB']['database']]
        self.collection = self.db[self.config['mongoDB']['collection']]

    def on_data(self, data):
        try:
            # save data to MongoDB specified in config
            tweet = json.loads(data)
            self.collection.insert(tweet)

            if self.reconnect_flag:
                self.reset_reconnect_timer()

            return True
        except Exception as e:
            logging.warn('Exception - failed on_data(): ' + str(e))
            time.sleep(self.reconnect_time)
            self.ramp_reconnect_timer()

            return False
        except BaseException as e:
            logging.warn('BaseException - failed on_data(): ' + str(e))
            time.sleep(self.reconnect_time)
            self.ramp_reconnect_timer()

            return False

    def on_error(self, status):
        logging.error('on_error(): ' + str(status))
        time.sleep(self.reconnect_time)
        self.ramp_reconnect_timer()

        return False