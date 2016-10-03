import socket
import json
import logging
import os

from tweepy import Stream
from tweepy import API
from tweepy import OAuthHandler

from listener_store import ListenerStoreFile, ListenerStoreMongoDB

# logging settings

logging.basicConfig(level=logging.INFO, format='(%(process)d) %(asctime)s [%(levelname)s]: %(message)s')

# get local hostname, build expected config filename

HOSTNAME = socket.gethostname()
CONFIG_FILE = u"config/" + HOSTNAME + u".json"
WEB_PROXY = os.environ.get('HTTPS_PROXY')

# try to load config; exit on failure
# TODO: check for specific exceptions rather than broad case

config = dict()
try:
    with open(CONFIG_FILE) as json_config_file:
        config = json.load(json_config_file)
except:
    logging.error('Config file ('+CONFIG_FILE+') not found. Exiting.')
    exit()

# check we have a name entry for the config - then assumes other config fields are present
# exit on false
if 'name' in config:
    logging.info('Config: Using ' + config['name'] + '.')
else:
    exit()

# Get OAuthHandler and API using web_proxy and set access tokens

auth = OAuthHandler(config['twitterAPI']['ckey'], config['twitterAPI']['csecret'])
api = API(auth, proxy=WEB_PROXY)
auth.set_access_token(config['twitterAPI']['atoken'], config['twitterAPI']['asecret'])

# -------------------------------------------------------------------------------------------------
# Main loop to log status, create stream filter and process errors; will recreate on handled errors
# -------------------------------------------------------------------------------------------------

if config['output_mode'] == "file":
    logging.info("Using FILE output mode to file:" + config['output_file'])

    while True:

        logging.info("Obtaining Stream...")
        twitterStream = Stream(auth, ListenerStoreFile(config))
        logging.info("Obtained Stream")

        try:
            logging.info("Starting Filter")
            twitterStream.filter(track=config['track_filter'])
            logging.info("Filter exited")

        except Exception as e:
            logging.error(e.__doc__ + e.message)
            time.sleep(10)
            logging.info("Restarting stream...")

elif config['output_mode'] == "mongoDB":
    logging.info("Using MONGODB output mode to server: " + config['mongoDB']['server'] +":" + config['mongoDB']['port'])
    logging.info("Using MONGODB output mode with database: " + config['mongoDB']['database'])
    logging.info("Using MONGODB output mode with collection: " + config['mongoDB']['collection'])

    while True:

        logging.info("Obtaining Stream...")
        twitterStream = Stream(auth, ListenerStoreMongoDB(config))
        logging.info("Obtained Stream")

        try:
            logging.info("Starting Filter")
            twitterStream.filter(track=config['track_filter'])
            logging.info("Filter exited")

        except Exception as e:
            logging.error(e.__doc__ + e.message)
            time.sleep(10)
            logging.info("Restarting stream...")