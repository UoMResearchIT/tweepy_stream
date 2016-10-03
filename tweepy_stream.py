from tweepy import Stream
from tweepy import API
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import datetime
import os

# Twitter API credentials
 
ckey = '???'
csecret = '???'
atoken = '???'
asecret = '???'

# Get process id for logging
 
pid = os.getpid()

# Define custom stream listener class, which appends individual tweets to JSON file

class listener (StreamListener):
 
    def on_data (self, data):
        try:
            saveFile = open ('twitterDB.json', 'a')
            saveFile.write (data)
            saveFile.write ('/n')
            saveFile.close ()

            # TODO: check if any limits are returned in data object
            return True
        except Exception, e:
            print '[', pid, ']', datetime.datetime.utcnow(), 'Exception - failed on_data(),', str(e)
            time.sleep(5)
            return False
        except BaseException, e:
            print '[', pid, ']', datetime.datetime.utcnow(), 'BaseException - failed on_data(),', str(e)
            time.sleep(5)
            return False
            
    def on_error(self, status):
        print '[', pid, ']', datetime.datetime.utcnow(), "on_error(): ", status
        return False

# Get OAuthHandler and API using web_proxy and set access tokens
        
auth = OAuthHandler (ckey, csecret)
web_proxy = os.environ['HTTPS_PROXY']
print '[', pid, ']', datetime.datetime.utcnow(), "Using Proxy: ", web_proxy
api = API(auth,proxy=web_proxy)
auth.set_access_token (atoken, asecret)

# Main loop to log status, create stream filter and process errors; will recreate on handled errors

while True:
    print '[', pid, ']', datetime.datetime.utcnow(), " - Obtaining Stream..."
    twitterStream = Stream(auth, listener())
    print '[', pid, ']', datetime.datetime.utcnow(), " - Obtained Stream"

    try:
        print '[', pid, ']', datetime.datetime.utcnow(), " - Starting Filter"
		# hard-coded search terms
        twitterStream.filter(track=["fracking","#CCS","#climatechange","#MMClimateControl"])
        print '[', pid, ']', datetime.datetime.utcnow(), " - Filter exited"

    except Exception, e:
        print '[', pid, ']', datetime.datetime.utcnow(), " - Error: "
        print e.__doc__
        print e.message
        time.sleep(30)
        print '[', pid, ']', datetime.datetime.utcnow(), " - Restarting stream..."
