# -*- coding: utf-8 -*-

import nltk
nltk.data.path.append("/media/lwang031/D/nltk_data/")
from nltk.tokenize import word_tokenize, TweetTokenizer
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

import re
import json
import string
import pickle
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import pyspark.mllib.regression as mllib_reg
import pyspark.mllib.linalg as mllib_lalg
import pyspark.mllib.classification as mllib_class
import pyspark.mllib.tree as mllib_tree

from pyspark import SparkConf
from pyspark import SparkContext

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener


from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from os import curdir, sep

from urlparse import urlparse

ckey="2mrZ03hb8Gkj7B4QELnPHG8dk"
csecret="iCIcdpaaru2USDnUZNj4pVeuqWFCzkrsEKZqNo7Es1vt1J9BWi"
atoken="3060718058-JKMeKV3vsPMxRi0FJ5I8j0Ijudv9tuw9XMwNi9v"
asecret="rOMFdpCGnDKjErrKogzDfEQ86BWB7QF5GPE5QAhWZpOWf"



conf = SparkConf()
conf.setAppName('spark-nltk')

sc = SparkContext("local[*]",conf = conf) # change '#' of workers 

PUNCTUATION = set(string.punctuation)
STOPWORDS = set(stopwords.words('english'))
STOPWORDS.add(u"\U0001f601")#"😁")
STOPWORDS.add(u"\U0001f602")#"😂")
STOPWORDS.add(u"\U0001f603")#"😃")
STOPWORDS.add(u"\U0001f604")#"😄")
STOPWORDS.add(u"\U0001f606")#"😆")
STOPWORDS.add(u"\U0001f60a")#"😊")
STOPWORDS.add(u"\U0001f60d")#"😍")
STOPWORDS.add(u"\U0001f613")#"😓")
STOPWORDS.add(u"\U0001f61e")#"😞")
STOPWORDS.add(u"\U0001f620")#"😠")
STOPWORDS.add(u"\U0001f621")#"😡")
STOPWORDS.add(u"\U0001f623")#"😣")
STOPWORDS.add(u"\U0001f629")#"😩")

STEMMER = PorterStemmer()
twtTok = TweetTokenizer()	


terms = []
tweets = []
nTweets = 0

featureWords = pickle.load(open("featureList.p", "rb"))

class listener(StreamListener):#this is a class that contains functions for dealing with the incoming tweets.

	def on_data(self, data):#this function runs everytime a new tweet arrives
		global nTweets

		try:#this is encapsulated in a try.catch block in order to keep the stream going even if there was an error on one of the tweets.
			tweet = json.loads(data)#the tweets come in json format. this converts the json into a python dictionary.
			if "text" in tweet.keys():
				text = tweet["text"]
				text = text.replace('\n', '').replace('\r', '')
				tweets.append(text)
				print "X", text
			if len(tweets)>=nTweets:
				return(False)
			else:
				return(True)

		except Exception as e:#if there was an error
			print >> sys.stderr, "ERR:", str(e)#print err to stderr

	def on_error(self, status):#i dont really know what this does but it was in the examlpe to i put it in.
		print status

auth = OAuthHandler(ckey, csecret)#authorize the app with twitter
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())#start the stream with a listener object.






# Function to break text into "tokens", lowercase them, remove punctuation and stopwords, and stem them
def clean(text):
	filteredText = re.sub(r"http\S+", "URL", text)
	filteredText = re.sub(r"RT", "", filteredText)

	tokens = twtTok.tokenize(filteredText)
	
	lowercased = [t.lower() for t in tokens]
	no_stopwords = [w for w in lowercased if not w in STOPWORDS]
	stemmed = [w for w in no_stopwords]#[STEMMER.stem(w) for w in no_stopwords]
	return [k for k in nltk.pos_tag([w for w in stemmed if w]) if k[1][0]=='N' or k[1][0]=='J' ]



def toFeatures(words):
	features = []
	for f in featureWords:
		if f[0] in words:
			features.append(1)
		else:
			features.append(0)
	return features



def tokenize(text):
	return word_tokenize(text)

def pos_tag(x):
    return nltk.pos_tag([x])
    
def wCount(x):
	return [(w,1) for w in x]




nbay = mllib_class.NaiveBayesModel.load(sc, "nbay-l1.00") #LOAD CLASSIFIER




PORT_NUMBER = 8080

#This class will handles any incoming request from
#the browser 
class myHandler(BaseHTTPRequestHandler):

	#Handler for the GET requests
	def do_GET(self):
		global nTweets
		global tweets
		global terms

		print self.path


		gotoIndex = False
		if self.path=="/index.html":
			print "YOYUO"
			gotoIndex = True
		else:
			actualPath = urlparse(self.path).path
			query = urlparse(self.path).query
			query_components = dict(qc.split("=") for qc in query.split("&"))
			searchTerm = query_components["term"]
			nTweets = int(query_components["nTweets"])
			


			##################DO DATA PROCESSING






			#######################################

		try:
			#set the right mime type

			if gotoIndex:
				print "HERE"
				f = open(curdir + sep + "index.html")
				mimetype = 'text/html'
				self.send_response(200)
				self.send_header('Content-type',mimetype)
				self.end_headers()
				self.wfile.write(f.read())
				f.close()
			else:

				terms = searchTerm.split('+')#[searchTerm]
				for t in terms:
					print t
				tweets = []
				twitterStream.filter( languages=["en"], track=terms)#filter the stream according to language and our search terms

				sentSum = 0
				classifiedTweets = []
				for t in tweets:
					vec = toFeatures(clean(t))
					sent = nbay.predict(vec)
					sentSum += sent
					classifiedTweets.append([t,sent])

				sentSum /= len(classifiedTweets)

				htmlStr = ""
				htmlStr += "Search Term: "
				htmlStr += searchTerm
				htmlStr += "<br>"
				htmlStr += "Average Sentiment: "
				htmlStr += str(sentSum)
				htmlStr += "<br>"


				for c in classifiedTweets:
					print c[0], c[1]
					if c[1]==1:
						htmlStr += "<font color=\"green\">"
					else:
						htmlStr += "<font color=\"red\">"

					htmlStr += c[0]
					htmlStr += "<br>"
					htmlStr +="</font><br>"



				#Open the static file requested and send it
				#f = open(curdir + sep + actualPath)#self.path) 
				mimetype = 'text/html'
				self.send_response(200)
				self.send_header('Content-type',mimetype)
				self.end_headers()
				self.wfile.write(htmlStr)
			
				
			return


		except IOError:
			self.send_error(404,'File Not Found: %s' % self.path)

try:
    #Create a web server and define the handler to manage the
    #incoming request
    server = HTTPServer(('', PORT_NUMBER), myHandler)
    print 'Started httpserver on port ' , PORT_NUMBER

    #Wait forever for incoming htto requests
    server.serve_forever()

except KeyboardInterrupt:
    print '^C received, shutting down the web server'
    server.socket.close()