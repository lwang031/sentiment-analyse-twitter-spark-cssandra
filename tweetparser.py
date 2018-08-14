# -*- coding: utf-8 -*-

import nltk
nltk.data.path.append("/media/lwang031/D/nltk_data/")
from nltk.tokenize import word_tokenize, TweetTokenizer
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer

import re
import json
import string

from pyspark import SparkConf
from pyspark import SparkContext


load = False


conf = SparkConf()
conf.setAppName('spark-nltk')

sc = SparkContext("local[*]",conf = conf) # change '#' of workers 
#("local[4]", "App Name", pyFiles=['MyFile.py', 'lib.zip', 'app.egg'])


#data_raw = sc.textFile('file:///media/lwang031/D/corpora/tweets/negative-cleanhuge.txt')#large file with 2.5 GB, test #workers
 #small file to test outputs


# Module-level global variables for the `tokenize` function below
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

# Function to break text into "tokens", lowercase them, remove punctuation and stopwords, and stem them
def clean(text):
	filteredText = re.sub(r"http\S+", "URL", text)
	filteredText = re.sub(r"RT", "", filteredText)

	tokens = twtTok.tokenize(filteredText)
	
	lowercased = [t.lower() for t in tokens]
	no_stopwords = [w for w in lowercased if not w in STOPWORDS]
	stemmed = [w for w in no_stopwords]#[STEMMER.stem(w) for w in no_stopwords]
	return [k for k in nltk.pos_tag([w for w in stemmed if w]) if k[1][0]=='N' or k[1][0]=='J' ]



def toFeatures(words, pol):
	features = []
	for f in featureWords:
		if f[0] in words:
			features.append(1)
		else:
			features.append(0)

	features.append(pol)
	return features



def tokenize(text):
	return word_tokenize(text)

def pos_tag(x):
    return nltk.pos_tag([x])
    
def wCount(x):
	return [(w,1) for w in x]
	

#data_raw_n = sc.textFile('file:///home/lwang031/tweetparser/negative-clean.txt')
#data_raw_p = sc.textFile('file:///home/lwang031/tweetparser/positive-clean.txt')

print "LOADING FILES"

data_raw_n = sc.textFile('file:///media/lwang031/D1/nltk_data/corpora/tweets/negative_big_clean.txt')
data_raw_p = sc.textFile('file:///media/lwang031/D1/nltk_data/corpora/tweets/positive_big_clean.txt')

print "PARSING JSON"

data_n = data_raw_n.map(lambda line: json.loads(line))
data_pared_n = data_n.map(lambda line: (0, line['text']))

data_p = data_raw_p.map(lambda line: json.loads(line))
data_pared_p = data_p.map(lambda line: (1, line['text']))

data_pared = data_pared_n.union(data_pared_p)
#print data_pared.take(10)

#print "BUILDING FREQUENCY DISTRIBUTION"

### make frequency distribution
data_tok = data_pared.map(lambda (label, text): (clean(text)))
data_words = data_tok.flatMap(wCount)
freqDist = data_words.reduceByKey(lambda a,b: a+b)
#print freqDist.takeOrdered(50, lambda x: -x[1])
############################

print "COUNTING"
print freqDist.count()
print "DONE COUNTING!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
featureWords = freqDist.takeOrdered(16000, lambda x: -x[1])
import pickle
pickle.dump(featureWords, open("featureList.p", "wb"))



print "MAKING FEATURE VECTORS"
### make feauture vectors
data_cleaned = data_pared.map(lambda (label, text): (label, clean(text)))
#print data_cleaned.take(1)

feature_vectors = data_cleaned.map(lambda (label, words): toFeatures(words, label))
#print feature_vectors.take(1)

'''
import matplotlib.pyplot as plt
import numpy as np

classes = feature_vectors.map( lambda fields : fields[-1])
#print sorted(classes.distinct().collect())




counts_by_class = sorted(classes.countByValue().items())
print counts_by_class
x_axis = np.array([tup[0] for tup in counts_by_class])
y_axis = np.array([tup[1] for tup in counts_by_class])

pos = np.arange(len(x_axis))
width = 1.0
ax = plt.axes()
ax.set_xticks(pos + (width / 2))
ax.set_xticklabels(x_axis)

plt.bar(pos, y_axis, width = width, color='lightblue')
fig = plt.gcf()
fig.set_size_inches(12, 7)

plt.plot()
plt.show()

'''


import pyspark.mllib.regression as mllib_reg
import pyspark.mllib.linalg as mllib_lalg
import pyspark.mllib.classification as mllib_class
import pyspark.mllib.tree as mllib_tree


print "CONVERTING FEATURE VECTOR FORMAT TO LabeledPoint"
labeled_data = feature_vectors.map(lambda fields: mllib_reg.LabeledPoint(fields[-1], mllib_lalg.Vectors.dense(fields[:-1])))

print "SPLITTING TRAINING AND TESTING DATA"
train, test = labeled_data.randomSplit([0.7, 0.3], seed = 13)

# parameters:
lamda = 1.0

print "TRAINING"

if load:
	nbay = mllib_class.NaiveBayesModel.load(sc, "nbay-l1.00") #LOAD CLASSIFIER
# initialize classifier:
else:
	nbay = mllib_class.NaiveBayes.train(train, lamda) #TRAIN CLASSIFIER

print "TESTING CLASSIFIER"
# Make prediction and test accuracy.
predictionAndLabel = test.map(lambda p : (nbay.predict(p.features), p.label))
testErr = predictionAndLabel.filter(lambda (v, p): v != p).count() / float(test.count())
accuracy = 100.0 * predictionAndLabel.filter(lambda (x, v): x == v).count() / test.count()

print('lamda=%.2f, Accuracy=%.2f%%' % (lamda, accuracy))
#Save model:
nbay.save(sc, "nbay-l%.2f" % lamda)
#load model:

