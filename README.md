# sentiment-analyse-twitter-spark-cssandra

## Phase I: Collect tweets for training classifier

### Overview

Using tweepy library in Python to interface with Twitter API an streaming. Define and collect positive tweets with positive emojis. Define and collect negative tweets with negative emojis. The emojis are also part of input terms for twitter stream. Only English tweets will be collelcted. Totally 400 chunk files with each about 12 MB saved in Json format. Totally cost 70 hours, 10 hours for positive tweets and 60 hours for negative tweets, whcih suggesting that people are more likely to use positive emojis.


## Phase II: Sentiment Analysis

### Procedure

#### Clean Data
  
1. Parse the Json files and extract the texts content from tweets. 
2. Tokenize the content with TweetTokenizer module from `NLTK` to handle usernames, hashtags, URLs. Replace the usernames with "USERNAME" and do the same to the other two.
3. Turn all text into lowercase.
4. Remove stop words with `NLTK`.
5. Remove the emojis used as search terms. Since we want to analyze the text only. 
6. Tag each word and leave only adjectives and nouns. This can reduce features.
![run command](https://user-images.githubusercontent.com/5117029/44167566-8ca89180-a09c-11e8-9b67-4e6418c83926.PNG)

![feature word](https://user-images.githubusercontent.com/5117029/44167564-8c0ffb00-a09c-11e8-8cad-3e4acfa33428.PNG)

#### Vectorize Data and Train Classifiers

1. Create frequency distribution of each word left. Use first 2000 highest frequency as the features.
2. Create a list to store the feature vectors. Each tweet is a vector with 2000 feature elements. If a feature word is found in a tweet, then put 1 into its corresponding position.
3. Combine the feature vectors with label, 1 for positive tweet and 0 for negative tweet.
4. Implement Baysian classifier using `sickit-learn`. Use random 80% of training data to train classifier, and the remaining 20% to test.

Pyspark is used in this section to build the frequency distribution and parallelize the task into 8 clusters.
![paraleleize](https://user-images.githubusercontent.com/5117029/44167565-8c0ffb00-a09c-11e8-9fb6-188bcdacf60b.PNG)

## Phase III. Demo

### Overview

Implement a web interface to allow users to search key words. Collect some tweets with the words. The classifier gives each tweet a sentiment score. If it's close to 1 then it's possible to be positive; if it's close to 0 then it's likely to be negative. The results output score for each tweet and average score for all the tweets.

### Intructions to run

Need NLTK Pyspark and Sickit-learn library to run

1. Navigate to the project folder in the terminal.
2. Run `python server.py`
3. Go to http://localhost:8080/index.html in your browser.


## Future Improvements

Allow user to track keywords over time.
Compare different keywords in a table.
Train multiple classifiers for voting.
Minimize feature by removing more useless words to decrease running time. 


