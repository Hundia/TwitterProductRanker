import re
import json
import os
import nltk
from nltk.tokenize import word_tokenize
from textblob import TextBlob
from nltk.corpus import stopwords
from string import punctuation

def removeStopWordsAndPunctuation(txt):
    customStopWords = set(stopwords.words('english') + list(punctuation))
    textWithoutStopWords = [word for word in word_tokenize(txt) if word not in customStopWords]
    newText = ''
    for word in textWithoutStopWords:
        newText += word + ' '
    return newText

# Function that cleans tweets
def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w +:\ / \ / \S +)", " ", tweet).split())

# Function extracts what we need from tweets
def parse_json_tweet(line):
    tweet = json.loads(line)
    year = 2016
    month = 1
    cleanedBodyText = TextBlob(clean_tweet(tweet['body']))

    wordTokens = word_tokenize(removeStopWordsAndPunctuation(cleanedBodyText.string))

    return [year, month, wordTokens]

import codecs

def printWordTokens(words):
    for s in words:
        print(s);
file_timeordered_json_tweets = codecs.open(r'D:\Dev\TwitterProductRanking\Project\TwitterExamples\tweets.txt', 'r', 'utf-8')
	# fout = codecs.open(sys.argv[2], 'w', 'utf-8')

	#efficient line-by-line read of big files
for line in file_timeordered_json_tweets:
    tweet = json.loads(line)

    [year, month, wordTokens] = parse_json_tweet(line)

    print('------------------------------')
    print('year: ' + str(year) + ' month: ' + str(month) + ' wordTokens: ')

    printWordTokens(wordTokens)



print('-----------------------!!!!!!!!!!!')


#
# tw = "@SNFonNBC: A few more hours Tom, just a few more! #2015\n\n(via Tom Brady) http://t.co/wEOTiUGkW7”\n\nWhat. The. Fuck? RT"
#

