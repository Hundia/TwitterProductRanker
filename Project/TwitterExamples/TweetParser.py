import re
import json
import os
import nltk
from nltk.tokenize import word_tokenize
from textblob import TextBlob
from nltk.corpus import stopwords
from string import punctuation
from nltk.sentiment.vader import SentimentIntensityAnalyzer

sid = SentimentIntensityAnalyzer()

# Function that removes stopwords in english and punctuations
def removeStopWordsAndPunctuation(txt):
    customStopWords = set(stopwords.words('english') + list(punctuation))
    textWithoutStopWords = [word for word in word_tokenize(txt) if word not in customStopWords]
    newText = ''
    for word in textWithoutStopWords:
        newText += word + ' '
    return newText

# Function that cleans tweets from hashtags and symbols
def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w +:\ / \ / \S +)", " ", tweet).split())

# Function extracts what we need from a line of a given tweet
def parse_json_tweet(line):
    tweet = json.loads(line)
    # TODO - year and month extract from field "Posted Time"
    year = 2016
    month = 1

    # Clean up the body of the text to be a regular text
    cleanedBodyText = TextBlob(clean_tweet(tweet['body']))

    wordTokens = word_tokenize(removeStopWordsAndPunctuation(cleanedBodyText.string))

    ss = sid.polarity_scores(cleanedBodyText.string)
    for k in sorted(ss):
        print('{0}: {1}, '.format(k, ss[k]), end='')

    # Here add code that decides if the line is positive / nuetral / negative, and return the result
    # 0 for negative, 1 for positive, 2 for neutral
    semanticResult = 1
    return [year, month, wordTokens, semanticResult]

import codecs

# Usefull function to print out tokenized words list
def printWordTokens(words):
    for s in words:
        print(s + ', ',end='');


# Main function
if __name__ == "__main__":
    # Read the tweet's file

    # For each document that contains tweets in the database, do the following
    # ------------------------------------------------------------------------
    file_timeordered_json_tweets = codecs.open(r'D:\Dev\TwitterProductRanking\Project\TwitterExamples\tweets.txt', 'r', 'utf-8')

    # Parse the file line by line
    for line in file_timeordered_json_tweets:
        # Parse the json into a JSON object called tweet
        tweet = json.loads(line)

        # Get what we need from the tweet
        [year, month, wordTokens, semanticResult] = parse_json_tweet(line)

        # With wordsTokens do:
        # 1. Add to the month TOTAL sum
        # 2. Consider the semanticResult, Add the word to the Positive / Negative sum
        print('------------------------------')
        print('year: ' + str(year) + ' month: ' + str(month) + ' wordTokens: ')

        printWordTokens(wordTokens)


#
# tw = "@SNFonNBC: A few more hours Tom, just a few more! #2015\n\n(via Tom Brady) http://t.co/wEOTiUGkW7”\n\nWhat. The. Fuck? RT"
#

