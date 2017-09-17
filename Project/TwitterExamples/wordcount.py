import argparse, os
import glob
import json
import codecs
import re
from textblob import TextBlob
from string import punctuation
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import sys
import operator
from pyspark import SparkConf, SparkContext

# from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from operator import add


def removeStopWordsAndPunctuation(txt):
    customStopWords = set(stopwords.words('english') + list(punctuation))
    textWithoutStopWords = [word for word in word_tokenize(txt) if word not in customStopWords]
    newText = ''
    for word in textWithoutStopWords:
        if word != "" or word != " ":
            newText += word + ' '
    return newText


def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w +:\ / \ / \S +)", " ", tweet).split())


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_folder')
    parser.add_argument('--output_folder', default=None)
    parser.add_argument('--year', default=2015)
    parser.add_argument('--month', default=None)
    parser.add_argument('--week', default=None)
    parser.add_argument('--remove_one_istance', default=True)
    args = parser.parse_args()

    if args.remove_one_istance:
        threshold = 1
    else:
        threshold = 0

    # spark = SparkSession \
    #     .builder \
    #     .appName("PythonWordCount") \
    #     .getOrCreate()

    conf = SparkConf().setAppName("Spark Count")
    sc = SparkContext(conf=conf)

    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info("pyspark script logger initialized")

    json_files = glob.glob(args.input_folder + r'/*.json')
    twitts = []
    for item in json_files:
        LOGGER.info('read file:' + item)
        splitted = item[item.find(args.input_folder) + 1 + len(args.input_folder):].split('_')
        if splitted[0] == args.year and splitted[1] == args.month:
            LOGGER.info('File Accepted for year and month:' + item)
            with codecs.open(item, 'rU', 'utf-8') as f:
                for line in f:
                    twitts.append(json.loads(line))

    LOGGER.info('Collecting words')
    word_db = []
    for item in twitts:
        # year_month = item['postedTime'][0:7]
        if 'body' in item.keys():
            body = item['body']
            clean_item = clean_tweet(body)
            cleanedBodyText = TextBlob(clean_item)
            words = removeStopWordsAndPunctuation(cleanedBodyText.string.lower())
            # wordTokens = word_tokenize(removeStopWordsAndPunctuation(cleanedBodyText.string))
            word_db.append(words)

    # LOGGER.info('save to temporary file')
    # tmp_file_name = 'tmp.txt'
    # with open(tmp_file_name, 'w') as tmp_fid:
    #     tmp_fid.write(' '.join(word_db))
    #     tmp_fid.flush()
    #     tmp_fid.close()

    LOGGER.info('Parallelizing')
    # create Spark context with Spark configuration

    counts = sc.parallelize(word_db).flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)
    # lines = spark.read.text(tmp_file_name).rdd.map(lambda r: r[0])
    # counts = lines.flatMap(lambda x: x.split(' ')) \
    #     .map(lambda x: (x, 1)) \
    #     .reduceByKey(add)

    output = counts.collect()
    # for (word, count) in output:
    #    print("%s: %i" % (word, count))

    output_dict = dict(output)
    LOGGER.info('ELI:: sort results')
    sorted_wordcount = sorted(output_dict.items(), key=operator.itemgetter(1), reverse=True)


    LOGGER.info('ELI:: save results to file')
    with open(os.path.join(args.output_folder, 'res.json'), 'w') as fp:
        json.dump(sorted_wordcount, fp)

    # spark.stop()

    '''
    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Spark Count")
    sc = SparkContext(conf=conf)

    # read in text file and split each document into words
    tokenized = sc.textFile(tmp_file_name).flatMap(lambda line: line.split(" "))



    # count the occurrence of each word
    wordCounts = tokenized.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)

    # filter out words with fewer than threshold occurrences
    filtered = wordCounts.filter(lambda pair:pair[1] >= threshold)

    print filtered

    # count characters
    #charCounts = filtered.flatMap(lambda pair:pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(lambda v1,v2:v1 +v2)

    #list = charCounts.collect()
    #print repr(list)[1:-1]
    '''

    # print('ELI:: finished executing spark stuff')

