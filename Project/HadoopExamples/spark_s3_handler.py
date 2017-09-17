'''
Created on 03/04/2017

@author: orensig
'''

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'
os.environ['HADOOP_HOME'] = 'C:\\Users\\orensig\\Documents\\hadoop-2.7.3-src'
import json
import sys
import unicodedata
import re
"""from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType, unicode"""
from pyspark import SparkContext
from pyspark import SparkConf
from collections import *
from random import randrange
import datetime
import csv
import string, glob
from sklearn import linear_model
from sklearn.feature_extraction.text import HashingVectorizer, CountVectorizer
from sklearn.externals import joblib
from os import path
import twokenize, emoticons
import numpy as np
from scipy import sparse
from scipy.sparse import hstack
import traceback
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key

INPUT_FOLDER = ["s3n://magnet-fwm/twitter/2015_01_01_00_00_activities.json.gz", "s3n://magnet-fwm/twitter/2015_01_01_00_10_activities.json.gz"]
S3_ACCESS_KEY_ID = 'AKIAJUZSJMHIS6ZSO2QQ'
S3_SECRET_ACCESS_KEY='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'
#INPUT_FOLDER = "s3n://magnet-fwm/twitter/*.json.gz"
#INPUT_FOLDER = ["s3n://magnet-fwm/home/TsviKuflik/playground/Oren/All_tweets_Oren_demog_info_040617_11_44/part-00000","s3n://magnet-fwm/home/TsviKuflik/playground/Oren/All_tweets_Oren_demog_info_040617_11_44/part-00001"]
#OUTPUT_FOLDER = "s3n://magnet-fwm/home/TsviKuflik/playground/Oren/All_tweets_Oren"
OUTPUT_FOLDER = "C:\\Users\\orensig\\Documents\\users_city_results"
BUCKET_NAME = 'magnet-fwm'
BUCKET_SUFFIX = 'home/TsviKuflik/playground/Oren/All_tweets_Oren_demog_info_070617_08_16.csv/'
BUCKET_SUFFIX2 = 'home/TsviKuflik/playground/Oren/'
GEN_LEX_LOC = "s3n://magnet-fwm/home/TsviKuflik/playground/Oren/demog_attributes_def_Oren/emnlp14gender.csv"
AGE_LEX_LOC = "s3n://magnet-fwm/home/TsviKuflik/playground/Oren/demog_attributes_def_Oren/emnlp14age.csv"
BODY_KEY = 'body'
TWEETS_KEY = 'tweets'
POSTED_TIME_KEY = 'postedTime'
TWITTER_TIME_ZONE_KEY = 'twitterTimeZone'
SENTIMENT_KEY='inferredSentiment'
LOCATIONS_KEY = 'locations'
MAIN_LOCATION_KEY = 'main_location'
INFERRED_DEMOG = 'inferred_demographics'
INFERRED_KEY = 'inferred'
UNKNOWN_VAL = 'Unknown'
INFERRED_DEMOG_KEYS = ['inferredGender', 'inferredAge'] #, 'inferredPolitical', 'inferredRelationship', 'inferredChildren', 'inferredRace', 'inferredIncome', 'inferredEducation', 'inferredSentiment']
AGE_UNDER_18_KEY = "Under_18"
AGE_18_24_KEY = "18-24"
AGE_25_34_KEY = "25-34"
AGE_35_44_KEY = "35-44"
AGE_45_54_KEY = "45-54"
AGE_55_64_KEY = "55-64"
AGE_65_74_KEY = "65-74"
AGE_OVER_75_KEY = "Over_75"
TWEETS_PER_USER = 200

class S3_handler(object):

    def __init__(self):
        #self.ageDict = self.readFileWHeader(AGE_LEX_LOC)
        #self.genderDict = self.readFileWHeader(GEN_LEX_LOC)
        #self.pred = Predictor()
        #self.model_dir = path.join(path.dirname(__file__), 'Trained_Models')
        #self.emo_vocab_file = path.join(os.getcwd(), 'emotion_vocab')
        #self.sent_vocab_file = path.join(os.getcwd(), 'sentiment_vocab')
        self.inferredAttr = [(attr.replace(INFERRED_KEY, "")).lower() for attr in INFERRED_DEMOG_KEYS]

    def rand_input_folders(self,_amtFiles):
        prefix = "s3n://magnet-fwm/twitter/"
        suffix = "_activities.json.gz"
        rand_timestamps = []
        current = datetime.datetime(2015, 1, 1, 00, 00)
        for i in range(0,_amtFiles):
            curr = current + datetime.timedelta(minutes=randrange(10000)*10)
            rand_timestamps.append(str(prefix + "20" + curr.strftime("%y_%m_%d_%H_%M") + suffix))
        #print(rand_timestamps)
        return  rand_timestamps

    def spark_initiator(self):
        conf = SparkConf().setAppName("magnet")
        sc = SparkContext.getOrCreate(conf)
        sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAJUZSJMHIS6ZSO2QQ")
        sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")
        return sc

    def boto_initiator(self):
        conn = S3Connection(host='s3-eu-west-1.amazonaws.com', aws_access_key_id=S3_ACCESS_KEY_ID, aws_secret_access_key=S3_SECRET_ACCESS_KEY, calling_format=OrdinaryCallingFormat())
        return conn

    def boto_list_of_files(self, _conn):
        bucket = _conn.get_bucket(BUCKET_NAME, validate=False)
        #contents = bucket.list(prefix='twitter/')
        contents = bucket.list(prefix=BUCKET_SUFFIX)
        return contents

    def get_nonempty_twitter_files(self,_fileList):
        path = []
        for f in _fileList:
            try:
                if (f.size != 0):
                    file = f.name
                    path.append(file)
            except:
                continue
        return path

    def create_urls_list(self,_non_empty_file_names):
        full_urls_list = []
        for url in _non_empty_file_names:
            full_uri = "s3n://{}/{}".format(BUCKET_NAME, url)
            full_urls_list.append(full_uri)
        return full_urls_list


    def readFileWHeader(self, _fileName):
        dict = {}
        sc = self.spark_initiator()
        file = sc.textFile(_fileName).map(lambda row:row.split(','))
        for row in file.take(file.count()):
            try:
                if str(row[0]).replace('\"','') != "term":
                    dict[str(row[0]).replace('\"','')] = float(str(row[1]).replace('\"',''))
            except:
                pass
        return dict

    def writeFileWHeader(self, _fileName, _data2write):
        with open(_fileName, 'a') as WriteFile:
            writer = csv.writer(WriteFile, delimiter=',', lineterminator='\n')
            writer.writerow([_data2write.user.userID, _data2write.user.screenName.encode("utf-8"), _data2write.user.age,
                             _data2write.user.gender, _data2write.tweetID, _data2write.text.encode("utf-8"),
                             _data2write.createdDate.encode("utf-8")])
        WriteFile.close()

    def read(self,line):
        try:
            #print(line)
            return json.loads(line)
        except:
            traceback.print_exc()
            pass

    def ignore(self,s):
        text = unicodedata.normalize('NFKD',str(s)).encode('ascii', 'ignore')
        text = text.replace("\n", " ")
        text = text.replace("\t", " ")
        text = text.lower()
        print(text)
        return text

    def infer_demog_for_user(self, _row, _ageDict, _genDict):  #, _predictor, _model_dir, _emo_vocab_file, _sent_vocab_file):
        try:
            #print(_row)
            body = _row[1][BODY_KEY]
            total_freq =  0
            tweetText = str(body).split(" ")
            wordsDict = dict()
            for wordIter in tweetText:
                total_freq += 1
                if (not wordIter in wordsDict):
                    wordsDict[wordIter] = []
                    wordsDict[wordIter].append(1)
                    if (wordIter in _ageDict):
                        wordsDict[wordIter].append(_ageDict[wordIter])
                    else:
                        wordsDict[wordIter].append(0)
                    if (wordIter in _genDict):
                        wordsDict[wordIter].append(_genDict[wordIter])
                    else:
                        wordsDict[wordIter].append(0)
                else:
                    wordsDict[wordIter][0] += 1
            """sum_gender = 0
            sum_age = 0
            for key in wordsDict:
                sum_gender += float(wordsDict[key][0])*float(wordsDict[key][1])
                sum_age += float(wordsDict[key][0])*float(wordsDict[key][2])"""
            #print(wordsDict)
            sum_gender = sum(float(wordsDict[key][0])*float(wordsDict[key][1]) for key in wordsDict.keys())
            sum_age = sum(float(wordsDict[key][0])*float(wordsDict[key][2]) for key in wordsDict.keys())
            # print(sum_gender)
            #print(sum_age)
            age = float(_ageDict["_intercept"]) + float(sum_age/total_freq)
            sum_gender = float(_genDict["_intercept"]) + float(sum_gender/total_freq)
            gender = 'x'
            if (sum_gender >= 0):
                gender = 'female'
            else:
                gender = 'male'
            ans = _row
            #print(ans[1][LOCATIONS_KEY])
            """vals = list(ans[1][LOCATIONS_KEY].values())
            print(vals)
            keys = list(ans[1][LOCATIONS_KEY].keys())
            print(keys)"""
            #main_loc_key = keys[vals.index(max(vals))]
            main_loc_key = max(ans[1][LOCATIONS_KEY], key=(lambda key: ans[1][LOCATIONS_KEY][key]))
            #print(main_loc_key)
            ans[1][LOCATIONS_KEY][MAIN_LOCATION_KEY] = dict()
            ans[1][LOCATIONS_KEY][MAIN_LOCATION_KEY][main_loc_key] = ans[1][LOCATIONS_KEY][main_loc_key]
            #print(ans[1])
            ans[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[0]] = gender
            ans[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] = age
            """for tweet in ans[1][TWEETS_KEY]:
                predSenti = _predictor.predict_attribute(_model_dir, "sentiment", tweet[BODY_KEY].split(" "), _sent_vocab_file, _emo_vocab_file)
                tweet[SENTIMENT_KEY] = predSenti['pred_class']
            tweetsGen = (tweet[BODY_KEY] for tweet in ans[1][TWEETS_KEY])
            tweetsArr = list(tweetsGen)
            #print(tweetsArr)
            for attr in _predictor.attributes:
                if attr in self.inferredAttr and str(attr) != "age" and str(attr) != "gender" and str(attr) != "sentiment":
                    idx = INFERRED_DEMOG_KEYS.index(INFERRED_KEY + attr.capitalize())
                    #print(idx)
                    pred = _predictor.predict_attribute(_model_dir, attr, tweetsArr, _sent_vocab_file, _emo_vocab_file)
                    #print(pred)
                    ans[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[idx]] = pred['pred_class']"""
            #print(ans)
            return ans
        except:
            traceback.print_exc()
            pass


    def users_locations(self,_row):
        key_value_list = []
        try:
            if 'actor' in _row:
                name = str(_row['actor']['preferredUsername'])
                key_vals = []
                key_vals.append(name)
                #print(name)
                if 'location' in _row:
                    location_displayName = str(_row['location']['displayName'])
                    key_vals.append(location_displayName)
                else:
                    key_vals.append("No_Location")
                if BODY_KEY in _row:
                    tweet = dict()
                    tweet[BODY_KEY] = str(_row[BODY_KEY])
                    #tweet[POSTED_TIME_KEY] = str(_row[POSTED_TIME_KEY])
                    #tweet[TWITTER_TIME_ZONE_KEY] = str(_row['actor'][TWITTER_TIME_ZONE_KEY])
                    key_vals.append(tweet)
                else:
                    key_vals.append("No_Body")
                key_value_list.append(key_vals)
                #print(name + ' ' + location_displayName)
        except:
            pass
        #print(key_value_list)
        return key_value_list

    def createDict(self, _key, _val, _body):
        res = dict()
        locations = dict()
        locations[_key]=_val
        res[LOCATIONS_KEY]=locations
        res[BODY_KEY] = _body[BODY_KEY]
        #res[TWEETS_KEY] = []
        #res[TWEETS_KEY].append(_body)
        res[INFERRED_DEMOG] = {key: UNKNOWN_VAL for key in INFERRED_DEMOG_KEYS}
        #print(res[INFERRED_DEMOG])
        return res

    def redTuple(self,_x,_y):
        results = dict()
        #print(_x)
        try:
            for key in _x.keys():
                if key == LOCATIONS_KEY:
                    results[LOCATIONS_KEY] = dict()
                    for key_loc in _x[key].keys():
                        results[LOCATIONS_KEY][key_loc] =  _x[key][key_loc]
                    if key_loc in _y[key].keys():
                        #print(1)
                        results[LOCATIONS_KEY][key_loc] =  results[LOCATIONS_KEY][key_loc] + _y[key][key_loc]
            for key in _y.keys():
                if key == LOCATIONS_KEY:
                    for key_loc in _y[key].keys():
                        if key_loc not in results[key].keys():
                            results[key][key_loc] = _y[key][key_loc]
            results[BODY_KEY] = _x[BODY_KEY] + _y[BODY_KEY]
            #print(results[BODY_KEY])
            #results[TWEETS_KEY] = _x[TWEETS_KEY] + _y[TWEETS_KEY]
            #print(results[TWEETS_KEY])
            results[INFERRED_DEMOG] = {key: _y[INFERRED_DEMOG][key] for key in INFERRED_DEMOG_KEYS}
            return results
        except:
            pass

    def mapCities(self,_row):
        ans = dict()
        #ans[LOCATIONS_KEY] = _row[1][LOCATIONS_KEY]
        try:
            for key_location in _row[1][LOCATIONS_KEY].keys():
                if key_location != MAIN_LOCATION_KEY:
                    ans[key_location] = []
                    ans[key_location].append(_row[1][LOCATIONS_KEY][key_location])
                    ans[key_location].append(1)
                    ans[key_location].append(int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    #print(key_location,next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY])))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[0]] == 'male')*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[0]] == 'female')*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] < 18)*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] >= 18)*int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] < 25)*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] >= 25)*int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] < 35)*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] >= 35)*int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] < 45)*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] >= 45)*int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] < 55)*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] >= 55)*int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] < 65)*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] >= 65)*int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] < 75)*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))
                    ans[key_location].append(int(_row[1][INFERRED_DEMOG][INFERRED_DEMOG_KEYS[1]] >= 75)*int(key_location == next(iter(_row[1][LOCATIONS_KEY][MAIN_LOCATION_KEY]))))

            #print(ans)
            return ans
        except:
            pass

    def demogReducer(self,_x,_y):
        try:
            ans = [_x[i] + _y[i] for i in range(len(_x))]
            return ans
        except:
            traceback.print_exc()
            pass




    def load_json_files(self):
        sc = self.spark_initiator()
        boto_conn = self.boto_initiator()
        #inp_folders = self.rand_input_folders(1) # ','.join(inp_folders)
        full_file_list = self.boto_list_of_files(boto_conn)
        nonempty_file_list = self.get_nonempty_twitter_files(full_file_list)
        urls_list = self.create_urls_list(nonempty_file_list)
        #files = sc.textFile(','.join(urls_list[0:2])).filter(lambda line: (len(line) > 1 and line is not None)).map(lambda line: self.read(line))
        #user_city_count = files.flatMap(lambda row: self.users_locations(row)).map(lambda users_location: (users_location[0],self.createDict(users_location[1],1,users_location[2]))).reduceByKey(lambda x, y: self.redTuple(x,y)).map(lambda user_dict: self.infer_demog_for_user(user_dict, self.ageDict, self.genderDict))
        #user_city_count = files.flatMap(lambda row: self.users_locations(row)).map(lambda users_location: (users_location[0],self.createDict(users_location[1],1,users_location[2]))).reduceByKey(lambda x, y: self.redTuple(x,y)).map(lambda user_dict: self.infer_demog_for_user(user_dict, self.ageDict, self.genderDict, self.pred, self.model_dir, self.emo_vocab_file, self.sent_vocab_file))
        #files = sc.textFile(','.join(INPUT_FOLDER))
        #.flatMap(lambda user: self.print_return(user))
        #cities_info = user_city_count.map(lambda userRow: self.mapCities(userRow)).map(lambda city: (next(iter(city)), city[next(iter(city))])).reduceByKey(lambda x,y: self.demogReducer(x,y))
        #numPartitions = len(cities_info)
        #cities_info_rdd = sc.parallelize(cities_info).filter(lambda demogDict: len(demogDict) > 0).map(lambda city: self.toCsvLine(city)).repartition(numPartitions)
        #cities_info.saveAsTextFile(OUTPUT_FOLDER + '_demog_info_' + datetime.datetime.now().strftime("%d%m%y_%H_%M") + '.csv')
        #cities_info_rdd.saveAsTextFile(OUTPUT_FOLDER + '_demog_info_' + datetime.datetime.now().strftime("%d%m%y_%H_%M"))
        #for user in user_city_count.take(user_city_count.count()):
        #    print(str(user))
        #user_city_count.saveAsTextFile(OUTPUT_FOLDER + '_demog_info_' + datetime.datetime.now().strftime("%d%m%y_%H_%M"))
        citiesFiles = sc.textFile(','.join(urls_list)).filter(lambda line: (len(line) > 1 and line is not None))  #.saveAsTextFile(OUTPUT_FOLDER + '_demog_info_' + datetime.datetime.now().strftime("%d%m%y_%H_%M") + '.csv')
        fileName = 'demog_info_' + datetime.datetime.now().strftime("%d%m%y_%H_%M") + '.csv'
        with open(fileName, 'w') as WriteFile:
            writer = csv.writer(WriteFile, delimiter=',', lineterminator='\n')
            writer.writerow(["City", "Amount of Tweets", "Amount of different users", "Main location", "Males",	"Females", "Under_18", "18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "Over_75" ])
        #fileLst = []
        #fileLst.append(["City", "Amount of Tweets", "Amount of different users", "Main location", "Males",	"Females", "Under_18", "18-24", "25-34", "35-44", "45-54", "55-64", "65-74", "Over_75" ])
            for city in citiesFiles.take(citiesFiles.count()):
                cityLst = ""
                if city[1] == "\"":
                    cityLst = city.split('\",')
                else:
                    cityLst = city.split('\',')
                cityLst[0] = cityLst[0].replace("(","").replace('\'','').replace('\"','')
                cityLstInfo = cityLst[1].split(',')
                cityLstInfo = [(num.replace("[","")).replace("]","").replace("(","").replace(")","") for num in cityLstInfo]
                demogInfoLst = []
                demogInfoLst.append(cityLst[0])
                for itemX in cityLstInfo:
                    demogInfoLst.append(itemX)
                writer.writerow(demogInfoLst)
            WriteFile.close()
            #fileLst.append(demogInfoLst)
        bucket = boto_conn.get_bucket(BUCKET_NAME , validate=False)
        k = Key(bucket)
        k.key = BUCKET_SUFFIX2 + "/" + fileName
        k.set_contents_from_filename(fileName)


class Predictor:
    """
    Take: a list of tweets
    Return: 1. predicted class; 2. predicted probabilities
    """

    def __init__(self):
        self.clf = linear_model.LogisticRegression(C=1.0, dual=False, penalty='l2', tol=1e-6)
        self.hv = HashingVectorizer(ngram_range=(1, 2), binary=True)

        self.pred_classes = []
        self.pred_probs = []

        self.attributes = ['age', 'political', 'relationship', 'iq', 'gender',
                           'children', 'religion', 'race', 'income', 'education',
                           'optimism', 'narcissist', 'anxious_moody_stressed',
                           'life_satisfaction', 'excited_outgoing_enthusiastic', 'sentiment', 'emotion']
        self.attr_values = {'age': 'Above_25, Below_25', 'political': 'Democratic, Republican',
                            'relationship': 'In a Relationship, Single', 'iq': 'Above_Average, Average_and_Below',
                            'gender': 'Female, Male', 'children': 'No, Yes', 'religion': 'Christian, Unaffiliated',
                            'race': 'Black, White', 'income': 'Over_35K, Under_35K', 'education': 'Degree, High School',
                            'optimism': 'Optimist, Pessimist', 'narcissist': 'Agree, Dissagree',
                            'anxious_moody_stressed': 'Agree, Dissagree',
                            'life_satisfaction': 'Dissatisfied, Satisfied',
                            'excited_outgoing_enthusiastic': 'Agree, Dissagree',
                            'sentiment': 'negative, neutral, positive',
                            'emotion': 'anger, disgust, fear, joy, sadness, suprise'}

    '''
    Take: dir with pre-trained models, attribute e.g., gender, a list of tweets,
          sentiment/emotion vocab files (a list of unigrams used to train sentiment/emotion models; 
          one per line)
    Return: predicted class and predicted probabilities for each attribute
    '''

    def predict_attribute(self, dir, attr, tweets, sent_vocab_file, emo_vocab_file):
        pred_dict = {}
        # loading pre-trained model
        current = os.getcwd()
        if dir not in os.getcwd():
            os.chdir(dir)
        for file in glob.glob("*.pkl"):
            if attr in file:
                self.clf = joblib.load(file)

        if 'emotion' not in attr and 'sentiment' not in attr:
            tweets = self.aggregate_tweets(tweets)
            features = self.hv.transform(tweets)

        else:
            if 'sentiment' in attr:
                features = self.extract_more_sentiment_features(tweets, sent_vocab_file)
            else:
                features = self.extract_more_sentiment_features(tweets, emo_vocab_file)

        pred_probs = self.clf.predict_proba(features).tolist()
        # print pred_probs

        #pred_classes = self.clf.predict(features).tolist()
        classesList = self.attr_values[attr].split(', ')
        pred_classes = classesList[pred_probs[0].index(max(pred_probs[0]))]
        # print pred_classes

        pred_dict['pred_probs'] = pred_probs
        pred_dict['pred_class'] = pred_classes
        # print pred_dict

        if 'emotion' in attr or 'sentiment' in attr:
            # print pred_classes
            pie_values = self.aggregare_sent_emotion(pred_classes)
            pred_dict['pie_values'] = pie_values

        data_string = json.dumps(pred_dict)
        # print data_string
        return pred_dict

    def aggregate_tweets(self, tweet_list):
        """
        Get a blob of tweets from a list
        """
        tweets = []
        str_ = ''
        for tweet in tweet_list:
            str_ += tweet + ' '

        tweets.append(str_)
        return tweets

    def aggregare_sent_emotion(self, pred_classes):
        # pred_classes = json.loads(json_pred)['pred_classes']

        valueToCount = {}
        for s in pred_classes:
            if s in valueToCount.keys():
                v = valueToCount[s]
                v += 1
            else:
                v = 1
            valueToCount[s] = v
        return valueToCount

    '''
    Additional features for sentiment/emotion classification:
        all-caps: YAY, COOL ..
        elongated words: waaay, sooo....
        emoticons: positive, negative
        hashtags: the number of hastags
        punctuation: !!!!, ???? ...
        POS tags and negation are in separate methods
    Return: a list of features vectors (one feature vector per tweet).
    '''

    def extract_more_sentiment_features(self, tweets, vocab_file):
        # print 'Extracting sentiment/emotion features with training vocab'
        train_vocab = {}
        k = 0
        for line in open(vocab_file):
            train_vocab[line.strip()] = k
            k += 1
        # print 'Train vocab size=>' + str(len(train_vocab))

        cv = CountVectorizer(ngram_range=(1, 1), binary=True, vocabulary=train_vocab)
        train_features_bow = cv.fit_transform(tweets)

        add_sentiment_matrix = []
        hash_pattern = re.compile('\#+[\w_]+[\w\'_\-]*[\w_]+')
        elong_pattern = re.compile("([a-zA-Z])\\1{2,}")
        caps_pattern = re.compile(('[A-Z][A-Z\d]+'))
        punc_pattern = re.compile('([.,!?]+)')

        for tweet in tweets:
            tweet_vector = []
            tokens = twokenize.tokenize(tweet)
            # count the number of elongated tokens
            n_elong = len(re.findall(elong_pattern, tweet))

            # count the number of all_caps tokens
            n_caps = len(re.findall(caps_pattern, tweet))

            # count the number of repeated punctuation
            n_rep_punct = len(re.findall(punc_pattern, tweet))

            # count the number of hasgtags
            n_hahtag = len(re.findall(hash_pattern, tweet))

            # check if the tweets has SAD, HAPPY, BOTH_SH or NA emoticon
            emoticon_mood = emoticons.analyze_tweet(tweet.strip())
            if emoticon_mood == 'NA':
                emoticon_mood = 0
            elif emoticon_mood == 'HAPPY':
                emoticon_mood = 2
            elif emoticon_mood == 'SAD':
                emoticon_mood = 1
            elif emoticon_mood == 'BOTH_HS':
                emoticon_mood = 4
            tweet_vector = [n_elong, n_caps, n_rep_punct, n_hahtag, emoticon_mood]
            add_sentiment_matrix.append(tweet_vector)

        # print np.asarray(add_sentiment_matrix).shape
        a = np.asarray(add_sentiment_matrix)
        # print 'additional 5 features: ' + str(a)

        sa = sparse.csr_matrix(add_sentiment_matrix)
        features = hstack([sa, train_features_bow])
        # print 'final feature matrix size: ' + str(features.shape)

        return features

    '''
        Take: a single tweet
        Return: appends a _NEG suffix to every word appearing between the negation and the clause-level punctuation mark
    '''

    def take_into_account_negation(self, tweet):
        neg_pattern = re.compile(
            'never|nothing|nowhere|noone|none|not|havent|hasnt|hadnt|cant|couldnt|shouldnt|wont|wouldnt|dont|doesnt|didnt|isnt|arent|aint|no|' +
            'n\'t|haven\'t|haven\'t|hasn\'t|hadn\'t|can\'t|couldn\'t|shouldn\'t|won\'t|wouldn\'t|don\'t|doesn\'t|didn\'t|isn\'t|aren\'t',
            re.IGNORECASE)
        clause_pattern = re.compile(r'^[.:;!?]$')

        neg = re.search(neg_pattern, tweet)
        if neg != None:
            # print 'Negation in tweet: ' + tweet
            pattern = tweet[neg.start():]
            end = re.search(clause_pattern, pattern)
            if end == None:
                end_str = len(tweet)
            else:
                end_str = end.start()
                end_str = int(end_str) - 1
            negated = ''

            tokens = twokenize.tokenize(pattern[:end_str])
            for w in tokens:
                negated += w + '_neg '
            negated = tweet[:neg.start()] + negated
            # print 'Negation in tweet: ' + negated
        else:
            negated = tweet
        return negated

    def read_data(self, filename):
        hash = {}
        line_num = 0
        for line in open(filename):
            if line_num >= 0:
                hash[line.strip()] = line.split('\t')[1].strip()
            line_num += 1
        # print len(hash)
        return hash

if __name__ == '__main__':
    s3 = S3_handler()
    s3.load_json_files()



