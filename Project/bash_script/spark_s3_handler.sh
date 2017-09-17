#!/bin/bash



#python ../TwitterExamples/TweetParser.py --input_folder /media/windows_share/TwitterProductRanker/Project/data/input_twitts


#PYTHON_SCRIPT=../TwitterExamples/wordcount.py
#JSON_FILE=../data/input_twitts/2015_01_01_00_00_activities.json

#/media/windows_share/spark-2.2.0-bin-hadoop2.7/bin/spark-submit $PYTHON_SCRIPT$ $JSON_FILE$






../../../spark-2.2.0-bin-hadoop2.7/bin/spark-submit /media/windows_share/TwitterProductRanker/Project/TwitterExamples/wordcount.py --input_folder /media/windows_share/TwitterProductRanker/Project/data/input_twitts