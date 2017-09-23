inFiles = [r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\res_2015_01_01.txt',
           r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\res_2015_01_02.txt',
           r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\res_2015_02_01.txt',
           r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\res_2015_02_02.txt',
           r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\res_2015_03_01.txt',
           r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\res_2015_03_02.txt'
           ]
outFiles = [r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\2015_01_01_map.csv',
            r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\2015_01_02_map.csv',
            r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\2015_02_01_map.csv',
            r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\2015_02_02_map.csv',
            r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\2015_03_01_map.csv',
            r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\2015_03_02_map.csv'
           ]
# r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\res_2015_02_01_fixed.txt',
from nltk.probability import FreqDist
from nltk.corpus import stopwords
from string import punctuation
from nltk.tokenize import word_tokenize
from heapq import nlargest,nsmallest
import csv

def CalculateFreqDist(input_file, output_file):
    print('FreqDist for file: ' + input_file)
    fIn = open(input_file, 'r')
    for line in fIn:
        words = line.split(" ")

        # Get all known stop words in corpus in english from the punctuation
        englishStopWords = set(stopwords.words('english') + list(punctuation))

        # Create our words set without the stop words
        words_set = [word for word in words if word not in englishStopWords]

        freq = FreqDist(words_set)

        maxFreqValTmp = nlargest(10, freq, key=freq.get)
        minFreqValTmp = nsmallest(10, freq, key=freq.get)

        print('10 largest words: ')
        for itm in maxFreqValTmp:
            print(' ' + itm + ' freqdist: ' + str(freq[itm]));
        print('10 lowest words: ')
        for itm in minFreqValTmp:
            print(' ' + itm + ' freqdist: ' + str(freq[itm]));

        # Write csv map of results
        file = csv.writer(open(output_file, 'w'))
        for key, count in freq.most_common(len(freq)):
            file.writerow([key, count])


            # Example of creating a set
            # words_set = set(words)



def CreateTmpFile():
    print('Openinig file: ' + inFiles[0])
    fIn = open(inFiles[0], 'r')
    words = []
    for line in fIn:
        my_list = line.split(" ")
        for word in my_list:
            words.append(word)

    #
    index = 0
    fOut1 = open(r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\tmp.txt', 'w')
    for word in words:
        index += 1
        if (index < 1500):
            fOut1.write(' ' + word)


def ConvertThreeToTwo():
    words = []
    for file in inFiles:
        print('Openinig file: ' + file)
        fIn = open(file, 'r')
        wordsCnt = 0

        for line in fIn:
            my_list = line.split(" ")
            wordsCnt += len(my_list)
            for word in my_list:
                words.append(word)

        print('file had ' + str(wordsCnt) + ' lines')
    index = 0
    fOut1 = open(r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\res_2015_02_01_fixed.txt', 'w')
    fOut2 = open(r'D:\Dev\TweeterProductRankerGithub\Project\MidResults\res_2015_02_02_fixed.txt', 'w')
    for word in words:
        if index < len(words) / 2:
            fOut1.write(' ' + word)
        else:
            fOut2.write(' ' + word)
        index += 1
    print('Total amount of words: ' + str(len(words)))


# ----------------- Two files out of three code for three files
# ConvertThreeToTwo()
# CreateTmpFile()
index = 0
for file in inFiles:
    CalculateFreqDist(inFiles[index],inFiles[index])
    index+=1