import tweepy
import csv
import random
import sys
from collections import defaultdict


def getApi():

    apiKey = 'xOgrLCQawS9Znz4piUExxOUMN'
    apiSecretKey = 'yjzUp3sVtz0lVOExeeqVZXxfByhOPcx7ablUpfYq5PFlesPGW8'
    accessToken = '1255341900376219648-6kvhWj5u2rmEHXoGbneL0JfoiHvIX1'
    accessTokenSecret = 'xN3EWt27KlIKaSLCdltYXazd0Csb7vIAaHEE00LFAXPUI'

    authenticate = tweepy.OAuthHandler(apiKey, apiSecretKey)
    authenticate.set_access_token(accessToken, accessTokenSecret)
    api = tweepy.API(authenticate)

    return api


def updateTags():

    global HASH
    global SAMPLE
    tags = defaultdict(int)

    for tweet in SAMPLE:
        for t in tweet:
            tags[t['text']] += 1

    return tags


def getTop3(tags):

    topVals = sorted(set(tags.values()), reverse = True)[:3]
    top3 = []
    for k, v in tags.items():
        if v in topVals:
            top3.append((k, v))
    top3 = sorted(top3, key = lambda x: (-x[1], x[0]))
    
    return top3


def writeOutput(top3):

    global TOTAL

    with open(outputFile, 'a', newline='') as f:

        wr = csv.writer(f, delimiter = ' ')
        header = ['The number of tweets with tags from the beginning:', TOTAL]
        wr.writerow(header)
        for t in top3:
            line = [t[0], ":", t[1]]
            wr.writerow(line)
        wr.writerow([])


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):

        global TOTAL
        global SIZE
        global SAMPLE

        if status.lang != 'en':
            pass
        else:

            tweet = status.entities['hashtags']
            if len(tweet):
                TOTAL += 1

                #first 100 tweets
                if TOTAL <= SIZE:

                    SAMPLE.append(tweet)

                else:

                    if random.random() <= float(SIZE / TOTAL):
                        SAMPLE.pop(random.randint(0, SIZE - 1))
                        SAMPLE.append(tweet)
                    else:
                        pass

                tags = updateTags()
                top3 = getTop3(tags)
                writeOutput(top3)

            else:
                pass

    def on_error(self, status_code):
        if status_code == 420:

            return False


SAMPLE = []
TOTAL = 0
SIZE = 100
K = 3

if __name__ == "__main__":

    port = 9999#int(sys.argv[1])
    outputFile = 'task3ans.csv'#sys.argv[2]
    api = getApi()
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener = myStreamListener)
    myStream.filter(track = ['#'])

