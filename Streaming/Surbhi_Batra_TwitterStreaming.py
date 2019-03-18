import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import random

hashtagdict = {}
twitterIDandhashtags ={}
twitterIDandlength ={}

c = 0
lengthofthistweet = 0
sofarlength = 0

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

class StdOutListener(StreamListener):

    def on_status(self, status):
        global c
        global lengthofthistweet
        global sofarlength

        c+=1

        try:
            text = status.extended_tweet["full_text"]
            text = text.encode('utf-8')
        except AttributeError:
            text = status.text
            text = text.encode('utf-8')
        lengthofthistweet = len(text)

        if(c<=100):

            tw_hashtaglist = status.entities.get('hashtags')
            hashtaglistforthistweet = []
            twitterIDandlength[c] = lengthofthistweet
            sofarlength += lengthofthistweet

            for htd in tw_hashtaglist:
                ht = htd.get('text').encode('utf-8')
                hashtagdict[ht] = hashtagdict.get(ht,0)+1
                hashtaglistforthistweet.append(ht)

            twitterIDandhashtags[c] = hashtaglistforthistweet

        else :
            r = random.randint(1,c)
            if(r<=100):
                tw_hashtaglist = status.entities.get('hashtags')
                hashtaglistforthistweet = []
                for htd in tw_hashtaglist:
                    ht = htd.get('text').encode('utf-8')
                    hashtagdict[ht] = hashtagdict.get(ht,0)+1
                    hashtaglistforthistweet.append(ht)

                purane_hashtags = twitterIDandhashtags.get(r,[])
                purani_length = twitterIDandlength[r]
                twitterIDandhashtags[r] = hashtaglistforthistweet

                for puranatag in purane_hashtags:
                    hashtagdict[puranatag] -= 1

                sortedhashtag = sorted(hashtagdict, key = hashtagdict.get, reverse = 1)

                print("\nThe number of the twitter from the beginning: ",c, "\nTop 5 hot hashtags: ")
                for i in range(5):
                    print(sortedhashtag[i]," : ", hashtagdict[sortedhashtag[i]])
                # print "(sofarlength + lengthofthistweet - purani_length): ", sofarlength, lengthofthistweet, purani_length
                print("The average length of the twitter is: ",(sofarlength + lengthofthistweet - purani_length)/float(100))


        return True

    def on_error(self, status_code):
        if status_code == 420:
            return False

l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

stream = Stream(auth, l, tweet_mode='extended')
stream.filter(track=['games'])
