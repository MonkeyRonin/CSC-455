# -*- coding: utf-8 -*-
"""
Created on Mon May 30 17:52:51 2016

@author: Yiyang
"""

from operator import itemgetter
import json
import urllib.request as urllib

response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/Assignment5.txt")
str_response = response.readline().decode("utf8")    
tDict = json.loads(str_response)

words = tDict['text'].split(' ')
dCount = {}
for word in words:
    if word not in dCount:
        dCount[word] = 0
    dCount[word] = dCount[word]+1

countKeys = dCount.keys()
countVals = dCount.values()

countPairs = zip(countVals, countKeys)

sorted_countPairs = sorted(countPairs, key=itemgetter(0), reverse=True)

print (sorted_countPairs)

