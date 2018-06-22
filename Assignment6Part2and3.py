# -*- coding: utf-8 -*-
"""
Created on Mon May 16 21:06:51 2016

@author: Yiyang
"""

import json
import sqlite3

TweetTable = '''CREATE TABLE Tweet
(
    created_at              VARCHAR2(50),
    id_str                  VARCHAR2(50),
    text                    VARCHAR2(50),
    source                  VARCHAR2(50),
    in_reply_to_user_id     NUMBER(20),
    in_reply_to_screen_name VARCHAR2(30),
    in_reply_to_status_id   NUMBER(20),
    retweet_count           NUMBER(4),
    contributors            VARCHAR2(10)
)'''

conn = sqlite3.connect('Assignment6.db')
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS Tweet")
c.execute(TweetTable)

fd = open('Assignment6.txt', 'r', encoding = 'utf8')
allLines = fd.readline().split('EndOfTweet')
type(allLines[0])
fd.close()


for OneTweetString in allLines:
    
    jsonobject = json.loads(OneTweetString)
    if 'retweeted_status' in jsonobject.keys():
        retweetcount = jsonobject['retweeted_status']['retweet_count']
    else:
        retweetcount = jsonobject['retweet_count']
        
    c.execute('''INSERT INTO Tweet VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
              (jsonobject['created_at'], jsonobject['id_str'], 
               jsonobject['text'], jsonobject['source'], 
               jsonobject['in_reply_to_user_id'], jsonobject['in_reply_to_screen_name'],
               jsonobject['in_reply_to_status_id'], retweetcount, 
               jsonobject['contributors']))

#Assignment6 Part3 f Start    
        j = [jsonobject['retweeted_status']['retweet_count']]
        x = [i for i in j if i >= 5]
        len(x)
#End
except:

conn.commit()
conn.close()

#==============================================================================
#Assignment6 Part3 a
#SELECT COUNT(source) FROM Tweet WHERE source LIKE '%iPhone%';
#
#Assignment6 Part3 b
#CREATE VIEW NotReply AS SELECT * FROM Tweet WHERE  in_reply_to_user_id IS NULL;
#
#Assignment6 Part3 c
#SELECT text FROM NotReply WHERE retweet_count > (SELECT AVG(retweet_count) FROM NotReply);
#
#Assignment6 Part3 d
#CREATE VIEW Retweet5 AS SELECT id_str, text, source FROM Tweet WHERE retweet_count >= 5;
#
#Assignment6 Part3 e
#SELECT COUNT(id_str) FROM Retweet5;
#
#Assignment6 Part3 f
#
#==============================================================================
