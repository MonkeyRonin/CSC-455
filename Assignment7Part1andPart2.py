# -*- coding: utf-8 -*-
"""
Created on Wed May 25 18:04:48 2016

@author: Yiyang
"""

import sqlite3, json
import urllib.request as urllib

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
    contributors            VARCHAR2(10),
    user_id                 VARCHAR2(50),
    CONSTRAINT Tweet_FK FOREIGN KEY(user_id)
    REFERENCES User(id)
)'''

UserTable = '''CREATE TABLE User
(
    id              VARCHAR2(50),
    name            VARCHAR2(50),
    screen_name     VARCHAR2(50),
    description     VARCHAR2(50),
    friends_count   NUMBER(50)
)'''

conn = sqlite3.connect('Assignment7.db')
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS Tweet")
c.execute(TweetTable)
c.execute("DROP TABLE IF EXISTS User")
c.execute(UserTable)

response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/Assignment5.txt")
for i in range(7000): 
    str_response = response.readline().decode("utf8")    
    try:
        tDict = json.loads(str_response)

        if 'retweeted_status' in tDict.keys():
            retweetcount = tDict['retweeted_status']['retweet_count']
        else:
            retweetcount = tDict['retweet_count']
        
        c.execute('''INSERT INTO Tweet VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                  (tDict['created_at'], tDict['id_str'], 
                   tDict['text'], tDict['source'], 
                   tDict['in_reply_to_user_id'], tDict['in_reply_to_screen_name'],
                   tDict['in_reply_to_status_id'], retweetcount, 
                   tDict['contributors'], tDict['user']['id']))

        c.execute('''INSERT INTO User VALUES(?, ?, ?, ?, ?)''',
                  (tDict['user']['id'], tDict['user']['name'], 
                   tDict['user']['screen_name'], tDict['user']['description'], 
                   tDict['user']['friends_count']))
    except ValueError:
        print("Error")

#Assignment Part2a       
query2a = c.execute("SELECT id, name FROM User WHERE friends_count = (SELECT MAX(friends_count) FROM User);").fetchall()

#Assignment Part2b
response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/Assignment5.txt")
for i in range(7000): 
    try:
        str_response = response.readline().decode("utf8") 
        tDict = json.loads(str_response)
        if 'user' in tDict.keys():
            ucount = tDict['user']['friends_count']
            uid = tDict['user']['id']
            uname = tDict['user']['name']
        if max(ucount):
            print(uid, uname)
    except ValueError:
        print("Error")
c.close()
conn.commit()
conn.close()

