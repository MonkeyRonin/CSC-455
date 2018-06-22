# -*- coding: utf-8 -*-
"""
Created on Wed Jun  1 17:48:01 2016

@author: Yiyang
"""
#Part1 a, c
"""a.	Create a 3rd table incorporating the Geo table (in addition to tweet and 
user tables that you already have) and extend your schema accordingly.You will 
need to generate an ID for the Geo table primary key (you may use any value 
or combination of values as long as it is unique) for that table and link it 
to the Tweet table (foreign key should be in the Tweet table because there can
 be multiple tweets sent from the same location). In addition to the primary
 key column, the geo table should have “type”, “longitude” and “latitude” columns.

c.Repeat what you did in part-b, but instead of saving tweets to the file, 
populate the 3-table schema that you created in SQLite. Be sure to execute 
commit and verify that the data has been successfully loaded (report row counts
for each of the 3 tables).If you use the posted example code be sure to turn 
off batching for this part. (i.e., batchRows set to 1). How long did this step take?"""
 
import sqlite3, json, time
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
    geo_id                  INTEGER(50),
    CONSTRAINT Tweer_PK PRIMARY KEY(id_str),
    CONSTRAINT Tweet_FK FOREIGN KEY(user_id)
    REFERENCES User(id),
    CONSTRAINT Tweet_FK1 FOREIGN KEY(geo_id)
    REFERENCES Geo(id)
)'''

UserTable = '''CREATE TABLE User
(
    id              VARCHAR2(50),
    name            VARCHAR2(50),
    screen_name     VARCHAR2(50),
    description     VARCHAR2(50),
    friends_count   NUMBER(10),
    CONSTRAINT User_PK PRIMARY KEY(id)
)'''

GeoTable = '''CREATE TABLE Geo
(
    id              INTEGER PRIMARY KEY,
    type            VARCHAR2(50),
    longitude       VARCHAR2(50),
    latitude        VARCHAR2(50)
)
'''

conn = sqlite3.connect('YangTweet.db')
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS Tweet")
c.execute(TweetTable)
c.execute("DROP TABLE IF EXISTS User")
c.execute(UserTable)
c.execute("DROP TABLE IF EXISTS Geo")
c.execute(GeoTable)

response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")

Start1c = time.time()
flag = 1

for i in range(500000): 
    str_response = response.readline().decode("utf8")
    try:
        tDict = json.loads(str_response)
        for value in tDict.values():
            if value == "null":
                value == None
#User Table Data
        uData = []
        uID = tDict['user']['id']
        uName = tDict['user']['name']
        uSname = tDict['user']['screen_name']
        uDescription = tDict['user']['description']
        uFcount = tDict['user']['friends_count']
        uData.append(uID)
        uData.append(uName)
        uData.append(uSname)
        uData.append(uDescription)
        uData.append(uFcount)
                      
#Tweet Table Data
        if 'retweeted_status' in tDict.keys():
            retweetcount = tDict['retweeted_status']['retweet_count']
        else:
            retweetcount = tDict['retweet_count']
        tData = []
        tCreatedat = tDict['created_at']
        tIdstr = tDict['id_str']
        tText = tDict['text']
        tSource = tDict['source']
        tInreplyID = tDict['in_reply_to_user_id']
        tInreplyName = tDict['in_reply_to_screen_name']
        tInreplysID = tDict['in_reply_to_status_id']
        tContributors = tDict['contributors']
        tUid = tDict['user']['id']
        tData.append(tCreatedat)
        tData.append(tIdstr)
        tData.append(tText)
        tData.append(tSource)
        tData.append(tInreplyID)
        tData.append(tInreplyName)
        tData.append(tInreplysID)
        tData.append(retweetcount)
        tData.append(tContributors)
        tData.append(tUid)
                       
#Geo Table Data
        if tDict['geo'] != None:
            gData = []
            gID = flag
            gType = tDict['geo']['type']
            gLongitude = tDict['geo']['coordinates'][1]
            gLatitude = tDict['geo']['coordinates'][0]
            gData.append(gID)
            gData.append(gType)
            gData.append(gLongitude)
            gData.append(gLatitude)
            c.execute("INSERT INTO Geo VALUES(?, ?, ?, ?);", gData)
            tData.append(gID)
            flag = flag + 1
        else:
            tData.append(None)
        
        c.execute("INSERT INTO Tweet VALUES(?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?);", tData)
        c.execute("INSERT INTO User VALUES(?, ?, ?, ?, ?);", uData)
    except ValueError:
        print(" ")

tRows = c.execute("SELECT COUNT(*) FROM Tweet;").fetchall()[0]
print("There are", tRows, "rows in Tweet Table.")
uRows = c.execute("SELECT COUNT(*) FROM User;").fetchall()[0]
print("There are", uRows, "rows in User Table.")
gRows = c.execute("SELECT COUNT(*) FROM Geo;").fetchall()[0]
print("There are", gRows, "rows in Geo Table.")

End1c = time.time()
RunTime1c = End1c - Start1c
#My run time is 1808 seconds
print("The run time is ", RunTime1c, "seconds.")

c.close()
conn.commit()
conn.close()


#Part1 b
"""b.	Use python to download from the web and save to a local text file 
(not into database yet) at least 500,000 lines worth of tweets. Test your 
code with fewer rows first – you can reduce the number of tweets if your 
computer is running too slow to handle 500K tweets in a reasonable time. How 
long did it take to save?
"""
import urllib.request as urllib
import time
response = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
tInput = {}
tweet = open('YangTweet.txt', 'w', encoding = 'utf8')
Start1b = time.time()
for i in range(500000):
    str_response = response.readline()
    tInput[i] = str_response
    tweet.write(str_response.decode("utf8"))
End1b = time.time()
RunTime1b = End1b - Start1b
#My run time is 1049 seconds
print("The run time is ", RunTime1b, "seconds.")
response.close()
tweet.close()

#Part1 d
"""Use your locally saved tweet file (created in part-b) to repeat the database
 population step from part-c. That is, load 500,000 tweets into the 3-table 
 database using your saved file with tweets (do not use the URL to read twitter
 data). How does the runtime compare with part-c?"""
import json, time, sqlite3
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
    geo_id                  INTEGER(50),
    CONSTRAINT Tweer_PK PRIMARY KEY(id_str),
    CONSTRAINT Tweet_FK FOREIGN KEY(user_id)
    REFERENCES User(id),
    CONSTRAINT Tweet_FK1 FOREIGN KEY(geo_id)
    REFERENCES Geo(id)
)'''

UserTable = '''CREATE TABLE User
(
    id              VARCHAR2(50),
    name            VARCHAR2(50),
    screen_name     VARCHAR2(50),
    description     VARCHAR2(50),
    friends_count   NUMBER(10),
    CONSTRAINT User_PK PRIMARY KEY(id)
)'''

GeoTable = '''CREATE TABLE Geo
(
    id              INTEGER PRIMARY KEY,
    type            VARCHAR2(50),
    longitude       VARCHAR2(50),
    latitude        VARCHAR2(50)
)
'''

conn = sqlite3.connect('YangTweet.db')
c = conn.cursor()

c.execute("DROP TABLE IF EXISTS Tweet")
c.execute(TweetTable)
c.execute("DROP TABLE IF EXISTS User")
c.execute(UserTable)
c.execute("DROP TABLE IF EXISTS Geo")
c.execute(GeoTable)

tweet = open('YangTweet.txt', 'r', encoding='utf8')

Start1d = time.time()
flag = 1
for i in range(500000):
    str_response = tweet.readline()
    try:
        tDict = json.loads(str_response)
#User Table Data
        uData = []
        uID = tDict['user']['id']
        uName = tDict['user']['name']
        uSname = tDict['user']['screen_name']
        uDescription = tDict['user']['description']
        uFcount = tDict['user']['friends_count']
        uData.append(uID)
        uData.append(uName)
        uData.append(uSname)
        uData.append(uDescription)
        uData.append(uFcount)
                      
#Tweet Table Data
        if 'retweeted_status' in tDict.keys():
            retweetcount = tDict['retweeted_status']['retweet_count']
        else:
            retweetcount = tDict['retweet_count']
        tData = []
        tCreatedat = tDict['created_at']
        tIdstr = tDict['id_str']
        tText = tDict['text']
        tSource = tDict['source']
        tInreplyID = tDict['in_reply_to_user_id']
        tInreplyName = tDict['in_reply_to_screen_name']
        tInreplysID = tDict['in_reply_to_status_id']
        tContributors = tDict['contributors']
        tUid = tDict['user']['id']
        tData.append(tCreatedat)
        tData.append(tIdstr)
        tData.append(tText)
        tData.append(tSource)
        tData.append(tInreplyID)
        tData.append(tInreplyName)
        tData.append(tInreplysID)
        tData.append(retweetcount)
        tData.append(tContributors)
        tData.append(tUid)
                       
#Geo Table Data
        if tDict['geo'] != None:
            gData = []
            gID = flag
            gType = tDict['geo']['type']
            gLongitude = tDict['geo']['coordinates'][1]
            gLatitude = tDict['geo']['coordinates'][0]
            gData.append(gID)
            gData.append(gType)
            gData.append(gLongitude)
            gData.append(gLatitude)
            c.execute("INSERT INTO Geo VALUES(?, ?, ?, ?);", gData)
            tData.append(gID)
            flag = flag + 1
        else:
            tData.append(None)
        
        c.execute("INSERT INTO Tweet VALUES(?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?);", tData)
        c.execute("INSERT INTO User VALUES(?, ?, ?, ?, ?);", uData)
    except ValueError:
        print(" ")

tRows = c.execute("SELECT COUNT(*) FROM Tweet;").fetchall()[0]
print("There are", tRows, "rows in Tweet Table.")
uRows = c.execute("SELECT COUNT(*) FROM User;").fetchall()[0]
print("There are", uRows, "rows in User Table.")
gRows = c.execute("SELECT COUNT(*) FROM Geo;").fetchall()[0]
print("There are", gRows, "rows in Geo Table.")

End1d = time.time()
RunTime1d = End1d - Start1d
#My run time is 51 seconds, this run time is less than the one in Part1 c.
print("The run time is ", RunTime1d, "seconds.")

c.close()
conn.commit()
conn.close()

#Part1 e
"""e.	Re-run the previous step with batching size of 500 (i.e. by inserting 
500 rows at a time with executemany). You can adapt the posted example code. 
How does the runtime compare when batching is used?
"""
def loadTweetsBatch(tLines):
    import json
    batchRows = 500
    gBatchInput = []
    tBatchInput = []
    uBatchInput = []
    flag = 1
    while len(tLines) > 0:
        line = tLines.pop()
        try:
            tDict = json.loads(line)
            for value in tDict.values():
                if value == "null":
                    value = None
#User Table Data
            uData = []
            uID = tDict['user']['id']
            uName = tDict['user']['name']
            uSname = tDict['user']['screen_name']
            uDescription = tDict['user']['description']
            uFcount = tDict['user']['friends_count']
            uData.append(uID)
            uData.append(uName)
            uData.append(uSname)
            uData.append(uDescription)
            uData.append(uFcount)
            
#Tweet Table Data
            if 'retweeted_status' in tDict.keys():
                retweetcount = tDict['retweeted_status']['retweet_count']
            else:
                retweetcount = tDict['retweet_count']
            tData = []
            tCreatedat = tDict['created_at']
            tIdstr = tDict['id_str']
            tText = tDict['text']
            tSource = tDict['source']
            tInreplyID = tDict['in_reply_to_user_id']
            tInreplyName = tDict['in_reply_to_screen_name']
            tInreplysID = tDict['in_reply_to_status_id']
            tContributors = tDict['contributors']
            tUid = tDict['user']['id']
            tData.append(tCreatedat)
            tData.append(tIdstr)
            tData.append(tText)
            tData.append(tSource)
            tData.append(tInreplyID)
            tData.append(tInreplyName)
            tData.append(tInreplysID)
            tData.append(retweetcount)
            tData.append(tContributors)
            tData.append(tUid)
                
#Geo Table Data
            if tDict['geo'] != None:
                gData = []
                gID = flag
                gType = tDict['geo']['type']
                gLongitude = tDict['geo']['coordinates'][1]
                gLatitude = tDict['geo']['coordinates'][0]
                gData.append(gID)
                gData.append(gType)
                gData.append(gLongitude)
                gData.append(gLatitude)
                c.execute("INSERT INTO Geo VALUES(?, ?, ?, ?);", gData)
                tData.append(gID)
                flag = flag + 1
            else:
                tData.append(None)
            tBatchInput.append(tData)
            uBatchInput.append(uData)
            if len(tBatchInput) >= batchRows:
                c.executemany("INSERT INTO Geo VALUES(?, ?, ?, ?);", gBatchInput)
                gBatchInput = []
                c.executemany("INSERT INTO Tweet VALUES(?, ?, ?, ?, ?, ?, ?, ?, ? ,?, ?);", tBatchInput)
                tBatchInput = []
            if len(uBatchInput) >= batchRows:
                c.executemany("INSERT INTO User VALUES(?, ?, ?, ?, ?);", uBatchInput)
                uBatchInput = []
            if len(gBatchInput) >= batchRows:
                c.executemany("INSERT INTO Geo VALUES(?, ?, ?, ?);", gBatchInput)
                gBatchInput = []
        except ValueError:
            print(" ")
            
import time, sqlite3

conn = sqlite3.connect('YangTweet.db')
c = conn.cursor()

response = open('YangTweet.txt', 'r')
Start1e = time.time()
loadTweetsBatch(response.readlines())

tRows = c.execute("SELECT COUNT(*) FROM Tweet;").fetchall()[0]
print("There are", tRows, "rows in Tweet Table.")
uRows = c.execute("SELECT COUNT(*) FROM User;").fetchall()[0]
print("There are", uRows, "rows in User Table.")
gRows = c.execute("SELECT COUNT(*) FROM Geo;").fetchall()[0]
print("There are", gRows, "rows in Geo Table.")

End1e = time.time()
RunTime1e = End1e - Start1e
#My run time is 0.320 seconds, batch will reduce the run time, make it faster.
print("The run time is ", RunTime1e, "seconds.")

c.close()
conn.commit()
conn.close()

#Part2
import sqlite3, time
conn = sqlite3.connect('YangTweet.db')
c = conn.cursor()
#Part2 ai
"""Find tweets where tweet id_str contains “44” or “77” anywhere in the column"""
Start2a1 = time.time()
c.execute("SELECT * FROM Tweet WHERE id_str LIKE '%44%' OR id_str LIKE '%77%';")
End2a1 = time.time()
RunTime2a1 = End2a1 - Start2a1
#My run time is 0.0009 seconds
print("The run time is ", RunTime2a1, "seconds.")

#Part2 aii
"""Find how many unique values are there in the “in_reply_to_user_id” column"""
Start2a2 = time.time()
c.execute("SELECT COUNT(DISTINCT in_reply_to_user_id) FROM Tweet;")
End2a2 = time.time()
RunTime2a2 = End2a2 - Start2a2
#My run time is 0.87 seconds
print("The run time is ", RunTime2a2, "seconds.")

#Part2 aiii
"""Find the tweet(s) with the longest text message, select id_str and text only."""
Start2a3 = time.time()
c.execute("SELECT id_str, text FROM Tweet WHERE length(text) == (SELECT max(length(text))FROM Tweet);")
End2a3 = time.time()
RunTime2a3 = End2a3 - Start2a3
#My run time is 0.62 seconds
print("The run time is ", RunTime2a3, "seconds.")

#Part2 aiv
"""Find the average longitude and latitude value for each user name."""
Start2a4 = time.time()
c.execute("SELECT u.name, avg(g.longitude), avg(latitude) FROM Tweet t, User u, Geo g\
           WHERE t.user_id = u.id AND t.geo_id = g.id GROUP BY u.name;")
End2a4 = time.time()
RunTime2a4 = End2a4 - Start2a4
#My run time is 4.76 seconds
print("The run time is ", RunTime2a4, "seconds.")

#Part2 av
"""Re-execute the query in part iv 10 times and 100 times and measure the total
 runtime (just re-run the same exact query using a for-loop). Does the runtime 
 scale linearly? """
Start2a5i = time.time()
for i in range(10):
    c.execute("SELECT u.name, avg(g.longitude), avg(latitude) FROM Tweet t, User u, Geo g\
           WHERE t.user_id = u.id AND t.geo_id = g.id GROUP BY u.name;")
End2a5i = time.time()
RunTime2a5i = End2a5i - Start2a5i
#My run time is 48.84 seconds, this run time is nearly 10x time of the one in iv.
print("The run time is ", RunTime2a5i, "seconds.")

Start2a5ii = time.time()
for i in range(100):
    c.execute("SELECT u.name, avg(g.longitude), avg(latitude) FROM Tweet t, User u, Geo g\
           WHERE t.user_id = u.id AND t.geo_id = g.id GROUP BY u.name;")
End2a5ii = time.time()
RunTime2a5ii = End2a5ii - Start2a5ii
#My run time is 474.08 seconds, this run time is nearly 10x time of the one in v.
print("The run time is ", RunTime2a5ii, "seconds.")

#Part2 b
"""Write python code that is going to read the locally saved tweet data file 
from 1-b and perform the equivalent computation for parts 2-i and 2-ii only. 
How does the runtime compare to the SQL queries"""
# ai
import time, json, re
tweet = open('YangTweet.txt','r', encoding = 'utf8')
Start2b1 = time.time()
flag = 0
for i in range(500000):
    str_response = tweet.readline()
    tDict = json.loads(str_response)
    regx = ['^44$', '^44.+$', '^.+44$', '^.+44.+$', '^77$', '^77.+$', '^.+77$', '^.+77.+$']
    for r in regx:
        m = re.compile(r)
        if m.findall(tDict['id_str']) != []:
            flag = 1 + flag
End2b1 = time.time()
RunTime2b1 = End2b1 - Start2b1
#My run time is 42 seconds
print("The run time is ", RunTime2b1, "seconds.")

# aii
import time, json
tweet = open('YangTweet.txt','r', encoding = 'utf8')
Start2b2 = time.time()
flag = []
for i in range(500000):
    str_response = tweet.readline()
    tDict = json.loads(str_response)
    if tDict['in_reply_to_user_id'] not in flag:
        flag.append(tDict['in_reply_to_user_id'])
End2b2 = time.time()
RunTime2b2 = End2b2 - Start2b2
#My run time is 144 seconds
print("The run time is ", RunTime2b2, "seconds.")

#aiii
import time, json
tweet = open('YangTweet.txt','r', encoding = 'utf8')
Start2b3 = time.time()
for i in range(500000):
    str_response = tweet.readline()
    tDict = json.loads(str_response)
    longest = max(tDict['text'].split(), key = len)
    if tDict['text'] < longest:
         print (longest)
   

#Part3 a
"""Export the contents of the User table from a SQLite table into a sequence of
 INSERT statements within a file. This is very similar to what you did in 
 Assignment 4. However, you have to add a unique ID column which has to be a 
 string (you cannot use any numbers). Hint: one possibility is to replace 
 digits with letters, e.g., chr(ord('a')+1) gives you a 'b' and chr(ord('a')+2)
 returns a 'c'"""
import sqlite3, time

def uniqueID():
    from string import ascii_lowercase
    from itertools import product
    for x in range(1, 6):
        for i in product(ascii_lowercase, repeat = x):
            yield"".join(i)

Start3a = time.time()
conn = sqlite3.connect('YangTweet.db')
c = conn.cursor()
data = c.execute("SELECT * FROM User;")
tFile = open('UserInsert.txt', 'w', encoding = 'utf8')
for eachRow in data:
    statement = ""
    for value in eachRow:
        for s in uniqueID():
            if value == None:
                value = ""
                statement = statement + s + value
            else:
                statement = statement + s + "'" + str(value) + "'"
            statement = statement + "\n"
        with open('UserInsert.txt', 'a'):
            tFile.write(statement)
tFile.close()
End3a = time.time()
RunTime3a = End3a - Start3a
#My run time is 26 seconds
print("The run time is ", RunTime3a, "seconds.")

#Part3 b
"""Create a similar collection of INSERT for the User table by reading/parsing
 data from the local tweet file that you have saved earlier. How do these 
 compare in runtime? Which method was faster?"""
import time, json

def uniqueID():
    from string import ascii_lowercase
    from itertools import product
    for x in range(1, 6):
        for i in product(ascii_lowercase, repeat = x):
            yield"".join(i)
        
Start3b = time.time()
data = open('YangTweet.txt', 'r', encoding = 'utf8')
tFile = open('UserInsert1.txt', 'w', encoding = 'utf8')
for i in range(500000):
    statement = ""
    str_response = data.readline()
    tDict = json.loads(str_response)
    uID = tDict['user']['id']
    uName = tDict['user']['name']
    uSname = tDict['user']['screen_name']
    uDescription = tDict['user']['description']
    uFcount = tDict['user']['friends_count']
    for s in uniqueID():        
        statement = statement + s + "," + str(uID) + "," + str(uName) + "," + str(uSname) + "," + str(uDescription) + "," + str(uFcount) + "\n"
    with open('UserInsert1.txt', 'a'):
        tFile.write(statement)
tFile.close()

End3b = time.time()
RunTime3b = End3b - Start3b
#My run time is 56 seconds, after compare first method is much faster.
print("The run time is ", RunTime3b, "seconds.")
    

#Part4 a
"""For the Geo table, at the time of your export, create a single default entry
 for the ‘Unknown’ location and round longitude and latitude to a maximum of 4 
 digits after the decimal."""
import sqlite3, time
Start4a = time.time()

conn = sqlite3.connect('YangTweet.db')
c = conn.cursor()
gData = c.execute("SELECT * FROM Geo;").fetchall()
gText = open('YangExportGeo.txt', 'w', encoding = 'utf8')
flag = 0

for d in gData:
    text = " "
    data = list(d)
    for i in range(len(data)):
        j = data[i]
        data[2] = round(float(data[2]), 4)
        data[3] = round(float(data[3]), 4)
        if type(j) == type(0):
            text = text + str(j) 
        elif type(j) == type(0) and i != 2:
            text = text + str(j) + "|"
        else:
            if type(j) == int:
                text = text + str(round(j, 4)) + "|"
            else:
                text = text + "'" + str(j) + "'" + "|"
    if i < len(data) - 1:
        text = text + "|"
    output = text + "\n"
    gText.write(output)
    flag = flag + 1
    gID = flag + 1
    formate = " " + str(gID) + "| 'Unknown' | 'Unknown' | 'Unknown'"
gText.write(formate)
gText.close()
c.close()
conn.close()

End4a = time.time()
RunTime4a = End4a - Start4a
#My run time is 0.2 seconds
print("The run time is ", RunTime4a, "seconds.")


#Part4 b
"""a.	For the Tweet table, for tweets without a location, replace their foreign
 key NULL value with a reference to ‘Unknown’ entry you added in part-a (i.e., 
 the foreign key column that references Geo table should refer to the “Unknown”
 entry you created in part-a). Report how many known/unknown locations there 
 were in total (e.g., 10,000 known, 490,000 unknown,  2% locations are available)
"""
import sqlite3, time
conn = sqlite3.connect('YangTweet.db')
c = conn.cursor()

Start4b = time.time()
gData = c.execute("SELECT * FROM Geo;").fetchall()
tData = c.execute("SELECT * FROM Tweet;").fetchall()
tText = open('YangExportTweet.txt', 'w', encoding = 'utf8')

nValue = 0

for line in tData:
    text = " "
    data = list(line)
    for i in range(len(data)):
        j = data[i]
        if data[10] == None:
            nValue = nValue + 1
            data[10] = (len(gData) + 1)
            if j == 0:
                text = text + str(j) + "|"
            else:
                text = text + str(j) + "|"     
        if i < len(data) - 1:
            text = text + " "
        output = text + "\n"
        tText.write(output)
tText.close()

aValue = len(tData)
value = aValue - nValue
percent = round(((value/aValue) * 100), 2)
print(value, " known", nValue, " unknown", percent, " locations are available.")

c.close()
conn.close()

End4b = time.time()
RunTime4b = End4b - Start4b
#My run time is 8.9 seconds
print("The run time is ", RunTime4b, "seconds.")

        
#Part4 c
"""For the User table file add a column (true/false) that specifies whether 
“screen_name” or “description” attribute contains within it the “name” attribute
 of the same user. That is, your output file should contain all of the columns
 from the User table, plus the new column. You do not have to modify the 
 original User table."""
import sqlite3, time
conn = sqlite3.connect('YangTweet.db')
c = conn.cursor()

Start4c = time.time()
uData = c.execute("SELECT * FROM User;").fetchall()
uText = open('YangExportUser.txt', 'w', encoding = 'utf8')

for line in uData:
    text = " "
    l = list(line)
    for(i, j) in enumerate(l):
        if l[1] != None:
            if l[3] == None:
                if l[1] in l[2]:
                    boolean = True
                else:
                    boolean = False
            else:
                if l[1] in l[2] and l[1] in l[3]:
                    boolean = True
                elif l[1] in l[2] or l[1] in l[3]:
                    boolean = True
                else:
                    boolean = False
        else:
            boolean = False
        if j == None:
            text = text + "|" + str("NULL") + "|"
        else:
            text = text + str(j) + "|"
        if i < len(l) - 1:
            text = text + ","
        output = text + str(boolean) + "\n"
        uText.write(output)
uText.close()
c.close()
conn.close()

End4c = time.time()
RunTime4c = End4c - Start4c
#My run time is 12 seconds
print("The run time is ", RunTime4c, "seconds.")

#Part 5
"""a.	Describe the patterns you’ve noticed from all the comparisons above. 
What have you learned from this, what seems to be working faster overall, 
what are the strengths and weaknesses of the different approaches (file vs db 
reads, file vs db write/inserts)?
"""

#==============================================================================
#From the part 1, insert from website, it spends 1808 seconds on my computer,
#but download to local file and insert, it spends 1049 + 51 = 1100 seconds on 
#my computer, so it is faster when use local files; however, Part 3, if we use 
#local file, it spends me 56 seconds, but from database it spends me 26 sceonds
#In my view, database is organized well, so it will spends less time, but the 
#local file is original version of data, so it will spend more time on orgnize
#and then output. From part 2 and part 3, it is very clear that read from database
#using DDL is much faster than read from local file using python, so I think if
#want to do more things about database, it is much faster to use DDL, it will 
#improve efficiency.
#==============================================================================
