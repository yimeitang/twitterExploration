# -*- coding: utf-8 -*-
"""
Retrieve 1M tweets from web and store it as a local text file
Generate three sqlite tables of 1M tweets

Created on Wed June 14 16:27:48 2017

@author: Yimei Tang
"""
import json
import sqlite3
import urllib
import codecs
import time
#%%Create sqlite tables
###Step 1: Prepare sqlite commands
geotable = '''CREATE TABLE Geo (
    ID NUMBER NOT NULL ,
    Type VARCHAR2(150),
    Longitude NUMBER ,
    Latitude NUMBER,
    CONSTRAINT Geo_PK PRIMARY KEY (ID)
    );'''

usertable = '''CREATE TABLE Users (
    ID NUMBER NOT NULL,
    Name VARCHAR2(150),
    Screen_name VARCHAR2(150),
    Description VARCHAR2(150),
    Friends_count NUMBER,
    CONSTRAINT Users_PK PRIMARY KEY (ID)
    );'''

tweetstable = '''CREATE TABLE Tweets (
    ID          NUMBER(20) NOT NULL,
    Created_At  DATE,
    Text        CHAR(140),
    Source VARCHAR(200) DEFAULT NULL,
    In_Reply_to_User_ID NUMBER(20),
    In_Reply_to_Screen_Name VARCHAR(60),
    In_Reply_to_Status_ID NUMBER(20),
    Retweet_Count NUMBER(10),
    Contributors  VARCHAR(200),
    UserID NUMBER,
    GeoID  NUNBER,
    CONSTRAINT Tweets_PK  PRIMARY KEY (ID),
    CONSTRAINT Tweets_FK1 FOREIGN KEY (UserID) REFERENCES Users (ID),
    CONSTRAINT Tweets_FK2 FOREIGN KEY (GeoID) REFERENCES Geo (ID)
    );'''


###Step 2: Connect to a database and execute sqlite commands

conn = sqlite3.connect('storyTelling.db')
c = conn.cursor()
c.execute("DROP TABLE IF EXISTS Geo")
c.execute(geotable)
c.execute("DROP TABLE IF EXISTS Users")
c.execute(usertable)
c.execute("DROP TABLE IF EXISTS Tweets")
c.execute(tweetstable)

#%%Save 1M tweets in a local text file
###Step 1: Save 1M rows into local text file for future use

start = time.time()
wFD = urllib.request.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
file = codecs.open('TweetFile.txt','w', 'utf-8')
count = 1
numTweets = 1000000
while count <=numTweets:
    tweetfiles = wFD.readline().decode("utf8")
    file.write(tweetfiles)
    count+=1
    
wFD.close()
file.close()
end = time.time()
print ("It takes ", round((end-start),4), "seconds to load " + str(numTweets) + " tweets to a local text file")

#%%Insert values into sqlite tables 
###Method 1 - read and execute oneline from web at a time
start = time.time()
wFD = urllib.request.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
count = 1
GeoID = 1    
while count <= 10:
    tweetline = wFD.readline().decode("utf8")    
    tDict = json.loads(tweetline)
    if tDict['coordinates'] == None or tDict['coordinates']['type'] == None:
        Type=None
    else:
        Type=tDict['coordinates']['type']
    if tDict['coordinates'] == None or tDict['coordinates']['coordinates'] == None:
        Longitude=None
        Latitude=None
    else:
        Longitude=float(tDict['coordinates']['coordinates'][0])
        Latitude=float(tDict['coordinates']['coordinates'][1])
    geodata=[GeoID,Type,Longitude,Latitude]
    
    c.execute("INSERT INTO Geo VALUES (?, ? , ? , ?)",geodata)
    
    if tDict['user']['id']==None:
        UserID=None
    else:
        UserID=int(tDict['user']['id'])
    if tDict['user']['friends_count']==None:
        Friends_count=None
    else:
        Friends_count=int(tDict['user']['friends_count'])
    Name=tDict['user']['name']
    Screen_name=tDict['user']['screen_name']
    Description=tDict['user']['description']
    userdata=[UserID,Name,Screen_name,Description,Friends_count]
    c.execute("INSERT INTO Users VALUES (?, ?, ?, ?, ?)",userdata)
    
    tweetsdata=[]
    tweetKeys = ['id_str','created_at','text','source','in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'retweet_count', 'contributors']
    for key in tweetKeys:
        if tDict[key] in ['',[],'null']:
            tweetsdata.append(None)
        else:
            tweetsdata.append(tDict[key])
    tweetsdata.append(UserID)
    tweetsdata.append(GeoID)
    c.execute("INSERT OR IGNORE INTO Tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",tweetsdata)       
    count += 1
    GeoID += 1
    
wFD.close()
c.close()
conn.commit()
conn.close() 
end = time.time()
print ("The Time It Takes is ", round((end-start),4), "seconds")

#%%Insert values into sqlite tables 
###Method 2 - read and execute multiple lines from web at a time
###Step 1: First define a loadTweets function
def loadTweets(tweetLines,batchsize):
    error = 0
    GeoId =1   
    # Collect multiple rows so that we can use "executemany".
    # We insert batchRows records at a time in this function.
    batchRows = batchsize
    geoInserts = []
    userInserts = []
    tweetsInsert= []


    # as long as there is at least one line remaining
    while len(tweetLines) > 0:
        try: 
            line = tweetLines.pop(0) 
            tDict = json.loads(line)

            geoNewRow = [] # for Geo Table
            if tDict['coordinates'] == None or tDict['coordinates']['type'] == None:
                Type=None
            else:
                Type=tDict['coordinates']['type']
            if tDict['coordinates'] == None or tDict['coordinates']['coordinates'] == None:
                Longitude=None
                Latitude=None
            else:
                Longitude=float(tDict['coordinates']['coordinates'][0])
                Latitude=float(tDict['coordinates']['coordinates'][1])
            geoNewRow=[GeoId,Type,Longitude,Latitude]
            geoInserts.append(geoNewRow)
            GeoId = GeoId+1
        
        
            userNewRow = [] # for User Table
            if tDict['user']['id']==None:
                UserID=None
            else:
                UserID=int(tDict['user']['id'])
            if tDict['user']['friends_count']==None:
                Friends_count=None
            else:
                Friends_count=int(tDict['user']['friends_count'])
            Name=tDict['user']['name']
            Screen_name=tDict['user']['screen_name']
            Description=tDict['user']['description']
            userNewRow=[UserID,Name,Screen_name,Description,Friends_count]
            userInserts.append(userNewRow)
     
        
            tweetsNewRow=[] # for Tweets Table
            tweetKeys = ['id_str','created_at','text','source','in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'retweet_count', 'contributors']
            for key in tweetKeys:
                if tDict[key] in ['',[],'null',None]:
                    tweetsNewRow.append(None)
                else:
                    tweetsNewRow.append(tDict[key])
            tweetsNewRow.append(UserID)
            tweetsNewRow.append(GeoId)
            tweetsInsert.append(tweetsNewRow)
        
            if len(geoInserts) >= batchRows or len(tweetLines) == 0:
            
                c.executemany('INSERT INTO Geo VALUES(?,?,?,?)', geoInserts)
                c.executemany("INSERT OR IGNORE INTO Users VALUES (?, ?, ?, ?, ?)",userInserts)
                c.executemany("INSERT OR IGNORE INTO Tweets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",tweetsInsert)   
                # Reset the batching process
                geoInserts = []
                userInserts = []
                tweetsInsert= []
        
        except ValueError:
            error += 1  
        
#%%Insert values into sqlite tables  
###Method 2 - read and execute multiple lines from web at a time
###Step 2: use the loadTweet function   
start = time.time()  
wFD = urllib.request.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt") 
tweetLines = []   
count = 1
 
while count <= 100:
    tweetline = wFD.readline().decode("utf8")    
    tweetLines.append(tweetline)
    count +=1
print(tweetLines[0])
loadTweets(tweetLines,10 )
print ("Loaded ", c.execute('SELECT COUNT(*) FROM Tweets').fetchall()[0], " rows")
wFD.close()
c.close()
conn.commit()
conn.close()
end = time.time()
print ("It takes is ", round((end-start),4), "seconds to load")


#%%Insert values into sqlite tables 
###Method 3 - read and execute multiple lines from a local text file
###Step 1: define the loadTweet function
###Step 2: use the loadTweet function   
start = time.time()  
file = codecs.open('TweetFile.txt','r', 'utf-8')
tweetLines = file.readlines()
numTweets = len(tweetLines)
loadTweets(tweetLines,1000)
print ("Loaded ", c.execute('SELECT COUNT(*) FROM Tweets').fetchall()[0], " tweets")
file.close()
c.close()
conn.commit()
conn.close()
end = time.time()

print ("It takes ", round((end-start),4), "seconds to load " + str(numTweets) + " lines from local text file to sqlite database")


#%%