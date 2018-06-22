# -*- coding: utf-8 -*-
"""
Created on Mon May  2 17:59:47 2016

@author: Yiyang
"""

import csv
import sqlite3

ChauffeurTable = '''CREATE TABLE CHAUFFEUR
(
  CHAUFFEURCITY  VARCHAR2(30),
  CHAUFFEURSTATE VARCHAR2(20),
  CONSTRAINT Chauffeur_PK
    PRIMARY KEY(CHAUFFEURCITY)
);'''

RecordTable = '''CREATE TABLE RECORD
(
  RECORDNUMBER      NUMBER(20),
  LICENSENUMBER     NUMBER(20),
  RENEWED           DATE,
  STATUS            VARCHAR2(20),
  STATUSDATE        DATE,
  DRIVERTYPE        VARCHAR2(20),
  LICENSETYPE       VARCHAR2(20),
  ORIGINALISSUEDATE DATE,
  NAME              VARCHAR2(20),
  SEX               VARCHAR2(10),
  CHAUFFEURCITY     VARCHAR2(30),
  CONSTRAINT Record_PK
    PRIMARY KEY(RECORDNUMBER),
  CONSTRAINT Rrcord_FK
    FOREIGN KEY(CHAUFFEURCITY)
    REFERENCES CHAUFFEUR(CHAUFFEURCITY)
);'''

conn = sqlite3.connect("RecordDatabase.db")

cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS CHAUFFEUR")
cursor.execute(ChauffeurTable)

cursor.execute("DROP TABLE IF EXISTS RECORD")
cursor.execute(RecordTable)

with open('Public_Chauffeurs_Short.csv') as f:
    for row in csv.DictReader(f):
        to_db = [row['Chauffeur City'], row['Chauffeur State']]
        cursor.execute('''INSERT INTO CHAUFFEUR(CHAUFFEURCITY, CHAUFFEURSTATE)
                            VALUES (?, ?)''', to_db)
        for cell in row:
        to_db1 = [row['Record Number'], row['License Number'], row['Renewed', if cell == 'NULL': None], row['Status'], 
                 row['Status Date'], row['Driver Type'], row['License Type', if cell == 'NULL': None], row['Original Issue Date', if cell == 'NULL': None], row['Name'], row['Sex'], row['Chauffeur City']]
        cursor.execute('''INSERT INTO RECORD(RECORDNUMBER, LICENSENUMBER, RENEWED, STATUS, STATUSDATE, DRIVERTYPE, LICENSETYPE, ORIGINALISSUEDATE, NAME, SEX, CHAUFFEURCITY) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', to_db1)
conn.commit()
conn.close()