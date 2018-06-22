# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 17:07:40 2016

@author: Yiyang
"""

def generateInser(table_name, info):
    CT = ('''CREATE TABLE Students
                (ID     NUMBER(5),
                 NAME   VARCHAR(25),
                 CREDIT VARCHAR(5),
                 CONSTRAINT STUDENT_PK
                 PRIMARY KEY(ID)
                 );''')
    ins = ("INSERT INTO Students(ID, NAME, CREDIT) VALUES (1, 'Jane', 'A-');")
    return ins

def generateInsert1(table_name, info):
    CT1 = ('''CREATE TABLE Phones
                (ID       VARCHAR(5),
                 PHONENUM VARCHAR(10),
                 CONSTRAINT STUDENT_PK
                 PRIMARY KEY(ID)
                 );''')
    ins1 = ("INSERT INTO Phones(ID, PHONENUM) VALUES ('42', '312-555-1212');")
    return ins1