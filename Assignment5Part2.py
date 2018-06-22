# -*- coding: utf-8 -*-
"""
Created on Sun May  1 23:01:02 2016

@author: Yiyang
"""

import sqlite3
from sqlite3 import OperationalError

conn = sqlite3.connect('csc455_HW3.db')
c = conn.cursor()


fd = open('Assignment5Part1.sql', 'r')
sqlFile = fd.read()
fd.close()

sqlCommands = sqlFile.split(';')

for command in sqlCommands:
    try:
        c.execute(command)
        print ("executed command: "+command)
    except OperationalError:
        print ("Command skipped: "+ command)

for table in ['ZooKeeper', 'Animal', 'Handles']:
    r = c.execute("SELECT * FROM %s;" % table)

    rows = r.fetchall()
    
    print ("\n",table, "\n")

    for d in r.description:
        print (d[0], "\n")
    
    for row in rows:
        for value in row:
            print (str(value), "\n")

c.close()
conn.commit()
conn.close()
