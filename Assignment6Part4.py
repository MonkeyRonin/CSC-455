# -*- coding: utf-8 -*-
"""
Created on Mon May 16 23:11:27 2016

@author: Yiyang
"""

def generateInsertStatement(table):
    sql = "INSERT INTO " + table + " VALUES "
    values = []
    for row in table:
    values.append("(%s, %s, %s, %s)" % 
    (row['item1'], row['item2'], row['item3'] , row['item4']))
    sql = sql + ",".join(values)
    return sql
