# -*- coding: utf-8 -*-
"""
Created on Wed May 11 18:01:00 2016

@author: Yiyang
"""

def ORFunction(a, b):
    import numpy as np
    arr1 = np.array(a)
    arr2 = np.array(b)
    if arr1.size == arr2.size:
        return np.logical_or(arr1, arr2)
    else:
        return "Error"