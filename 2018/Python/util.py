#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  6 16:42:01 2018

@author: hardyhe
"""

import pandas as pd

def codec(x):
    try:
        return x.encode('latin1').decode('gbk')
    except:
        return '-1'

def timeTrans(x):
    x = x.replace('/','-')
    x += ':00'
    return x

def load_data(option,filename):
    PATH = '../Data/'   
    
    # first load
    DAY = '2017-11-01' 
            
    df = pd.read_csv(PATH+DAY+'/'+filename+'.csv',encoding='latin-1')
    df['subject'] = df.subject.apply(codec)
    df = df[df['subject'] != '-1']

    if option == "all":
        print(option)
        # load all
        for i in range(2,31):
            if i < 10:
                day = '2017-11-0' + str(i)
            else:
                day = '2017-11-' + str(i)
            
            try:
                tmp = pd.read_csv(PATH + day + '/'+filename+'.csv',encoding='latin-1')
                tmp['subject'] = tmp.subject.apply(codec)
                tmp = tmp[tmp['subject'] != '-1']
                if i == 3:
                    tmp.time = tmp.time.apply(timeTrans)
                df = pd.concat([df,tmp])
            except:
                print(day)

    print("Load Data Success!")
    return df
