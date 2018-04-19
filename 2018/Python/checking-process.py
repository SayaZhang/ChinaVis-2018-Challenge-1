#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  6 16:42:01 2018

@author: hardyhe
"""

import pandas as pd
import MySQLdb
from sqlalchemy import create_engine
import util
    
def raw_to_sql(conn):
    df = util.load_data('all','checking')
    df.to_sql('checking',con=conn,if_exists='replace')

if __name__ == "__main__":
    
    conn = create_engine('mysql+pymysql://root:123456@localhost:3306/chinavis?charset=utf8??unix_socket=/Applications/XAMPP/xamppfiles/var/mysql/mysql.sock') 
    raw_to_sql(conn)