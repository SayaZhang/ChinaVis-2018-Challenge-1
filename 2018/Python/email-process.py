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
import json
    
def raw_to_sql(conn):
    df = util.load_data('all','email')
    df.to_sql('email',con=conn,if_exists='replace',index=None)

def staff2ip(conn):
    df = util.load_data('all','email')
    df['name'] = df['from'].apply(lambda x: x.split("@")[0])
    df['domain'] = df['from'].apply(lambda x: x.split("@")[1])
    #df[df['domain'].str.contains('hightech')][['name','sip','sport']].drop_duplicates().to_sql('staff2ip:port',con=conn,if_exists='replace')

def flatten(conn):
    sql = 'SELECT `from`,`to`,`subject`  FROM `email` WHERE  `email`.`from` LIKE "%%hightech.com"  and `email`.`to` LIKE "%%hightech.com"  ORDER BY `email`.`time`  ASC'
    df = pd.read_sql_query(sql, conn)
    
    data = []
    for index,row in df.iterrows():
        source = row['from'].split('@')[0]
        
        if source in ['ti','school','notice','guanhuai','fuli','notice','hr','kaoqin','allstaff','meeting','finance']:
            continue

        if source == 'work':
            row['subject'] = row['subject'][1:].split(']')[0]
        
        # 群发
        if ';' in row['to']:
            toes = row['to'].split(';')
            for x in toes:
                target = x.split('@')[0]
                data.append([source, target, row['subject']])
        else:
            data.append([source, row['to'].split('@')[0], row['subject']])

    data = pd.DataFrame(data, columns=['from','to','subject'])
    data.to_sql('email_hightech_flatten',con=conn,if_exists='replace',index=None)

def subject2word(conn):
    sql = 'SELECT DISTINCT `subject`  FROM `email_hightech_flatten` WHERE 1'
    df = pd.read_sql_query(sql, conn)
    
    data = []
    for x in df['subject']:
        if 'ALARM' in x or 'RECOVER' in x:
            continue

        data.append(x)
    data = pd.Series(data)
    data.to_csv('../Stat/subject.csv',index=None,encoding='gbk')


def merge_subject(conn):
    sql = 'SELECT *  FROM `email_hightech_flatten` WHERE 1'
    df = pd.read_sql_query(sql, conn)
    topic = pd.read_csv('../Stat/subject.csv',encoding='gbk')
    df = pd.merge(df,topic,on='subject',how='left')
    df['topic'] = df['topic'].fillna('研发')
    df.to_sql('email_hightech_flatten_with_topic',con=conn,if_exists='replace',index=None)

def flat_freq(coon):
    '''
    sql = 'SELECT *,COUNT(*) AS freq FROM `email_hightech_flatten_with_topic` WHERE 1 GROUP BY `from`,`to`,`topic`'
    df = pd.read_sql_query(sql, conn)
    df[['from','to','topic','freq']].to_sql('freq_email_hightech_flatten_with_topic',con=conn,if_exists='replace',index=None)
    '''
    sql = 'SELECT * FROM `freq_email_hightech_flatten_with_topic` WHERE `freq`>3 and `topic` <> "研发-监控" ORDER BY `freq` DESC'
    df = pd.read_sql_query(sql, conn)
    print(df.head())

    data = {
        'category': [],
        'nodes': [],
        'links': []
    }

    category_list = list(df['topic'].drop_duplicates())
    category_dict = {}
    for i in range(len(category_list)):
        category_dict[category_list[i]] = i
        data['category'].append({
            'name': category_list[i]
        })

    node_dict = {}
    i = 0
    tmp1 = df[df['topic'] != '综合'][['from','topic','freq']]
    tmp1.columns = ['node','topic','freq']
    
    tmp2 = df[df['topic'] != '综合'][['to','topic','freq']]
    tmp2.columns = ['node','topic','freq']
    
    tmpdf = pd.concat([tmp2,tmp1])
    print(tmpdf.head())
    for x in tmpdf.groupby('node'):
        node_dict[x[0]] = str(i)
        data['nodes'].append({
            'id': str(i),
            'name': x[0],
            'category': category_dict[list(x[1]['topic'])[0]]
        })
        i += 1

    i = 0
    for index,row in df.iterrows():
        try:
            data['links'].append({
                'id': str(i),
                'source': node_dict[row['from']],
                'target': node_dict[row['to']]
            })
            i += 1
        except:
            print(row)

    with open('../flat_freq.json', 'w') as file_obj:
        '''写入json文件'''
        json.dump(data, file_obj)


def staff2department_to_sql(conn):
    with open("../department.json",'r') as load_f:
        load_dict = json.load(load_f)
        print(load_dict)

    data = []
    for x in load_dict:

        if x == '财务' or x == '人力资源':
            for staff in load_dict[x]:
                data.append([staff,x])

        if x == '研发':
            for department1 in load_dict[x]:
                for dedepartment2 in load_dict[x][department1]:
                    if dedepartment2 == 'leader':
                        data.append([load_dict[x][department1]['leader'],'研发_'+department1+'_leader'])
                    else:
                        index = 0
                        for i in load_dict[x][department1][dedepartment2]:
                            if index == 0:
                                data.append([i,'研发_'+department1+'_'+dedepartment2+'_leader'])
                            else:
                                data.append([i,'研发_'+department1+'_'+dedepartment2])
                            index += 1


    df = pd.DataFrame(data, columns=['staff','department'])
    df.to_sql('staff2department',con=conn,if_exists='replace',index=None)

def department2subject(conn):
    
    departments = []
    for i in range(1,8):
        departments.append('研发_A_'+str(i))
    for i in range(1,12):
        departments.append('研发_B_'+str(i))
    for i in range(1,10):
        departments.append('研发_C_'+str(i))
    
    sql = 'SELECT DISTINCT `email_hightech_flatten_with_topic`.`subject`,COUNT(*) as freq FROM `staff2department`,`email_hightech_flatten_with_topic` WHERE `staff2department`.department LIKE "{0}%%" AND `staff2department`.`staff` = `email_hightech_flatten_with_topic`.`from` GROUP BY `subject` ORDER BY COUNT(*) DESC'.format(departments[0])
    df = pd.read_sql_query(sql, conn)
    df['department'] = departments[0]

    for i in range(1,len(departments)):
        print(departments[i])
        sql = 'SELECT DISTINCT `email_hightech_flatten_with_topic`.`subject`,COUNT(*) as freq FROM `staff2department`,`email_hightech_flatten_with_topic` WHERE `staff2department`.department LIKE "{0}%%" AND `staff2department`.`staff` = `email_hightech_flatten_with_topic`.`from` GROUP BY `subject` ORDER BY COUNT(*) DESC'.format(departments[i])
        tmp = pd.read_sql_query(sql, conn)
        tmp['department'] = departments[i]
        df = pd.concat([df,tmp])

    df.to_sql('department2subject',con=conn,if_exists='replace',index=None)

def department2subject2json(conn):
    sql = 'SELECT * FROM `department2subject` WHERE 1'
    df = pd.read_sql_query(sql, conn)

    x = list(df['subject'].drop_duplicates())
    y = list(df['department'].drop_duplicates().sort_values())

    data = []
    for group in df.groupby('department'):
        print(group[0])
        total = group[1]['freq'].sum()

        for index,row in group[1].iterrows():
            data.append([row['subject'], row['department'], float(row['freq'])/total])

    with open('../department2subject.json', 'w') as file_obj:
        json.dump({'x':x, 'y':y, 'data':data}, file_obj)

def staff2json(conn):
    with open("../Output/department.json",'r') as load_f:
        load_dict = json.load(load_f)
    
    data = {
        'name': 'hightech',
        'children': []
    }

    for x in load_dict:

        if x == '人力资源' or x == '财务':
            print(x)
            tmp = []
            for staff in load_dict[x]:
                tmp.append({
                    'name': staff,
                    'value': 0
                })

            data['children'].append({
                'name': str(x),
                'children': tmp
            })

        if x == '研发':
            children1 = []
            for a in load_dict[x]:
                print("a",a)
                children2 = []
                for b in load_dict[x][a]:
                    print("b",b)
                    print(load_dict[x][a][b])


                    tmp = []
                    for staff in load_dict[x][a][b]:
                        tmp.append({
                            'name': staff,
                            'value': 0
                        })

                    children2.append({
                        'name': b,
                        'children': tmp
                    })

                children1.append({
                    'name': a,
                    'children': children2
                })


            data['children'].append({
                'name': str(x),
                'children': children1
            })



    with open('../staff.json', 'w') as file_obj:
        json.dump(data, file_obj)

    


if __name__ == "__main__":
    
    conn = create_engine('mysql+pymysql://root:123456@localhost:3306/chinavis?charset=utf8??unix_socket=/Applications/XAMPP/xamppfiles/var/mysql/mysql.sock') 
    staff2json(conn)




