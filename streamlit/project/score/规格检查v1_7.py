# coding=utf-8
import numpy as np
import pandas as pd
import re
import warnings
from fuzzywuzzy import fuzz,process
import os
import sys
warnings.filterwarnings("ignore")


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common


#20230417
#修复了产品名称有数字、单位导致计算错误的问题
#修复了规格中带有小数点的问题

def sql_whole():
    sql='''
        select
        CAST(客户品类 AS nvarchar ( 2000 )),CAST(品类 AS nvarchar ( 2000 )),CAST(数据库名 AS nvarchar ( 2000 )),CAST(单规格是否带单位 AS nvarchar(2000)),\
        CAST(产品名称字段名 AS nvarchar ( 2000 )),CAST(单规格字段名 AS nvarchar ( 2000 )),CAST(套装数字段名 AS nvarchar ( 2000 )),CAST(总规格字段名	AS nvarchar ( 2000 )),\
        CAST(制造商字段名 AS nvarchar ( 2000 )) from [QC].[dbo].[产品名称规格套装数判断]

    '''
    database='QC'
    x=[ '客户品类','品类','数据库名','单规格是否带单位','产品名称字段名','单规格字段名','套装数字段名','总规格字段名','制造商字段名']

    df= sql_connect.select('192.168.0.15',database,'sqlsever',sql,x)
    return df


def sql_check(list,time1,time2):
    if list[10]=='null':
        sql=f'''
            SELECT distinct \
            cast({list[4]} as NVARCHAR(1000)) 产品名称,\
            cast({list[5]} as NVARCHAR(1000)) 单位包装规格, \
            cast({list[6]} as float ( 53 )) 套装数,\
            cast({list[7]} as float ( 53 ))  总规格数,\
            cast({list[8]} as NVARCHAR(1000)) 制造商 from {list[2].replace('[','').replace(']','')}
        '''
        print(sql)
    else:
        sql=f'''
            SELECT distinct \
            cast({list[4]} as NVARCHAR(1000)) 产品名称,\
            cast({list[5]} as NVARCHAR(1000)) 单位包装规格, \
            cast({list[6]} as float ( 53 )) 套装数,\
            cast({list[7]} as float ( 53 ))  总规格数,\
            cast({list[8]} as NVARCHAR(1000)) 制造商 from {list[2].replace('[','').replace(']','')}\
            where SUBSTRING( REPLACE( {list[10]},'-',''),0,7) between {time1} and {time2}
        '''
        print(sql)
    database=list[2].split('.')[0].replace('[','').replace(']','')
    x=['产品名称','单规格','套装数','总规格','制造商']
    df= sql_connect.select('192.168.0.15',database,'sqlsever',sql,x)
    return df

def test(df):
    list=df['产品名称'].tolist()
    zongguige=[]
    taozhuangshu=[]
    danguige=[]
    error=[]
    for str1 in list :
        try: 
            replace_list=['2go','9mlk','3.7g倍鲜','lg21','4.1g鲜牛奶','3.2g鲜牛奶','4.4g高蛋白','15g蛋白质','25g蛋白质'\
                          'g01冷压鲜榨','g03冷压鲜榨','g05冷压鲜榨','g7冷压鲜榨','g09冷压鲜榨','g11冷压鲜榨','华茂g3'\
                          ]
            for iii in replace_list:
                str1=str1.replace(iii,'')           
            list1=re.split('[+]|\s', str1)
            list_1=['八克白']            
            list_2=['3.3g','3.6g','3.8g','5.7g','3.6g乳蛋白','3.8g乳蛋白','3.3g乳蛋白','3.2g纯牛奶','3.3g纯牛奶','3.6g纯牛奶',\
                    '3.8g纯牛奶','3.2g蛋白','3.3g蛋白','3.6g蛋白','3.8g蛋白','5g蛋白','液体版v3.3','g1','g2','g3','g4','g5','g7','g5+'\
                    '25g蛋白质','4.0g','3.5g'
                    ]
            dict1={
                '通用':list_2,
                '八克白':['14g','21g','30g'],

            }
            if list1[0] in list_1:
                for x in dict1['八克白']:
                    for y in list1:
                        if y == x:
                            list1.remove(y)
                        else:
                           continue
            list1.remove(list1[0])
            for i in list_2:
                for m in list1:
                    if m == i :
                        list1.remove(m)
                    else:
                        continue
            
            a=[]
            sum_a=0;sum_b=0
            for str2 in list1:
                x=len(re.findall(r'\d',str2))
                y1=fuzz.partial_ratio(str2,'ml')
                y2=fuzz.partial_ratio(str2,'g')
                y3=fuzz.partial_ratio(str2,'mg')
                if (y1 ==100 or y2 ==100 or y3==100) and x>0:
                    a.append(str2.split('+')[0])
                    list2=str2.replace('ml','').replace('g','').replace('mg','').split('*')
                    suma=1
                    for num in list2:
                        suma=suma*float(num)
                    sum_a+=suma
                    sum_b+=suma/float(list2[0])
            danguige.append(a[0].split('*')[0])
            taozhuangshu.append(sum_b)
            zongguige.append(sum_a)
            error.append('no')
        except:
            error.append('名称错误')
    df['错误']=error
    df1=df[df['错误']=='no']
    df2=df[df['错误']=='名称错误']
    df1['单规格结果']=danguige
    df1['总规格结果']=zongguige
    df1['套装数结果']=taozhuangshu
    return df1,df2


def check(df,a):
    if a=='是':
        df[['套装数','总规格']]=df[['套装数','总规格']].astype('float')
        df[['套装数','总规格','套装数结果','总规格结果']]=df[['套装数','总规格','套装数结果','总规格结果']].round(2)
        df_wrong=df[(df['套装数']!=df['套装数结果'])|(df['总规格']!=df['总规格结果'])|(df['单规格']!=df['单规格结果'])]
    else:
        df['单规格结果']=df['单规格结果'].str.replace('ml','').str.replace('g','').str.replace('mg','')
        df[['套装数','总规格','单规格','单规格结果']]=df[['套装数','总规格','单规格','单规格结果']].astype('float')
        df[['套装数','总规格','套装数结果','总规格结果','单规格','单规格结果']]=df[['套装数','总规格','套装数结果','总规格结果','单规格','单规格结果']].round(2)
        df_wrong=df[(df['套装数']!=df['套装数结果'])|(df['总规格']!=df['总规格结果'])|(df['单规格']!=df['单规格结果'])] 
    return df_wrong
             



def main(kehu_name,time1,time2):
    df=sql_whole()


    error=[]
    kehudf=df[df['客户品类']==kehu_name].reset_index(drop=True)
    list1=[]
    list2=[]
    for m in range(0,len(kehudf)):
        list_whole=kehudf.loc[m].tolist()
        try:
            df_new=sql_check(list_whole,time1,time2)
        except:
            error.append(f'{kehu_name}{list_whole[2]}sql语句出错')
            continue
        df_new['是否带单位']=list_whole[3]
        result1,result2=test(df_new)

        result1=check(result1,list_whole[3])            
        result1['数据库名']=list_whole[2].replace('[','').replace(']','')
        result1['品类']=list_whole[1]      
        result2['数据库名']=list_whole[2].replace('[','').replace(']','')
        result2['品类']=list_whole[1]            
    
        print('error:\n',result1.values)
        list1.append(result1)
        list2.append(result2)

    if list1==[]:
        df_result=pd.DataFrame(columns=['数据库名','品类','产品名称','单规格','套装数','总规格','制造商','是否带单位','错误'])
    
    if list2==[]:
        df_wrong=pd.DataFrame(columns=['数据库名','品类','产品名称','单规格','套装数','总规格','制造商','是否带单位','单规格结果','总规格结果','套装数结果'])
    
    
    if (list1!=[])&(list2!=[]):
        df_result=pd.concat(list1).reset_index(drop=True)   
        df_wrong=pd.concat(list2).reset_index(drop=True)  

    df_result=df_result[['数据库名','品类','产品名称','单规格','套装数','总规格','制造商','是否带单位','错误']]
    df_wrong=df_wrong[['数据库名','品类','产品名称','单规格','套装数','总规格','制造商','是否带单位','单规格结果','总规格结果','套装数结果']]
    return df_result







