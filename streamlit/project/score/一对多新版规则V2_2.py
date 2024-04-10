import pandas as pd 
import numpy as np
import re
import unicodedata
import datetime
import sys
start  = datetime.datetime.now()
from openpyxl.utils.dataframe import dataframe_to_rows


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common








pd.set_option('display.max_rows', None) 
def sql_directory():

    sql = '''select CAST ( 客户 AS nvarchar ( 500 ) ),CAST ( 品类 AS nvarchar ( 500 ) ),\
                CAST ( 客户品类 AS nvarchar ( 500 ) ),CAST ( 数据库名 AS nvarchar ( 500 ) ),\
                CAST ( 字段1 AS nvarchar ( 500 ) ),CAST ( 字段2 AS nvarchar ( 500 ) ),CAST ( 判断 AS nvarchar ( 500 ) ),\
                CAST ( 时间 AS nvarchar ( 500 ) ),CAST ( 类型 AS nvarchar ( 500 ) ) from 客户字段及内容_一对多'''
    print(sql)
    columns = ['客户','品类','客户品类','数据库名','字段1','字段2','判断','时间','类型']
    df_directory=sql_connect.select('192.168.0.15','QC','sqlsever',sql,columns)
    return df_directory


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
    try:
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False


def sku_output(str):
    if str.endswith("版") or str.endswith("款") or str.endswith("装") :
        return str
    else:
      arr=re.split('[*+]',str)
      arr = np.array(arr,dtype=object)
      m=0
      for j in range(len(arr)-1):
          i=j-m
          if is_number(arr[i+1]):
                arr = np.delete(arr, i + 1)
                m = m + 1

      return '+'.join(arr)






def sql_select(name_list,key_word1,key_word2,time_list,db_name,timebegin,timeend):
    if key_word1==key_word2:
        sql1=f'select CAST ({key_word1} AS nvarchar(500))'
    else:
        sql1=f'select CAST ({key_word1} AS nvarchar(500)),CAST ({key_word2} AS nvarchar(500))'

    if time_list==[]:
        sql2=''
        for name in name_list:
            sql_linshi=f', CAST ({name} AS nvarchar(500))'
            sql2=sql2+sql_linshi 
        
        sql3=f'from {db_name}'
    
    else:

        sql2=''
        for name in name_list:
            sql_linshi=f',CAST ({name} AS nvarchar(500))'
            sql2=sql2+sql_linshi 
        
        sql3=f'''from {db_name} where SUBSTRING( REPLACE( {time_list[0]},'-',''),0,7) between {timebegin} and {timeend} '''

    sql=sql1+sql2+sql3
    print(sql)
    dbname=db_name.split('.')[0].replace('[','').replace(']','')

    
    if key_word1==key_word2:

        column=[key_word1]+name_list
        df=sql_connect.select('192.168.0.15',dbname,'sqlsever',sql,column)

        df[key_word1]=df[key_word1].apply(lambda x:sku_output(x))

        counts = df.groupby([key_word1],as_index=False).nunique()


    else:
        column=[key_word1,key_word2]+name_list
        df=sql_connect.select('192.168.0.15',dbname,'sqlsever',sql,column)

        df[key_word1]=df[key_word1].apply(lambda x:sku_output(x))

        df[key_word2]=df[key_word2].apply(lambda x:sku_output(x))

        counts = df.groupby([key_word1,key_word2],as_index=False).nunique()

    return counts


def check_21(df_whole,time1,time2,kehu_name):
    server='192.168.0.21' 
    sql = '''select CAST ( 客户名 AS nvarchar ( 500 ) ),CAST ( 品类 AS nvarchar ( 500 ) ),CAST ( 数据库名 AS nvarchar ( 500 ) ),\
        CAST ( 字段1 AS nvarchar ( 500 ) ),CAST ( 字段2 AS nvarchar ( 500 ) ),CAST ( 判断 AS nvarchar ( 500 ) ),\
        CAST ( 字段1内容 AS nvarchar ( 4000 ) ),CAST ( 字段2内容 AS nvarchar ( 4000 ) ),CAST ( 判断数量 AS nvarchar ( 500 ) )from 客户字段及内容_一对多白名单 '''

    print(sql)

    columns2 = ['客户品类','品类','数据库名','字段1','字段2','判断','字段1内容','字段2内容','判断数量']


    df_rule=sql_connect.select('192.168.0.15','QC','sqlsever',sql,columns2)
    df_rule['分组']='特殊'
    df_rule=df_rule[['字段1','字段2','判断','字段1内容','字段2内容','判断数量','分组']]
    df_input=df_whole[df_whole['客户品类']==kehu_name].reset_index(drop=True)


    df_list=[]
    for num1 in range(len(df_input)):
        name_list=df_input.iloc[num1].tolist()
        sql=f'''
                select SUBSTRING( REPLACE({name_list[-1]},'-',''),0,7),CAST ( {name_list[4]} AS nvarchar ( 500 ) ),\
                CAST ( {name_list[5]} AS nvarchar ( 500 ) ),count(distinct {name_list[6]})\
                from {name_list[3]} group by  {name_list[4]},{name_list[5]},{name_list[-1]} having count(distinct {name_list[6]})>1 \
                and SUBSTRING( REPLACE({name_list[-1]},'-',''),0,7) between {time1} and {time2}

        '''
        print(sql)
        columns = ['时间','字段1内容','字段2内容','数据库数量']
        db=name_list[2].split('.')[0].replace('[','').replace(']','')
        df_result=sql_connect.select(server,db,'sqlsever',sql,columns)

        if df_result.empty:
            df_list.append(df_result)
        else:
            df_result['字段1']=name_list[3]
            df_result['字段2']=name_list[4]
            df_result['判断']=name_list[5]
            df_result['客户名']=name_list[0]
            df_result['品类']=name_list[1]
            df_result['数据库名']=name_list[2]
            df_list.append(df_result)
    try:
        result=pd.concat(df_list).reset_index(drop=True)
        df_result=pd.merge(result,df_rule,how='left',on=['字段1','字段2','判断','字段1内容','字段2内容']).reset_index(drop=True)

        df_result[['数据库数量','判断数量']]=df_result[['数据库数量','判断数量']].astype(float)


        wrong=df_result[((df_result['分组']=='特殊')&(df_result['数据库数量']!=df_result['判断数量']))|(df_result['分组']!='特殊')].reset_index(drop=True)
        wrong=wrong[['时间','客户名','品类','数据库名','字段1','字段2','判断','字段1内容','字段2内容','数据库数量','判断数量','分组']].drop_duplicates().reset_index(drop=True)
    
    except:
        wrong=pd.DataFrame()
    return wrong




def main(kehu_name,time_begin,time_end):
    df_directory=sql_directory()

    if '弦镜' in kehu_name:
        dfresult=check_21(df_directory,time_begin,time_end,kehu_name)
    else:
        rule_total=df_directory[df_directory['客户品类']==kehu_name].reset_index(drop=True)
        rule_group=rule_total.groupby(['数据库名','字段1','字段2','类型'],as_index=False)
        df_list=[]
        key_list=[]
        filtered_list=[]
        for key,group in rule_group:
            name_list=group['判断'].values.tolist()
            time_list=list(group['时间'].unique())
            db_name=key[0]
            key_word1=key[1]
            key_word2=key[2]

            try:      
                df_output=sql_select(name_list,key_word1,key_word2,time_list,db_name,time_begin,time_end)
            except:
                df_output=pd.DataFrame()
                print(f'数据库{db_name},字段1{key_word1},字段2{key_word2}读取失败')


            if df_output.empty:
                continue
            else:
                filtered_columns = df_output[name_list].columns[df_output[name_list].gt(1).any()]
                filtered_df = df_output[df_output[filtered_columns] > 1].any(axis=1)
                if key_word1==key_word2:
                    df_result = df_output[filtered_df][[key_word1]+list(filtered_columns)].reset_index(drop=True)
                    key_list=key_list+[key_word1]
                    filtered_list=filtered_list+list(filtered_columns)
                else:
                    df_result = df_output[filtered_df][[key_word1,key_word2]+list(filtered_columns)].reset_index(drop=True)
                    key_list=key_list+[key_word1,key_word2]
                    filtered_list=filtered_list+list(filtered_columns)
            
            df_result['类型']=key[3]
            
            df_list.append(df_result)
            
        dfresult=pd.concat(df_list).reset_index(drop=True)
        dfresult = dfresult.loc[:, ~dfresult.columns.duplicated()]

        move_to_front = '类型'

        # 将指定列移动到最前面
        cols = dfresult.columns.tolist()
        cols.insert(0, cols.pop(cols.index(move_to_front)))
        dfresult = dfresult.reindex(columns=cols)

    return dfresult
