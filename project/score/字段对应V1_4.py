import os
import pandas as pd
import numpy as np
from time import sleep

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common





def sql_catalogue():
    sql=f'''select 
            CAST(客户品类 AS nvarchar ( 2000 )),CAST(品类 AS nvarchar ( 2000 )),CAST(数据库名 AS nvarchar ( 2000 )),CAST(时间字段 AS nvarchar ( 2000 )),\
            CAST(制造商字段名 AS nvarchar ( 2000 )),CAST(品牌字段名 AS nvarchar ( 2000 )),CAST(依据字段名 AS nvarchar ( 2000 )),CAST(判断字段名 AS nvarchar ( 2000 )),\
            CAST(制造商内容 AS nvarchar ( 2000 )),CAST(品牌内容 AS nvarchar ( 2000 )),CAST(依据字段内容 AS nvarchar ( 2000 )),CAST(判断字段对应内容 AS nvarchar ( 2000 ))\
            from [QC].[dbo].[客户字段及内容_一一对应] 
        '''
    database='QC'
    column=['客户名','品类','数据库名','时间字段','制造商字段名','品牌字段名' ,'依据字段名','判断字段名' ,'制造商内容','品牌内容' ,'依据字段内容','判断字段对应内容']
    df_catalogue= sql_connect.select('192.168.0.15',database,'sqlsever',sql,column)
    data_df=df_catalogue[['客户名','品类','数据库名','时间字段','制造商字段名','品牌字段名' ,'依据字段名']].drop_duplicates().reset_index(drop=True)
    return df_catalogue,data_df


def sql_whole(list_name,list_according,time1,time2,server):
    sql1=f'''CAST({list_name[4]} AS nvarchar ( 2000 )),CAST({list_name[5]} AS nvarchar ( 2000 ))'''
    sql2=''
    for i in range(0,len(list_according)):
        str_sql=f'CAST ( {list_according[i]}  AS nvarchar  ( 2000 ) ),'
        sql2=sql2+str_sql
    sql=' select '+ sql2 + sql1+ f''' from {list_name[2]} where SUBSTRING( REPLACE( {list_name[3]},'-',''),0,7) between {time1} and {time2} '''
    print(sql)
    database=list_name[2].split('.')[0].replace('[','').replace(']','')
    column=list_according+['制造商','品牌']
    df_whole= sql_connect.select(server,database,'sqlsever',sql,column)
    return df_whole


def main(kehu_name,time1,time2):
    df_catalogue,data_df=sql_catalogue()

    database_list=list(set(df_catalogue[df_catalogue['客户名']==kehu_name]['数据库名'].tolist()))
    if '21' in kehu_name:
        server='192.168.0.21'
    else:
        server='192.168.0.15'

    result_output=[]
    for  database in database_list:
        list_Account=np.array(data_df[data_df['数据库名']==database].reset_index(drop=True))
        list_name=list_Account[0]
        namelist1=df_catalogue[df_catalogue['数据库名']==database]['依据字段名'].tolist()
        namelist2=df_catalogue[df_catalogue['数据库名']==database]['判断字段名'].tolist()
        list_according=list(set(namelist1+namelist2))
        list_total=np.array(df_catalogue[df_catalogue['数据库名']==database][['制造商字段名','品牌字段名','依据字段名','判断字段名','制造商内容','品牌内容' ,'依据字段内容','判断字段对应内容']].drop_duplicates().reset_index(drop=True))
        df_whole=sql_whole(list_name,list_according,time1,time2,server)
        result=[]
        for list1 in list_total:
            name1=list1[2]
            name2=list1[3]
            compare_list=list(set(df_whole[(df_whole['制造商']==list1[4])&(df_whole['品牌']==list1[5])&(df_whole[name1]==list1[6])][name2].tolist()))
            if compare_list==[list1[7]]:
                continue
            elif compare_list==[]:
                continue
            elif compare_list!=[list1[7]]:
                if list1[7] in compare_list:

                    result.append([list1[4],list1[5],name1,list1[6],name2,list1[7],','.join(compare_list),'数量错误'])
                else:
                    result.append([list1[4],list1[5],name1,list1[6],name2,list1[7],','.join(compare_list),'内容错误'])
            
        df_result=pd.DataFrame(result,columns=['制造商','品牌','依据字段名','依据字段内容','字段名','字段对应内容','错误内容','错误类型'])
        df_result['客户名']=list_name[0]
        df_result['品类']=list_name[1]
        df_result['数据库名']=list_name[2]
       
        df_result=df_result[['客户名','品类','数据库名','制造商','品牌','依据字段名','依据字段内容','字段名','字段对应内容','错误内容','错误类型']]
        result_output.append(df_result)
    
    df_output=pd.concat(result_output).drop_duplicates().reset_index(drop=True)

    return df_output

