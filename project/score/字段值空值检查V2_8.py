#coding:utf-8
import pandas as pd

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common












#20230406
#新增数据库接口，客户选择界面调用数据库
#可以多客户运行，输出多个结果文件
#20230407
#修复输出格式不对应
#20230425
#新增益海嘉里数据库接口
#20230522
#修复15库益海嘉里不能输出的问题
def main(kuhu_name,time_strat,time_end):
    sql = '''
    select CAST ( 客户 AS nvarchar ( 500 ) ),CAST ( 品类 AS nvarchar ( 500 ) ),CAST ( 数据库名 AS nvarchar ( 500 ) ),\
    CAST ( 字段名 AS nvarchar ( 500 ) ),CAST ( 客户品类 AS nvarchar ( 500 ) ),CAST ( 时间 AS nvarchar ( 500 ) ) from [QC].[dbo].[客户字段及内容_空值]
    '''
    print(sql)
    db='QC'
    columns=['客户','品类','数据库名','字段名','客户品类','时间']

    df_new=sql_connect.select('192.168.0.15',db,'sqlsever',sql,columns)

    if '21库' in kuhu_name:
        server='192.168.0.21'
    else:
        server='192.168.0.15'
    df=df_new[df_new['客户品类']==kuhu_name].reset_index(drop=True)
    print(len(df))
    dd_list=[]
    for i in range(0,len(df)):
        a=df.iloc[i].tolist()
        db_name=a[2].split('.')[0].replace('[','').replace(']','')
        sql=f'select distinct cast({a[3]} as nvarchar(1000))   from  {a[2]} where {a[5]} between {time_strat} and {time_end}'
        column=[a[3]]
        df_result=sql_connect.select(server,db_name,'sqlsever',sql,column)
        
        for num in range(len(df_result)):
            jieguo=df_result.loc[num,a[3]]
            p = ''
            if jieguo == "#N/A":
                p = [a[2],  str(a[3]), "列存在#N/A:", jieguo]
            elif jieguo == "0":
                p = [a[2],  str(a[3]), "列存在0:", jieguo]
            elif jieguo == "null":
                p = [a[2],  str(a[3]), "列存在null:", jieguo]
            elif jieguo is None:
                p = [a[2],  str(a[3]), "列存在空值:", jieguo]
            elif jieguo.startswith(' '):
                p = [a[2],  str(a[3]), "列空格开头:", jieguo]
            elif jieguo.endswith(' '):
                p = [a[2],  str(a[3]), "列空格结尾:", jieguo]

            if p != "":
                dd_list.append(p)
            # break

    list1=pd.DataFrame(dd_list)
    return list1