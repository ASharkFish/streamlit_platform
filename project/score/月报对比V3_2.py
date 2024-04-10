# %%
# import _scproxy
import pandas as pd
import numpy as np
import warnings
from openpyxl import load_workbook
warnings.filterwarnings('ignore')
from time import sleep
from tqdm import tqdm


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common










#修复闪退问题，更改为输入客户序号，可多条输入
#20230815
#新增数据库判断模块


# %%
def main(inp_,inp_date):
    sql = 'select CAST ( 客户 AS nvarchar ( 500 ) ),CAST ( 品类 AS nvarchar ( 500 ) ),CAST ( 数据库名 AS nvarchar ( 500 ) ),\
        CAST ( 对比数据库名 AS nvarchar ( 500 ) ),	CAST ( 平台 AS nvarchar ( 500 ) ),	CAST ( 月份 AS nvarchar ( 500 ) ),\
        CAST ( 销售额 AS nvarchar ( 500 ) ),CAST ( 升销量 AS nvarchar ( 500 ) ),CAST ( 对比数据库平台 AS nvarchar ( 500 ) ),\
        CAST ( 对比数据库月份 AS nvarchar ( 500 ) ),CAST ( 对比数据库销售额 AS nvarchar ( 500 ) ),\
        CAST ( 对比数据库升销量 AS nvarchar ( 500 ) )  from [QC].[dbo].历史对比配置'
    columns =['客户','品类','数据库名','对比数据库名','平台','月份','销售额','升销量','对比数据库平台','对比数据库月份','对比数据库销售额','对比数据库升销量']
    QC_DF=sql_connect.select('192.168.0.15','QC','sqlsever',sql,columns)


    columns_dict = {0:'平台名称',1:'品牌',2:'产品名称'}


    use_df_ = QC_DF[QC_DF['客户']==inp_].reset_index(drop=True)
    mer_df = use_df_[['客户','品类','数据库名']]


    pd.set_option('display.float_format',lambda x : '%.2f' % x)


    a_box,b_box,c_box = [pd.DataFrame()],[pd.DataFrame()],[pd.DataFrame()]
    def lg(n,server):
        use_df = use_df_[n:n+1]
        sjk = use_df[['数据库名','平台','月份','销售额','升销量']]
        db_sjk = use_df[['对比数据库名','对比数据库平台','对比数据库月份','对比数据库销售额','对比数据库升销量']]
        sjk_ = pd.concat([sjk[['数据库名','月份','销售额','升销量']],sjk['平台'].str.split(',',expand=True).rename(columns = columns_dict)],axis=1)
        db_sjk_ = pd.concat([db_sjk[['对比数据库名','对比数据库月份','对比数据库销售额','对比数据库升销量']],db_sjk['对比数据库平台'].str.split(',',expand=True).rename(columns = columns_dict)],axis=1)

        sql_li_a = []
        sql_li_b = []
        for i in sjk_.columns:
            sql_li_a.append( sjk_[i].values[0])
        for i in db_sjk_.columns:
            sql_li_b.append(db_sjk_[i].values[0])
        
        try:
            s,a,b,c,d,e,f = sql_li_a
            S,A,B,C,D,E,F = sql_li_b
            if S.endswith("_"):
                S = S+inp_date  
            else:
                S=S
        except:
            try:
                s,a,b,c,d,e = sql_li_a
                S,A,B,C,D,E = sql_li_b
                if S.endswith("_"):
                    S = S+inp_date
                else:
                    S=S            
            except:
                s,a,b,c,d = sql_li_a
                S,A,B,C,D = sql_li_b
                if S.endswith("_"):
                    S = S+inp_date
                else:
                    S=S   

        db_a=s.split('.')[0].replace('[','').replace(']','')

        db_b=S.split('.')[0].replace('[','').replace(']','')
        
        if len(sjk['平台'].str.split(',').values[0]) >= 1:
            a_sql = f"SELECT {a},CAST( {d} AS nvarchar ( 500 ) ),sum({b}),sum({c}) from {s} group by {a},{d}"
            b_sql = f"SELECT {A},CAST( {D} AS nvarchar ( 500 ) ),sum({B}),sum({C}) from {S} group by {A},{D}"




            print(a_sql)
            a_df = sql_connect.select(server,db_a,'sqlsever',a_sql,['月份','平台','新数据库销售额','新数据库升销量'])
            print(b_sql)
            b_df = sql_connect.select(server,db_b,'sqlsever',b_sql,['月份','平台','备份数据库销售额','备份数据库升销量'])
            
            c_df = a_df.merge(b_df,how='outer',on=['月份','平台']).assign(
                数据库名 = s,
                备份数据库名 = S,
                销售额差异 = lambda x: x['新数据库销售额']-x['备份数据库销售额'],
                升销量差异 = lambda x: x['新数据库升销量']-x['备份数据库升销量'],
                是否差异 = lambda x: abs(x['销售额差异']+x['升销量差异']) > 0.0001).drop_duplicates().reset_index(drop=True)
            
            a_box.append(c_df[c_df['是否差异'] == True])
            
        if len(sjk['平台'].str.split(',').values[0]) >= 2:
            a_sql = f"SELECT {a},CAST( {d} AS nvarchar ( 500 ) ),CAST( {e} AS nvarchar ( 500 ) ),sum({b}),sum({c}) from {s} group by {a},{d},{e}"
            b_sql = f"SELECT {A},CAST( {D} AS nvarchar ( 500 ) ),CAST( {E} AS nvarchar ( 500 ) ),sum({B}),sum({C}) from {S} group by {A},{D},{E}"
        
            
            
            
            
            print(a_sql)
            a_df = sql_connect.select(server,db_a,'sqlsever',a_sql,['月份','平台','品牌','新数据库销售额','新数据库升销量'])
            print(b_sql)
            b_df = sql_connect.select(server,db_b,'sqlsever',b_sql,['月份','平台','品牌','备份数据库销售额','备份数据库升销量'])
            c_df = a_df.merge(b_df,how='outer',on=['月份','平台','品牌']).assign(
                数据库名 = s,
                备份数据库名 = S,
                销售额差异 = lambda x: x['新数据库销售额']-x['备份数据库销售额'],
                升销量差异 = lambda x: x['新数据库升销量']-x['备份数据库升销量']
                ,
                是否差异 = lambda x: abs(x['销售额差异']+x['升销量差异']) > 0.0001
                ).drop_duplicates().reset_index(drop=True)
            
            b_box.append(c_df[c_df['是否差异'] == True])
        
        if len(sjk['平台'].str.split(',').values[0]) == 3:
            a_sql = f"SELECT {a},CAST( {d} AS nvarchar ( 500 ) ),CAST( {e} AS nvarchar ( 500 ) ),CAST( {f} AS nvarchar ( 500 ) ),sum({b}),sum({c}) from {s} group by {a},{d},{e},{f}"
            b_sql = f"SELECT {A},CAST( {D} AS nvarchar ( 500 ) ),CAST( {E} AS nvarchar ( 500 ) ),CAST( {F} AS nvarchar ( 500 ) ),sum({B}),sum({C}) from {S} group by {A},{D},{E},{F}"
            print(a_sql)

            a_df = sql_connect.select(server,db_a,'sqlsever',a_sql,['月份','平台','品牌','产品名称','新数据库销售额','新数据库升销量'])
            print(b_sql)

            b_df = sql_connect.select(server,db_b,'sqlsever',b_sql,['月份','平台','品牌','产品名称','备份数据库销售额','备份数据库升销量'])
            c_df = a_df.merge(b_df,how='outer',on=['月份','平台','品牌','产品名称']).assign(
                数据库名 = s,
                备份数据库名 = S,
                销售额差异 = lambda x: x['新数据库销售额']-x['备份数据库销售额'],
                升销量差异 = lambda x: x['新数据库升销量']-x['备份数据库升销量'],
                是否差异 = lambda x: abs(x['销售额差异']+x['升销量差异']) > 0.0001).drop_duplicates().reset_index(drop=True)

            c_box.append(c_df[c_df['是否差异'] == True])


        return a_box,b_box,c_box

    df_list=[]
    for i in tqdm(range(len(use_df_))):
        if '21' in use_df_.loc[i,'客户']:
            server='192.168.0.21'
        else:
            server='192.168.0.15'
        lg(i,server)

        result1=mer_df.merge(pd.concat(a_box,axis=0)[['月份','数据库名','备份数据库名','平台','新数据库销售额','新数据库升销量','备份数据库销售额','备份数据库升销量','销售额差异','升销量差异','是否差异']],how='right', on=['数据库名']).drop_duplicates().reset_index(drop=True)
        result2=mer_df.merge(pd.concat(b_box,axis=0)[['月份','数据库名','备份数据库名','平台','品牌','新数据库销售额','新数据库升销量','备份数据库销售额','备份数据库升销量','销售额差异','升销量差异','是否差异']],how='right', on=['数据库名']).drop_duplicates().reset_index(drop=True)
        try:
            result3=mer_df.merge(pd.concat(c_box,axis=0)[['月份','数据库名','备份数据库名','平台','品牌','产品名称','新数据库销售额','新数据库升销量','备份数据库销售额','备份数据库升销量','销售额差异','升销量差异','是否差异']],how='right', on=['数据库名']).drop_duplicates().reset_index(drop=True)
        except:
            result3=pd.DataFrame(columns=['月份','数据库名','备份数据库名','平台','品牌','产品名称','新数据库销售额','新数据库升销量','备份数据库销售额','备份数据库升销量','销售额差异','升销量差异','是否差异'])
            pass   
        result=pd.concat([result3,result2,result1]).drop_duplicates().reset_index(drop=True)
        df_list.append(result)
    df_result=pd.concat(df_list)
    return df_result


