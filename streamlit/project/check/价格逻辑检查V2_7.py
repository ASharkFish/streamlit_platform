from tqdm import tqdm
from time import sleep
import pandas as pd 
import numpy as np
from time import sleep
import warnings

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common

warnings.filterwarnings("ignore")



def sqlcatalogue ():
    sql='''
    select 	CAST(客户名 AS nvarchar ( 2000 )),CAST(数据库名 AS nvarchar ( 2000 )),CAST(时间 AS nvarchar ( 2000 )),\
    CAST(销售额 AS nvarchar ( 2000 )), CAST(升价格	AS nvarchar ( 2000 )),CAST(销量 AS nvarchar ( 2000 )),\
    CAST(URL AS nvarchar ( 2000 )),CAST(SKU AS nvarchar ( 2000 )),CAST(单位	AS nvarchar ( 2000 )),\
    CAST(总规格	AS nvarchar ( 2000 )),CAST(升销量 AS nvarchar ( 2000 )),CAST(平台 AS nvarchar ( 2000 )),\
    CAST(逻辑检查 AS nvarchar ( 2000 )),CAST(升销量计算逻辑 AS nvarchar ( 2000 )) from QC.dbo.月报检查程序配置表\
    '''    

    database='QC'
    column=['客户品类','数据库名','时间','销售额','升价格','销量','url_id','sku_id' ,'单位','总规格','升销量','平台','逻辑检查','升销量计算逻辑']
    data_df= sql_connect.select('192.168.0.15',database,'sqlsever',sql,column)
    data_df['逻辑检查']=data_df['逻辑检查'].fillna('')

    return data_df




def sql_whole(Account_s,time1,time2,server):
    count_logic=Account_s[13]
    if Account_s[12]!='':
        name_list=Account_s[12].split('、')
    else:
        name_list=[]
    print(name_list)
    sql_list=[]
    if name_list==[]:
        sql_list_input=''
    else:
        for name in name_list:
            sql_list.append(f',CAST ( {name} AS float )')
        sql_list_input=''.join(sql_list)


    if server=='192.168.0.15':
        sql1=f'''select SUBSTRING( REPLACE( {Account_s[2]},'-',''),0,7),\
                CAST ( {Account_s[3]}  AS float ),\
                CAST ( {Account_s[5]}  AS float ),\
                CAST ( {Account_s[6]}  AS nvarchar ( 2000 ) ),\
                CAST ( {Account_s[7]}  AS nvarchar ( 2000 ) ),\
                CAST ( {Account_s[9]}  AS float ),\
                CAST ( {Account_s[11]}  AS nvarchar ( 2000 ) )
                '''
        
        sql2=f''',CAST ( {Account_s[10]}  AS float) from {Account_s[1]}  where SUBSTRING( REPLACE( {Account_s[2]},'-',''),0,7) between {time1} and {time2}'''

        sql=sql1+sql_list_input+sql2
        print(sql)
        database=Account_s[1].split('.')[0].replace('[','').replace(']','')
        column=['时间','销售额','销量','url_id','sku_id','总规格','平台名称']+name_list+['升销量']
        data_df=sql_connect.select(server,database,'sqlsever',sql,column)
        
        data_df['销售额']=data_df['销售额'].fillna(0)
        data_df['销售额']=data_df['销售额'].replace('null',0)

        data_df['销量']=data_df['销量'].fillna(0)
        data_df['销量']=data_df['销量'].replace('null',0)


        if count_logic=='伊利奶酪':
            data_df['计算升销量']=data_df['销量']*data_df['单口味个数']*data_df['内包装单位规格']*data_df['内包装支数']/1000
        elif count_logic=='伊利冰品':
            data_df['计算升销量']=data_df['销量']*data_df['单个个数']*data_df['单包装规格']/1000
        elif count_logic=='益海嘉里':
            data_df['计算升销量']=data_df['销量']*data_df['套装数']*data_df['单规格']
        else:
            data_df['计算升销量']=data_df['销量']*data_df['总规格']/1000



    else:
        sql1=f'''select distinct\
                CAST ( {Account_s[3]}  AS float ),CAST ( {Account_s[11]}  AS nvarchar ( 2000 ) )'''
                
        sql2=f'''    
                ,CAST ( {Account_s[5]}  AS float )\
                from {Account_s[1]}  where SUBSTRING( REPLACE( {Account_s[2]},'-',''),0,7) between {time1} and {time2}
        '''
        sql=sql1+sql_list_input+sql2
        print(sql)
        database=Account_s[1].split('.')[0].replace('[','').replace(']','')
        column=['销售额','平台名称']+name_list+['销量']
        data_df=sql_connect.select(server,database,'sqlsever',sql1,column)
    
        data_df['销售额']=data_df['销售额'].fillna(0)
        data_df['销售额']=data_df['销售额'].replace('null',0)

        data_df['销量']=data_df['销量'].fillna(0)
        data_df['销量']=data_df['销量'].replace('null',0)

    op=1
    if Account_s[8]=='万元':
        op=10000
    if Account_s[8]=='千元':
        op = 1000
    if Account_s[8] == '元':
        op=1  
    data_df['销售额']=data_df['销售额']*op
    data_df['升销量'] = data_df['升销量'].round(5)
    data_df['计算升销量'] = data_df['计算升销量'].round(5)

    return data_df

def main(customer,time1,time2):
    python_path,file_total=common.path()

    result_path=f'{file_total}/result'

    df_catalogue=sqlcatalogue()

    output_list=[]

    for kehu_name in tqdm(customer):

        list_Account=np.array(df_catalogue[df_catalogue['客户品类']==kehu_name].reset_index(drop=True))
        Account_s=list_Account[0]
        if '21库' in Account_s[0] :
            sever='192.168.0.21'
            df_input=sql_whole(Account_s,time1,time2,sever)
            df_input['销量存在小数']=df_input.apply(lambda row: '无异常' if row['销量'].is_integer() else '有异常' ,axis=1 )
            df_input['销量销售额间不合逻辑']=df_input.apply(lambda row: '无异常' if ((row['销量']==0)&(row['销售额']==0))|((row['销量']!=0)&(row['销售额']!=0))  else '有异常' ,axis=1  )
            df_outout=df_input[(df_input['销量存在小数']=='有异常')|(df_input['销量销售额间不合逻辑']=='有异常')].drop_duplicates().reset_index(drop=True)
            df_outout['客户品类']=kehu_name
            df_outout=df_outout[['平台名称','客户品类','销售额','销量','销量存在小数','销量销售额间不合逻辑']]
            output_list.append(df_outout)

        else:
            sever='192.168.0.15'
            df_input=sql_whole(Account_s,time1,time2,sever)
            df_input['销量存在小数']=df_input.apply(lambda row: '无异常' if row['销量'].is_integer() else '有异常' ,axis=1 )
            df_input['销量销售额间不合逻辑']=df_input.apply(lambda row: '无异常' if ((row['销量']==0)&(row['销售额']==0))|((row['销量']!=0)&(row['销售额']!=0))  else '有异常' ,axis=1  )
            df_input['升销量计算错误']=df_input.apply(lambda row: '有异常' if ((row['计算升销量'])!=(row['升销量'])) else '无异常' ,axis=1  )
            df_outout=df_input[(df_input['销量存在小数']=='有异常')|(df_input['销量销售额间不合逻辑']=='有异常')|(df_input['升销量计算错误']=='有异常')].drop_duplicates().reset_index(drop=True)
            df_outout['客户品类']=kehu_name
            df_outout=df_outout[['平台名称','客户品类','时间','销售额','销量','计算升销量','升销量','url_id','sku_id','销量存在小数','销量销售额间不合逻辑','升销量计算错误']]
            output_list.append(df_outout)

    df_output=pd.concat(output_list)
    write=pd.ExcelWriter(f'{result_path}/价格逻辑性检查.xlsx')
    df_output.to_excel(write,index=False)
    write.close()



if __name__ == "__main__":

    received_arguments = sys.argv[1:]
    kehu_list=received_arguments[0].split(',')
    time_begin=received_arguments[1]
    time_end=received_arguments[2]
    main(kehu_list,time_begin,time_end)