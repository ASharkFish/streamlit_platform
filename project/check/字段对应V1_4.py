import pandas as pd
import numpy as np
import datetime
from time import sleep
import warnings

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common

warnings.filterwarnings('ignore')

#20231116
#新增21库检查


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


def main(customer,time_start,time_end):

    python_path,file_total=common.path()

    result_path=f'{file_total}/result'
    
    start  = datetime.datetime.now()
    df_catalogue,data_df=sql_catalogue()

    result_output=[]
    for kehu_name in  customer:
        if '21弦镜' in kehu_name:
            server='192.168.0.21'
        else:
            server='192.168.0.15'
        if data_df[data_df['客户名']==kehu_name].empty:
            print(f'无{kehu_name}对应客户名')
            continue
        list_Account=np.array(data_df[data_df['客户名']==kehu_name].reset_index(drop=True))
        list_name=list_Account[0]

        namelist1=df_catalogue[df_catalogue['客户名']==kehu_name]['依据字段名'].tolist()
        namelist2=df_catalogue[df_catalogue['客户名']==kehu_name]['判断字段名'].tolist()

        list_according=list(set(namelist1+namelist2))
        list_total=np.array(df_catalogue[df_catalogue['客户名']==kehu_name][['制造商字段名','品牌字段名','依据字段名','判断字段名','制造商内容','品牌内容' ,'依据字段内容','判断字段对应内容']].drop_duplicates().reset_index(drop=True))
        df_whole=sql_whole(list_name,list_according,time_start,time_end,server)
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
    
    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H_%M_%S")

    df_output=pd.concat(result_output).drop_duplicates().reset_index(drop=True)
    write=pd.ExcelWriter(f'{result_path}/字段对应{formatted_time}.xlsx')

    df_output.to_excel(write,index=False)
    write.close()
    input('输出完成请去结果文件夹查看结果，按回车键退出')
    end  = datetime.datetime.now()
    print("程序运行时间："+str((end-start).seconds)+"秒")


if __name__ == "__main__":

    received_arguments = sys.argv[1:]
    kehu_list=received_arguments[0].split(',')
    time_begin=received_arguments[1]
    time_end=received_arguments[2]
    main(kehu_list,time_begin,time_end)