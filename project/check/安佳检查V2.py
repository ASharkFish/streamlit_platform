from tqdm import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta
from functools import reduce
import pandas as pd 
import numpy as np
import warnings
import os


import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common





warnings.filterwarnings("ignore")



def sql_catalogue ():
    sql='''
    select 	CAST(客户名 AS nvarchar ( 2000 )),CAST(数据库名 AS nvarchar ( 2000 )),CAST(时间 AS nvarchar ( 2000 )),CAST(平台 AS nvarchar ( 2000 )),\
    CAST(制造商 AS nvarchar ( 2000 )),CAST(品类 AS nvarchar ( 2000 )),CAST(子品牌 AS nvarchar ( 2000 )),CAST(产品名称 AS nvarchar ( 2000 )),\
    CAST(店铺名称 AS nvarchar ( 2000 )),CAST(销售额 AS nvarchar ( 2000 )), CAST(升价格	AS nvarchar ( 2000 )),CAST(销量 AS nvarchar ( 2000 )),\
    CAST(URL AS nvarchar ( 2000 )),CAST(SKU AS nvarchar ( 2000 )),CAST(单位	AS nvarchar ( 2000 )),CAST(品类类型	AS nvarchar ( 2000 )),
    CAST(细分市场 AS nvarchar ( 2000 )),CAST(重点制造商 AS nvarchar ( 2000 )),CAST(重点细分市场 AS nvarchar ( 2000 )) from QC.dbo.恒天然月报检查配置表\
    '''    
    print(sql)

    database='QC'
    column=['客户品类','数据库名','时间' ,'平台名称','制造商' ,'品类','子品牌','系列','店铺名称' ,'销售额','升价格','销量','url_id','sku_id' ,'单位','品类类型','细分市场','重点制造商','重点细分市场']
    data_df= sql_connect.select('192.168.0.15',database,'sqlsever',sql,column)
    return data_df


# def sql_series():
#     sql='''select 	CAST(制造商 AS nvarchar ( 2000 )),	CAST(系列 AS nvarchar ( 2000 )),CAST(品类 AS nvarchar ( 2000 )) from QC.dbo.恒天然固定系列
#     '''
#     database='QC'
#     column=['制造商','系列','品类类型']
#     data_df= sql_connect.select('192.168.0.15',database,'sqlsever',sql,column)
#     return data_df


def sql_check(Account_s,key_list,time1,time2):
    listname=Account_s[-3].split(',')

    sql = f"SUBSTRING( REPLACE( {Account_s[2]},'-',''),0,7),\
            CAST ( {Account_s[3]}  AS nvarchar ),\
            CAST ( {Account_s[4]}  AS nvarchar ),\
            CAST ( {Account_s[5]}  AS nvarchar ),\
            CAST ( {Account_s[7]}  AS nvarchar ( 2000 ) ),\
            CAST (  {Account_s[8]}  AS nvarchar ( 2000 ) ),\
            {Account_s[9]},\
            {Account_s[11]} ,\
            {Account_s[12]} ,\
            {Account_s[13]} ,\
            CAST (  {Account_s[6]}  AS nvarchar ( 2000 ) )\
    "
    sql1=''    
    database=Account_s[1].split('.')[0].replace('[','').replace(']','')
    if listname==['']:
        sql1=''
        column=['时间','平台名称','制造商','品类','系列','店铺名称','销售额','销量','url_id','sku_id','子品牌']
    else:
        for i in range(0,len(listname)):
            str_sql=f'CAST ( {listname[i]}  AS nvarchar  ( 2000 ) ),'
            sql1=sql1+str_sql
        column=listname+['时间','平台名称','制造商','品类','系列','店铺名称','销售额','销量','url_id','sku_id','子品牌']
    
    a_sql=f"SUBSTRING( REPLACE({ Account_s[2]},'-',''),0,7)"   

    sql_new=' select '+sql1 + sql +f''' FROM  {Account_s[1]}  where  {a_sql}  between {time1} and  {time2} and  {Account_s[2]} NOT LIKE N'%06-18%' '''

    print(sql_new)
    data_df = sql_connect.select('192.168.0.15',database,'sqlsever',sql_new,column)
    data_df['制造商']=data_df['制造商'].apply(lambda x : x if x in key_list else '其他' )
    op=1
    if Account_s[14]=='万元':
        op=10000
    if Account_s[14]=='千元':
        op = 1000
    if Account_s[14] == '元':
        op=1  

    
    return data_df


def check(df,time_list,key_list,key_word,timeword,op):
    if key_word=='price':
        keyword=['销售额','销量']

    elif key_word=='sale':
        keyword=['销售额']

    elif key_word=='volume':
        keyword=['销量']

    if  timeword=='list':
        df_catalogue=df[key_list].drop_duplicates().reset_index(drop=True)
        name_list=[]
        for time in time_list:
            time_name=get_var_name(time)
            df_input=df[df['时间'].isin(time)][key_list+keyword].groupby(key_list,as_index=False).sum()
            if keyword==['销售额','销量']:
                df_input[time_name]=df_input['销售额']/df_input['销量']*op
            else:
                df_input[time_name]=df_input[keyword[0]]
            df_input=df_input[key_list+[time_name]]
            df_catalogue=pd.merge(df_catalogue,df_input,how='left',on=key_list)
            name_list.append(time_name)
    elif timeword=='word':
        df_input=df[df['时间'].isin(time_list)][['时间']+key_list+keyword].groupby(['时间']+key_list,as_index=False).sum()
        if keyword==['销售额','销量']:
            df_input['price']=df_input['销售额']/df_input['销量']*op
            keyword=['price']
        df_input=df_input[['时间']+key_list+keyword]
        
        df_catalogue=df_input.pivot(index=key_list,columns='时间',values=keyword[0]).rename_axis(columns=None).reset_index()  
        name_list=time_list
    
    
    return df_catalogue




def market(df,listname,time_list,timeword):
    df_list=[]
    error=[]
    for name in listname :
        name_list=[]
        try:
            key_list=['时间','品类',name,'销售额']
            if  timeword=='list':
                df_catalogue=df[['品类',name]].drop_duplicates().reset_index(drop=True)
                for time in time_list:
                    time_name=get_var_name(time)
                    df_input=df[df['时间'].isin(time)][key_list].groupby(['品类',name],as_index=False).sum()
                    df_input[time_name]=df_input['销售额']
                    df_input=df_input[['品类',name]+[time_name]]
                    df_catalogue=pd.merge(df_catalogue,df_input,how='left',on=['品类',name])
                    name_list.append(time_name)

            elif timeword=='word':
                df_input=df[df['时间'].isin(time_list)][key_list].groupby(['时间','品类',name],as_index=False).sum()
                
                df_catalogue=df_input.pivot(index=['品类',name],columns='时间',values='销售额').rename_axis(columns=None).reset_index()  
                name_list=time_list
            df_catalogue['细分市场']=name
            df_catalogue=df_catalogue.rename(columns={name:'内容'})
            df_catalogue=df_catalogue[['品类','细分市场','内容']+name_list]
            df_list.append(df_catalogue)
        except:
            error.append(name)
            continue
    if df_list==[]:
        df_output=pd.DataFrame(columns=['品类','细分市场','内容'])
    else:
        df_output=pd.concat(df_list).reset_index(drop=True)
    return df_output,error



def list_merge(df_list,on,how):
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=on, how=how), df_list)
    return merged_df


def get_var_name(var):
    # 获取调用该函数的上下文中的变量名
    for name, value in globals().items():
        if value is var:
            return name
    return None



def output(time_total,time_whole_list,df_check,list_total,write,key_word,total_word,op,sheet_name=None):
    for key_list in list_total:
        df_list=[]
        for time_list in tqdm(time_total):

            time_word='list'
            if time_list==time_whole_list:
                time_word='word'
            
            df_test=check(df_check,time_list,key_list,key_word,time_word,op)
            df_list.append(df_test)
        df_output=list_merge(df_list,key_list,'left')
        df_output['环比']=df_output[time_whole_list[-1]]/df_output[time_whole_list[-2]]-1
        if sheet_name==None:
            sheetname=''.join(key_list)
        else:
            sheetname=sheet_name
        if total_word=='no':
            wrong=df_output[abs(df_output['环比'])>=0.1].reset_index(drop=True)
            wrong.to_excel(write,sheet_name=sheetname,index=False,float_format="%0.2f")
        elif total_word=='yes':
            df_output.to_excel(write,sheet_name=sheetname,index=False,float_format="%0.2f")


def output_market(time_total,time_whole_list,df_check,segmented_markets_list,sheet_name,total_word,write):
    error_list=[]
    for time_list in tqdm(time_total):
        df_list=[]

        time_word='list'
        if time_list==time_whole_list:
            time_word='word'

        df_test,error=market(df_check,segmented_markets_list,time_list,time_word)
        df_list.append(df_test)
        error_list=error_list+error
    df_output=list_merge(df_list,['品类','细分市场','内容'],'left')
    df_output['环比']=df_output[time_whole_list[-1]]/df_output[time_whole_list[-2]]-1
    error_list=list(set(error_list))
    if error_list!=[]:
        print(f'异常字段{error_list}')
    if total_word=='no':
        wrong=df_output[abs(df_output['环比'])>=0.1].reset_index(drop=True)
        wrong.to_excel(write,sheet_name=sheet_name,index=False,float_format="%0.2f")
    elif total_word=='yes':
        df_output.to_excel(write,sheet_name=sheet_name,index=False,float_format="%0.2f")


def check_membership(list1, list2):
    diff = set(list1) - set(list2)
    return len(diff) != 0




#主程序部分
def main(customer,time):
    start  = datetime.now()

    python_path,file_total=common.path()

    result_path=f'{file_total}/result'


    df_catalogue=sql_catalogue()
    # df_series=sql_series()
    kehu_total=list(df_catalogue['客户品类'].unique())
    if check_membership(customer, kehu_total):
        print('无对应客户名')
        sys.exit

    mat_ty,ytd_ty=common.mat_ytd(time,0)

    mat_ly,ytd_ly=common.mat_ytd(time,1)


    time_whole_list=common.month_time(time,13)


    mat_list=[mat_ty,mat_ly]

    ytd_list=[ytd_ty,ytd_ly]

    list_rule=time_whole_list+mat_ly
    list_rule.sort()
    time_input=list_rule[0]



    for kehu in customer:
        write=pd.ExcelWriter(f'{result_path}/{kehu}月报检查结果.xlsx')
        Account_s=list(df_catalogue[df_catalogue['客户品类']==kehu].values[0])
        op=1
        if Account_s[14]=='万元':
            op=10000
        if Account_s[14]=='千元':
            op = 1000
        if Account_s[14] == '元':
            op=1  

        key_list=Account_s[-2].split('、')
        segmented_markets_list=Account_s[-1].split('、')

        segmented_markets_all=Account_s[-3].split(',')

        df_check=sql_check(Account_s,key_list,time_input,time)


        time_total=[mat_list,ytd_list,time_whole_list]

        list_total=[['平台名称','品类'],['制造商']]
        output(time_total,time_whole_list,df_check,list_total,write,'sale','yes',op)

        list_total=[['品类','制造商']]
        output(time_total,time_whole_list,df_check,list_total,write,'sale','no',op)




        list_total=[['平台名称','制造商'],['平台名称','制造商','店铺名称'],['制造商','子品牌','系列']]
        output(time_total,time_whole_list,df_check,list_total,write,'sale','yes',op)


        output_market(time_total,time_whole_list,df_check,segmented_markets_all,'细分市场','no',write)

        output_market(time_total,time_whole_list,df_check,segmented_markets_list,'细分市场重点','yes',write)


        df_input=df_check[df_check['制造商']=='安佳'].reset_index(drop=True)
        output_market(time_total,time_whole_list,df_input,segmented_markets_list,'细分市场重点_安佳','yes',write)

        list_total=[['平台名称','品类','制造商','系列']]
        df_input=df_check[df_check['平台名称'].isin(['JD','Tmall','Standalone','PDD','Douyin'])].reset_index(drop=True)
        output(time_total,time_whole_list,df_check,list_total,write,'price','no',op,'系列升价格')

        write.close()

    end  = datetime.now()
    print("程序运行时间："+str((end-start).seconds)+"秒")


if __name__ == "__main__":

    received_arguments = sys.argv[1:]
    kehu_list=received_arguments[0].split(',')
    time_start=received_arguments[1]
    time_end=received_arguments[2]
    main(kehu_list,time_start)