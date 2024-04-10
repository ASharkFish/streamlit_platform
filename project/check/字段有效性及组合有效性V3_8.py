from tqdm import tqdm
from time import sleep
import pandas as pd 

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common


#-- coding: utf-8 --
#20230424
#修改数据库接口
#修改有效性判断程序因报错跳过无抛出问题
#修改程序目录
#修改客户选取方式现可多条输入或all
#修改结果显示,无异常将不会生成xlsx文件，并显示无异常，有异常会生曾xlsx文件并显示异常
#更新了输出显示，会显示错误语句，和错误的客户
#20230425
#新增益海嘉里数据库接口
#20230505
#修复输出中文乱码的问题
#20230512
#修复因sql语句错误而导致的未输出结果
#20230515
#修复字段中括号导致sql语句错误的问题
#20230522
#修复15库益海嘉里不能输出的问题
#20230608
#修复因数据库书写不规范(如多空格)造成的程序报错
#20231016
#修复了有效性部分拼接sql语句错误的问题(删除了一个原程序的字段处理部分（空格删除），因为不知道这个处理是什么作用不保证新程序没有问题)
pd.set_option('display.max_rows', None) 


def sql_whole():
    sql1= '''select CAST ( 客户 AS nvarchar ( 2000 ) ),CAST ( 品类 AS nvarchar ( 2000 ) ),CAST ( 数据库名 AS nvarchar ( 2000 ) ),\
    CAST ( 依据字段1 AS nvarchar ( 2000 ) ),CAST ( 依据字段2 AS nvarchar ( 2000 ) ),CAST ( 依据字段1内容 AS nvarchar ( 2000 ) ),\
    CAST ( 依据字段2内容 AS nvarchar ( 2000 ) ),CAST ( 判断字段 AS nvarchar ( 2000 ) ),CAST ( 判断字段内容 AS nvarchar ( 2000 ) ),CAST ( 客户品类 AS nvarchar ( 2000 ) ) from 客户字段及内容_组合有效性'''
    print(sql1)
    database='QC'
    column1 = ['客户','品类','数据库名','依据字段1','依据字段2','依据字段内容1','依据字段内容2','判断字段','判断字段内容','客户品类']
    data1 =sql_connect.select('192.168.0.15',database,'sqlsever',sql1,column1)
    
    sql2 = '''
    select 
    CAST ( 客户 AS nvarchar ( 2000 ) ),CAST ( 品类 AS nvarchar ( 2000 ) ),CAST ( 数据库名 AS nvarchar ( 2000 ) ),\
    CAST ( 字段名 AS nvarchar ( 2000 ) ),CAST ( 字段内容 AS nvarchar ( 2000 ) ),CAST ( 客户品类 AS nvarchar ( 2000 ) ) from 客户字段及内容_有效性'''
    print(sql2)
    column2 = ['客户','品类','数据库名','字段名','字段内容','客户品类']
    data2 = sql_connect.select('192.168.0.15',database,'sqlsever',sql2,column2)
    
    
    sql3 = '''
    select 
    CAST ( 客户 AS nvarchar ( 2000 ) ),CAST ( 品类 AS nvarchar ( 2000 ) ),CAST ( 数据库名 AS nvarchar ( 2000 ) ),\
    CAST ( 判断字段 AS nvarchar ( 2000 ) ),CAST ( 判断字段内容 AS nvarchar ( 2000 ) ),CAST ( 依据字段名 AS nvarchar ( 2000 ) ),\
    CAST ( 依据字段内容 AS nvarchar ( 2000 ) ),CAST ( 客户品类 AS nvarchar ( 2000 ) ) from 分组类判断'''
    print(sql3)
    column3 = ['客户','品类','数据库名','判断字段','判断字段内容','依据字段名','依据字段内容','客户品类']
    data3 = sql_connect.select('192.168.0.15',database,'sqlsever',sql3,column3)

    
    
    return data1,data2,data3




def youxiaoxing(df,server):
    dd_list = []
    db_names = df['数据库名'].unique().tolist()
    for db_name in tqdm(db_names):
        excel_lie_ = df[df['数据库名'] == db_name]['字段名'].unique().tolist()
        excel_lies = []
        error=[]
        for ss in excel_lie_:
            if ss not in excel_lies: 
                excel_lies.append(ss)
        cast_ = [f'cast([{x}] as nvarchar)as [{x}]' for x in excel_lies]
        cast_str = ','.join(cast_)
        select_sql = f'SELECT distinct {cast_str} FROM {db_name}  '
        try:
            db=db_name.split('.')[0].replace('[','').replace(']','')
            sqljieguo= sql_connect.select(server,db,'sqlsever',select_sql,excel_lies)
        except:
            error.append(f'查询语句{select_sql}有异常')
            continue
        # 库内每张表关注的所有字段名列表：
        excel_lie = df[df['数据库名'] == db_name]['字段名'].unique().tolist()
        for h in excel_lie:
            # 每个关注的字段名对应值的列表：b
            excel_zhi = df[(df['数据库名'] == db_name) & (df['字段名'] == h)]['字段内容'].unique().tolist()
            # 判断库内表字段h的值是否存在于b中(两个列表内的元素是否相同？在库列表中而不在对照列表中为错误
            ss = h.replace('[', '').replace(']', '')
            if ss not in sqljieguo:
                continue
            ku_lis = sqljieguo[ss].unique().tolist()

            err_lis1 = [x for x in ku_lis if x not in excel_zhi]
            err_lis2 = [x for x in excel_zhi if x not in ku_lis]

            if err_lis1 != []:
                p = f'在 {db_name} 表中 {h} 列发现异常值{err_lis1}!'
                dd_list.append(p)
            if err_lis2 != []:
                p = f'在 {db_name} 表中 {h} 列发现缺失值{err_lis2}!'
                dd_list.append(p)

 
    list1 = pd.DataFrame(dd_list)
    print("list1= ",list1)
    return list1,error


def zuheyouxiao(df,sever):
    df=df.reset_index(drop=True)
    df_wrong=pd.DataFrame(columns=['数据库名','依据字段1','依据字段2','判断字段','判断字段内容'])
    error=[]
    df_input=df.copy()
    for w in range(len(df_input)):
        a=df_input.loc[w]['数据库名']
        b=df_input.loc[w]['依据字段1']
        c=df_input.loc[w]['依据字段2']
        d=df_input.loc[w]['依据字段内容1']
        e=df_input.loc[w]['依据字段内容2']
        f=df_input.loc[w]['判断字段内容'].split(';&')
        g=df_input.loc[w]['判断字段']
        database=a.split('.')[0].replace('[','').replace(']','')
        sql =f''' select distinct  CAST ( {b} AS nvarchar ( 2000 ) ),CAST ( {c} AS nvarchar ( 2000 ) ),\
            CAST ( {g} AS nvarchar ( 2000 ) ) from {a} '''
        try:
            df2=sql_zuhe(sql,a,database,sever)
            df_linshi=df2[(df2['依据字段内容1']==d)&(df2['依据字段内容2']==e)&(~df2['判断字段数据库内容'].isin(f))].drop_duplicates().reset_index(drop=True)
            df_linshi['判断字段内容']='、'.join(f)
            df_linshi['依据字段1']=b
            df_linshi['依据字段2']=c
            df_linshi['判断字段']=g
            df_linshi=df_linshi[['数据库名','依据字段1','依据字段2','依据字段内容1','依据字段内容2','判断字段','判断字段内容','判断字段数据库内容']]
            df_wrong=pd.concat([df_wrong,df_linshi]).drop_duplicates().reset_index(drop=True)
        except:
            error.append(f'查询语句{sql}有异常')
            continue
    return df_wrong,error
   
    
def mkdir(path):
    folder = os.path.exists(path)
    if not folder:    
        os.makedirs(path)           


def sql_zuhe(sql,a,database,server):
    column = ['依据字段内容1','依据字段内容2','判断字段数据库内容']
    df = sql_connect.select(server,database,'sqlsever',sql,column)
    df['数据库名']=a
    df=df.reindex(columns=['数据库名','依据字段内容1','依据字段内容2','判断字段数据库内容'])
    return df


def sql_group(sql,a,database,server):
    column = ['判断字段数据库内容','依据字段内容']
    df = sql_connect.select(server,database,'sqlsever',sql,column)
    df['数据库名']=a
    df=df.reindex(columns=['数据库名','判断字段数据库内容','依据字段内容'])
    return df




def grouping_judgment(df,sever):
    df=df.reset_index(drop=True)
    df_wrong=pd.DataFrame(columns=['数据库名','判断字段名','依据字段名','判断字段数据库内容','判断字段内容'])
    error=[]
    df_input=df.copy()
    for w in range(len(df_input)):
        a=df_input.loc[w]['数据库名']
        b=df_input.loc[w]['判断字段']
        c=df_input.loc[w]['判断字段内容'].split('、')
        d=df_input.loc[w]['依据字段名']
        e=df_input.loc[w]['依据字段内容']
        database=a.split('.')[0].replace('[','').replace(']','')

        sql =f''' select distinct  CAST ( {b} AS nvarchar ( 2000 ) ),CAST ( {d} AS nvarchar ( 2000 ) ) from {a} '''
        try:
            df2=sql_group(sql,a,database,sever)
            df_linshi=df2[(~df2['判断字段数据库内容'].isin(c))&(df2['依据字段内容']==e)].drop_duplicates().reset_index(drop=True)
            df_linshi['判断字段内容']=','.join(c)
            df_linshi['判断字段名']=b
            df_linshi['依据字段名']=d

            df_linshi=df_linshi[['数据库名','判断字段名','依据字段名','判断字段数据库内容','判断字段内容']]
            df_wrong=pd.concat([df_wrong,df_linshi]).drop_duplicates().reset_index(drop=True)
        except:
            error.append(f'查询语句{sql}有异常')
            continue
    return df_wrong,error
   

def main(customer):
    python_path,file_total=common.path()
    
    result_path=f'{file_total}/result'
    
    df1,df2,df3=sql_whole()

    aaa=[]
    error=[]
    for i in customer:
        if '21库' in i :
            server='192.168.0.21'
        else:
            server='192.168.0.15'

        select_kehu1=df1[df1['客户品类']==i].reset_index(drop=True)
        select_kehu2=df2[df2['客户品类']==i].reset_index(drop=True)
        select_kehu3=df3[df3['客户品类']==i].reset_index(drop=True)

        list1,error1=youxiaoxing(select_kehu2,server) 

        if select_kehu1.empty:
            df_wrong=pd.DataFrame()
            error2=[]
        else:
            df_wrong,error2=zuheyouxiao(select_kehu1,server)

        if select_kehu3.empty:
            df_wrong1=pd.DataFrame()
            error3=[]
        else:
            df_wrong1,error3=grouping_judgment(select_kehu3,server)


        if (list1.empty) and (df_wrong.empty) and(df_wrong1.empty):
            if len(error1)+len(error2)>0:
                string1=','
                aaa.append(f'{i}查询异常未输出结果')
                error.append(string1.join(error1+error2))
                
            else:
                aaa.append(f'{i}无异常')
        else:
            aaa.append(f'{i}文件已输出')
            writer = pd.ExcelWriter(f'{result_path}/{i}字段有效性及字段组合有效性.xlsx')
            list1.to_excel(writer,sheet_name='有效性抛出')
            df_wrong.to_excel(writer,sheet_name='字段组合抛出')
            df_wrong1.to_excel(writer,sheet_name='分组判断')
            writer.close()
    print('\n'.join(error))

    
    
if __name__ == "__main__":

    received_arguments = sys.argv[1:]
    kehu_list=received_arguments[0].split(',')
    time_begin=received_arguments[1]
    time_end=received_arguments[2]
    main(kehu_list)