import pandas as pd   
import numpy as np
import re
import datetime
from fuzzywuzzy import fuzz,process
import warnings



import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common


warnings.filterwarnings("ignore")
def sql_rule(kehu_name,key_word):
    sql=f'''select distinct CAST(判断字段 AS nvarchar ( 2000 )) from   QC.dbo.产品名称细分市场规则表_{key_word} where 客户品类='{kehu_name}' '''
    column=['判断字段']
    df=sql_connect.select('192.168.0.15','QC','sqlsever',sql,column)
    df['判断字段']=df['判断字段'].apply(lambda x: x.split('、'))
 
    max_length = df['判断字段'].apply(len).max()
    
    if np.isnan(max_length):
        df_rule=pd.DataFrame()
        return df_rule

    sql_output=''
    column=['客户品类','数据库名','产品名称','月份','包含内容1','判断字段']
    for num in range(max_length):
        name=f'字段内容{num+1}'
        sql_linshi=f',CAST({name} AS nvarchar ( 2000 ))'
        sql_output=sql_output+sql_linshi
        column.append(name)


    sql=f''' select  CAST( 客户品类 AS nvarchar ( 2000 )),CAST( 数据库名 AS nvarchar ( 2000 )),CAST( 产品名称 AS nvarchar ( 2000 )),
            CAST( 月份 AS nvarchar ( 2000 )),CAST( 包含内容1 AS nvarchar ( 2000 )),CAST( 判断字段 AS nvarchar ( 2000 ))
        '''
    
    sql=sql+sql_output+f''' from QC.dbo.产品名称细分市场规则表_{key_word} where 客户品类='{kehu_name}' '''

    df_rule=sql_connect.select('192.168.0.15','QC','sqlsever',sql,column)

    return df_rule


def sql_check(db_name,rule,field_list,time1,time2):
    rule_linshi=rule[['月份','产品名称']].drop_duplicates().reset_index(drop=True)
    rule_list=rule_linshi.loc[0].to_list()
    sql1=f'''select SUBSTRING( REPLACE( {rule_list[0]},'-',''),0,7),CAST( {rule_list[1]} AS nvarchar ( 2000 )) '''
    sql2=''
    for field in field_list:
        sql_linshi=f',CAST( {field} AS nvarchar ( 2000 ))'
        sql2=sql2+sql_linshi
    sql3= f''' from {db_name}  where SUBSTRING( REPLACE( {rule_list[0]},'-',''),0,7) between {time1} and {time2} '''
    sql=sql1+sql2+sql3
    db=db_name.split('.')[0].replace('[','').replace(']','')
    column=['时间','产品名称']+field_list
    print(sql)
    df=sql_connect.select('192.168.0.15',db,'sqlsever',sql,column)
    return df 


def fn_replace(s):
    name_new = re.sub("[*][0-9]{1,3}", "", s)
    return name_new

def Series_judgment(df,field_list):
    a=[]
    for i in range(0,len(df)):
        x=df.loc[i,'产品名称']
        if x.count('ml') >= 2 or x.count('g') >= 2 or (x.count('g') > 0 and x.count('ml') > 0 ):
            a.append(x) 
        
        elif (len(re.findall(r'ml[+][\u4e00-\u9fa5]',x)) == 0) \
                and (len(re.findall(r'g[+][\u4e00-\u9fa5]', x)) == 0) \
                and (len(re.findall(r'\*[0-9]+\+[\u4e00-\u9fa5]', x)) == 0) \
                and (len(re.findall(r'\*[0-9]+\+[a-zA-Z]',x)) == 0) \
                and (len(re.findall(r'ml[+][a-zA-Z]', x)) == 0) \
                and (len(re.findall(r'g[+][a-zA-Z]', x)) == 0) \
                and (len(re.findall(r'[\u4e00-\u9fa5][+][a-zA-Z]', x)) == 0)\
                and (len(re.findall(r'\*[0-9]+\+[0-9]', x)) == 0)\
                and (len(re.findall(r'礼盒装[+][\u4e00-\u9fa5]', x)) == 0)\
                and (len(re.findall(r'半月装[+][\u4e00-\u9fa5]', x)) == 0)\
                and (len(re.findall(r'星期装[+][\u4e00-\u9fa5]', x)) == 0)\
                and (len(re.findall(r'q宠版[+][\u4e00-\u9fa5]', x)) == 0)\
                and (len(re.findall(r'版[+][\u4e00-\u9fa5]', x)) == 0)\
                and (len(re.findall(r'款[+][\u4e00-\u9fa5]', x)) == 0)\
                and (len(re.findall(r'盒[+][\u4e00-\u9fa5]', x)) == 0)\
                and (len(re.findall(r'罐[+][\u4e00-\u9fa5]', x)) == 0)\
                and (len(re.findall(r'瓶[+][\u4e00-\u9fa5]', x)) == 0)\
                and (len(re.findall(r'瓶[+][\u4e00-\u9fa5]', x)) == 0):
            a.append(x.replace('+', ''))
        else:
            a.append(x) 
    df.loc[:,'判断']=a        
    b=[]
    for m in range(0,len(df)):
        x=fuzz.partial_ratio('+',df.loc[m,'判断'])
        if x > 0 :
           b.append('是')
        else:
            b.append('否')
    df.loc[:,'是否为组合装']=b

    df1=df[df['是否为组合装']=='是'].reset_index(drop=True)
    df2=df[df['是否为组合装']=='否'].reset_index(drop=True)
    listname=df2['产品名称'].tolist()
    xilie1=[]
    for mm in range(0,len(listname)):
        xilie1.append(fn_replace(listname[mm]))
    df2['判断系列']=xilie1
    b=[]
    for i in range(0,len(df1)):
        list1=df1.loc[i,'判断'].split(' ') 
        a=[]
        for m in range(0,len(list1)):
            string1=list1[m]
            if  (len(re.findall(r'ml[+][\u4e00-\u9fa5]',string1)) == 0) \
                and (len(re.findall(r'g[+][\u4e00-\u9fa5]', string1)) == 0) \
                and (len(re.findall(r'\*[0-9]+\+[\u4e00-\u9fa5]', string1  )) == 0) \
                and (len(re.findall(r'\*[0-9]+\+[a-zA-Z]',string1)) == 0) \
                and (len(re.findall(r'ml[+][a-zA-Z]', string1)) == 0) \
                and (len(re.findall(r'g[+][a-zA-Z]', string1)) == 0) \
                and (len(re.findall(r'[\u4e00-\u9fa5][+][a-zA-Z]', string1)) == 0)\
                and (len(re.findall(r'\*[0-9]+\+[0-9]', string1)) == 0)\
                and(len(re.findall(r'[\u4e00-\u9fa5][+][0-9]', string1)) == 0)\
                and (len(re.findall(r'[\u4e00-\u9fa5][+][a-zA-Z]', string1)) == 0)\
                and (len(re.findall(r'[a-zA-Z][+][\u4e00-\u9fa5]', string1)) == 0)\
                and (len(re.findall(r'半月装[+][\u4e00-\u9fa5]', string1))==0)\
                and (len(re.findall(r'星期装[+][\u4e00-\u9fa5]', string1))==0)\
                and (len(re.findall(r'礼盒装[+][\u4e00-\u9fa5]', string1))==0)\
                and (len(re.findall(r'版[+][\u4e00-\u9fa5]', string1)) == 0)\
                and (len(re.findall(r'款[+][\u4e00-\u9fa5]', string1)) == 0)\
                and (len(re.findall(r'盒[+][\u4e00-\u9fa5]', string1)) == 0)\
                and (len(re.findall(r'罐[+][\u4e00-\u9fa5]', string1)) == 0)\
                and (len(re.findall(r'瓶[+][\u4e00-\u9fa5]', string1)) == 0)\
                and (len(re.findall(r'瓶[+][\u4e00-\u9fa5]', string1)) == 0)\
                and (len(re.findall(r'q宠版[+][\u4e00-\u9fa5]', string1)) == 0):
                a.append(string1)
            else:
                a.append(string1.replace('+','+++'))
        str1=' '
        b.append(str1.join(a)) 
    df1['组合装拆分辅助判断']=b
    list2=[]
    for x in range(0,len(b)):
        list2.append(b[x].split('+++')[0])
    df1['产品名称辅助']=list2
    xilie=[]
    for m in range(0,len(list2)):
        xilie.append(fn_replace(list2[m]))
    df1['判断系列']=xilie
    result = pd.concat([df1,df2])[['时间','产品名称','判断系列']+field_list].reset_index(drop=True)
    return result




def compare(df,columns_list1,columns_list2):
    boolean_index = pd.Series([False] * len(df))
    
    different_values = []

    for index, row in df.iterrows():
        for col1, col2 in zip(columns_list1, columns_list2):
            if row[col1] != row[col2]:
                different_values.append((index, col1, col2, row[col1], row[col2]))
                boolean_index[index] = True


    different_rows = df[boolean_index]

    different_rows.reset_index(inplace=True)
    different_rows.rename(columns={'index': 'Row_Index'}, inplace=True)


    different_values_df = pd.DataFrame(different_values, columns=['Row_Index', '错误字段', 'Column2', '数据库内容', '判断内容'])


    different_values_df['错误字段'] = different_values_df['错误字段'].astype(str)
    different_values_df['数据库内容'] = different_values_df['数据库内容'].astype(str)
    different_values_df['判断内容'] = different_values_df['判断内容'].astype(str)

    df_test=different_values_df[['Row_Index','错误字段','数据库内容','判断内容']].groupby('Row_Index').agg({'错误字段': lambda x: ', '.join(x),
                                                                                                        '数据库内容': lambda x: ', '.join(x),
                                                                                                        '判断内容': lambda x: ', '.join(x)}).reset_index()
    test=pd.merge(different_rows,df_test,on=['Row_Index'],how='left')
    return test




def main(kehu,time_start,time_end):




    wrong1_list=[]
    wrong2_list=[]

    for key_word in ['非组合装','组合装']:

        df_rule=sql_rule(kehu,key_word)
        if df_rule.empty:
            continue



        kehu_rule=df_rule[df_rule['客户品类']==kehu].reset_index(drop=True)
        db_name=kehu_rule['数据库名'].values[0]

    
        rule=kehu_rule[kehu_rule['数据库名']==db_name][['数据库名','产品名称','月份','包含内容1','判断字段']].drop_duplicates().reset_index(drop=True)
        name_list=rule['包含内容1'].unique()
        field_list=rule.loc[0,'判断字段'].split('、')
        check_name=[]
        field_db=[]
        for num in range(len(field_list)):
            check_name.append(f'字段内容{num+1}')
            field_db.append(f'判断{field_list[num]}')



        df_whole=sql_check(db_name,rule,field_list,time_start,time_end)
        if key_word=='组合装':
            df_whole['判断系列']=df_whole['产品名称'].apply(lambda x : fn_replace(x) )
            df_test=df_whole.copy()

        elif key_word=='非组合装':
            df_test=Series_judgment(df_whole,field_list)


        df_merge=kehu_rule[kehu_rule['数据库名']==db_name]
        df_merge=df_merge.rename(columns=dict(zip(check_name, field_db)))[['包含内容1']+field_db]
        df_merge=df_merge.rename(columns={'包含内容1':'判断系列'})
        df_merge['关联']=1

        df_result2=pd.merge(df_test,df_merge,on=['判断系列'],how='left').reset_index(drop=True)






        result=compare(df_result2,field_list,field_db)

        result = result.drop('Row_Index', axis=1)




        wrong1=result[result['关联'].isnull()]            
        wrong2=result[~result['关联'].isnull()]
        wrong1_list.append(wrong1)  
        wrong2_list.append(wrong2) 
    try:
        result_wrong1=pd.concat(wrong1_list)
        cols_to_move = ['错误字段','数据库内容','判断内容']

        result_wrong1 = result_wrong1.reindex(columns=[col for col in result_wrong1.columns if col not in cols_to_move] + cols_to_move)
    except:
        result_wrong1=pd.DataFrame()


    try:
        result_wrong2=pd.concat(wrong2_list)

        cols_to_move = ['错误字段','数据库内容','判断内容']

        result_wrong2 = result_wrong2.reindex(columns=[col for col in result_wrong2.columns if col not in cols_to_move] + cols_to_move)
        
    except:
        result_wrong2=pd.DataFrame()

    return result_wrong2
