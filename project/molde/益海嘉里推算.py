import pandas as pd
import numpy as np
import random
from time import sleep
from datetime import datetime, timedelta

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import 榜单销售额
import 数据查询
from main import sql_connect
from main import common

def sql_category():
    sql='''
    select CAST ( category_name  AS nvarchar ),CAST ( second_category_name  AS nvarchar ) from predict.dbo.品类对照 
    '''    
    database='syntun_base'
    column=['品类','二级品类名称']
    df=sql_connect.select('192.168.0.15',database,'sqlsever',sql,column)
    return df

def format_date(year_month):
    year = int(year_month[:4])
    month = int(year_month[4:])
    
    current_month_20 = datetime(year, month, 20).strftime('%Y-%m-%d')
    
    next_month = datetime(year, month, 1) + timedelta(days=32)
    next_month_5 = datetime(next_month.year, next_month.month, 5).strftime('%Y-%m-%d')
    
    return current_month_20, next_month_5


def group(df,namelist):
    df_name=df.copy()
    for i in range(len(namelist)):
        name=namelist[i]
        list=[name,'销量','评论数']
        dfgrouped=df[list].groupby(name,as_index=False).sum().reset_index(drop=True)
        dfgrouped[f'{name}销评比']=dfgrouped['销量']/dfgrouped['评论数']
        df_name=pd.merge(df_name,dfgrouped[[name,f'{name}销评比']],how='left',on=[name])
    
    return df_name



def main(time1,time2):
    python_path,file_total=common.path()

    result_path=f'{file_total}/result'

    time_1,time_2=format_date(time2)
    #数据接口
    df_whole=数据查询.main('益海嘉里',time2,time1)
    df_whole=df_whole.rename(columns={'url_id':'url'})
    df_whole['评论数']=df_whole['真实评论条数']

    #数据接口
    df_list=榜单销售额.main(time_1,time_2)
    df_category=sql_category()

    #训练集
    dftrain=df_whole[df_whole['时间']==time1][['产线','下属分类','品牌','url','销量','评论数','真实评论条数','平均成交价']].drop_duplicates(subset=['url']).reset_index(drop=True)
    dftrain = dftrain[dftrain['销量'].notna() & ((dftrain['销量'] != 0)&(dftrain['评论数'] != 0))]
    #输出3个销评比
    namelist=['url','品牌','下属分类']
    df_train=group(dftrain,namelist)[['下属分类','品牌','url','评论数','真实评论条数','销量','url销评比','下属分类销评比','品牌销评比']]

    df_train['前一月真实评论'] = df_train['真实评论条数']
    df_train['前一月销评比'] = df_train.apply(lambda row: np.nan if (row['前一月真实评论'] == 0) else (row['销量']/row['前一月真实评论']), axis=1)
    df_train=df_train.rename(columns={'评论数':'前一月评论数','销量':'前一月销量','真实评论条数':'前一月真实评论条数'})

    #结果集
    dftest=df_whole[df_whole['时间']==time2].drop_duplicates(subset=['url']).reset_index(drop=True)
    dftest['评论数']=dftest['真实评论条数'].fillna(0)
    #关联外部url销量
    dftest['外部销量']=0

    #关联3个销评比及预测评论数
    dftest=pd.merge(dftest,df_train[['url','url销评比','前一月评论数','前一月销量','前一月真实评论条数','前一月真实评论','前一月销评比']],how='left',on=['url'])
    dftest=pd.merge(dftest,df_train[['品牌','品牌销评比']].drop_duplicates().reset_index(drop=True),how='left',on=['品牌']).drop_duplicates().reset_index(drop=True)
    dftest=pd.merge(dftest,df_train[['下属分类','下属分类销评比']].drop_duplicates().reset_index(drop=True),how='left',on=['下属分类']).drop_duplicates().reset_index(drop=True)
    dftest=dftest.fillna(0)
    #计算最终销评比(按照url到品牌到品类)
    dftest['销评比'] = dftest.apply(lambda row: random.uniform(7,8) if ((row['url销评比']<1.8)|(row['url销评比']==np.inf)) else row['url销评比'] ,axis=1) 
    min_value = dftest['销评比'].loc[dftest['销评比'] > 100].min()
    max_value = dftest['销评比'].loc[dftest['销评比'] > 100].max()

    # 将大于200的数值放缩到100-200的范围内，其余值保持不变
    dftest.loc[dftest['销评比'] > 100, '销评比'] = 50 + ((dftest['销评比'] - min_value) / (max_value - min_value)) * 150

    #计算预测销量
    dftest['预测销量']=dftest['评论数']*dftest['销评比']
    #计算最终销量
    dftest['最终销量']=dftest[['外部销量', '预测销量']].apply(max, axis=1)
    #关联榜单数据和店铺对照数据
    dftest=pd.merge(dftest,df_list,how='left',on=['url'])
    dftest['榜单销量']=dftest['榜单销量'].fillna(0)


    dftest['最终销量']=dftest.apply(lambda row:round(row['榜单销量']) if (row['最终销量']<row['榜单销量'])&(row['榜单销量']!=0) else round(row['最终销量']),axis=1)
    
    
    #计算预测销售额
    dftest['计算销售额']= dftest['最终销量']*dftest['平均成交价']
    #品牌分组求和
    df111=dftest.copy()
    dftest_group=dftest[['产线','品牌','计算销售额']].groupby(['产线','品牌'],as_index=False).sum()
    df222=dftest_group.copy()
    #计算训练部分品牌销售额
    dfinput=dftrain.copy()
    dfinput['训练销售额']= dfinput['销量']*dfinput['平均成交价']
    dftrain_group=dfinput[['产线','品牌','训练销售额']].groupby(['产线','品牌'],as_index=False).sum()  
    #关联品牌id对照和外部品牌环比
    dftest_group['销额环比']=np.nan
    #关联训练品牌销售额
    dftest_group=pd.merge(dftest_group,dftrain_group,how='left',on=['产线','品牌'])
    #填充空值环比
    dftest_group['销额环比']=dftest_group['销额环比'].fillna(1*random.uniform(-0.3, 0.3))
    dftest_group['训练销售额']=dftest_group['训练销售额'].fillna(0)
    #计算预测环比
    dftest_group['计算环比'] = dftest_group.apply(lambda row: np.nan if (row['训练销售额'] == 0) else (row['计算销售额']/row['训练销售额']-1), axis=1)
    #输出最终环比
    dftest_group[['计算环比', '销额环比']]=dftest_group[['计算环比', '销额环比']].astype(float)
    dftest_group['最终环比']=dftest_group[['计算环比', '销额环比']].apply(max, axis=1)
    #计算最终销量
    dftest_group['最终销售额']=dftest_group.apply(lambda row:0 if(row['最终环比'] == np.nan)  else((1+row['最终环比'])*row['训练销售额'] ) ,axis=1)
    #计算倍率系数
    dftest_group['调整系数']=(dftest_group['最终销售额']/dftest_group['计算销售额'])
    #关联系数
    dftest=pd.merge(dftest,dftest_group[['产线','品牌','调整系数']],how='left',on=['产线','品牌'])
    #计算最终调整销量
    dftest['调整销量']= dftest['最终销量']*dftest['调整系数']
    #输出
    df_test=dftest[['时间','下属分类','产线','品牌','店铺id','店铺名称','店铺类型','url','锁定','销评比','url销评比','品牌销评比','下属分类销评比','外部销量', '预测销量','最终销量','调整系数','调整销量','评论数','真实评论条数','前一月销量','前一月评论数','前一月真实评论条数','前一月销评比','平均成交价','榜单价格','榜单销量','销售信息','榜单','榜单日期']].drop_duplicates().reset_index(drop=True)



    write=pd.ExcelWriter(f'{result_path}/模型推算_益海嘉里{time2}.xlsx')
    df_test.to_excel(write,sheet_name='url统计量',index=False)
    dftest_group.to_excel(write,sheet_name='下属分类品牌增速',index=False)
    write.close()


if __name__ =="__main__":
    received_arguments = sys.argv[1:]
    time_begin=received_arguments[0]
    time_end=received_arguments[1]
    main(time_begin,time_end)