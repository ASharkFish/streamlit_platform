
import random
from sqlalchemy import create_engine
import warnings
from tqdm import tqdm
import mysql.connector
import pandas as pd
import numpy as np
warnings.filterwarnings("ignore")



def mysql_connect(host='192.168.0.131', user='wgdata', password='syntun-000', database='skyscope',time_input=None,time_input2=None):
    cnx = mysql.connector.connect(host=host, user=user, password=password, database=database)
    sql1=f''' select 
        product_id,product_name ,product_price ,promotion_price,shop_id ,monthly_buy_num,saleInfoStr,rankId,get_date  from wgdata_jingdong.presale_price_list where \
        earnest=-1 and amountDeposit=-1 and expAmount=-1 and saleInfoStr in('618','15日','30日','11.11') and  `get_date` >= '{time_input}' and `get_date` <= '{time_input2}'
    '''
    print(sql1)
    df = pd.read_sql(sql1, cnx)
    return df


def mysql_connect_2(host='192.168.0.131', user='wgdata', password='syntun-000', database='skyscope'):
    cnx = mysql.connector.connect(host=host, user=user, password=password, database=database)
    sql1=f''' select distinct rankId,remask from wgdata_jingdong.presale_rankId_list '''
    print(sql1)
    df = pd.read_sql(sql1, cnx)
    return df


def main(time_input,time_input2):
    df=mysql_connect(host='192.168.0.131', user='shuju ', password='shuju', database='wgdata_jingdong',time_input=time_input,time_input2=time_input2)

    new_columns = ['商品id','商品名称','product_price','promotion_price', '店铺id','销量','销售信息','榜单id','日期']
    
    df.rename(columns=dict(zip(df.columns, new_columns)), inplace=True)



    df_connect=mysql_connect_2(host='192.168.0.131', user='shuju ', password='shuju', database='wgdata_jingdong')

    new_columns = ['榜单id','榜单']
    
    df_connect.rename(columns=dict(zip(df_connect.columns, new_columns)), inplace=True)

    df=pd.merge(df,df_connect,how='left',on=['榜单id'])



    df[['product_price','promotion_price']]=df[['product_price','promotion_price']].astype(float)

    df['价格'] = np.where((df['product_price']!= -1) & (df['promotion_price']!= -1),
                df[['product_price', 'promotion_price']].min(axis=1), df[['product_price', 'promotion_price']].max(axis=1))

    for i in range(len(df)):
        str1=df.loc[i,'销量']
        if str1.find('万')>=0:
            a=float(str1[:-1])*10000
        else:
            a=float(str1)
        str2=df.loc[i,'销售信息']
        if str2 in['15日','11.11']:
            df.loc[i,'销量']=round(a*1.3*random.uniform(1, 1.1))
        else:
            df.loc[i,'销量']=round(a*random.uniform(1, 1.1))


    mapping = {'11.11':0,'30日': 1, '15日': 2}
    df['销售信息映射'] = df['销售信息'].map(mapping).fillna(3)
    # 按照'timestamp'列降序排序

    # dfgrouped = df[['商品id','店铺id','日期','销售信息映射','销量']].sort_values(['销量'], ascending=[False]).groupby(['商品id','店铺id']).head(1).reset_index(drop=True)

    df_result = df.sort_values(['销量','日期','销售信息映射'], ascending=[False,False,True]).groupby(['商品id','店铺id'],as_index=False).head(1).reset_index(drop=True)

    df_result=df_result[ ['商品id','商品名称','价格', '店铺id','销量','销售信息','榜单id','榜单','日期']]
    columns = ['url','商品名称','榜单价格','榜单店铺id', '榜单销量','销售信息','榜单id','榜单','榜单日期']
    
    df_result.rename(columns=dict(zip(df_result.columns, columns)), inplace=True)

    df_result=df_result.drop_duplicates(subset=['url']).reset_index(drop=True)
    return df_result


if __name__ =="__main__":
    time='2024-02-20'
    time2='2024-03-05'

    df_test=main(time,time2)    
    

    write=pd.ExcelWriter('result/榜单销售额.xlsx')
    df_test.to_excel(write,index=False)
    write.close()