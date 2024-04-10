import pandas as pd
from datetime import datetime

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from main import sql_connect



def time_convert(input_time):
    time_obj = datetime.strptime(input_time, "%Y%m")
    output_time = time_obj.strftime("%Y-%m-%d")
    return output_time


def comment_select(time_list):
    df_list=[]
    for time in time_list:
        sql=f'''
            SELECT SUBSTRING
                ( REPLACE( MONTH, '-', '' ), 0, 7 ) AS 月份,
                url_id,
                comment_total_num 
            FROM
                [duishu].[dbo].[etl_sumtotal_all_{time}01] 
            WHERE
                comment_total_num IS NOT NULL '''
        
        column=['时间','url_id','真实评论条数']
        df=sql_connect.select('192.168.0.20','duishu','sqlsever',sql,column)
        df_list.append(df)
    df_output=pd.concat(df_list).reset_index(drop=True)
    return df_output




def sql_select(key_word,time_tm,time_lm):
    tm_day=time_convert(time_tm)
    lm_day=time_convert(time_lm)
    if key_word=='乳品':
        df_list=[]
        db_list=['YILICHUSHU_TEMP','DIWENNAI_TEMP','naiyou_TEMP','ice_TEMP','GTRJ_TEMP','yinliao_TEMP','maipian_TEMP']
        for db_name in db_list:
            sql1=f'''
                        SELECT 
                            SUBSTRING( REPLACE( month, '-', '' ), 0, 7 ) AS 时间, 
                            CAST (CATEGORY_ID AS nvarchar),
                            CAST (BRAND_ID AS nvarchar),
                            CAST (SHOP_ID AS nvarchar),
                            url_id,
                            评论数,
                            销量,
                            平均成交价 
                        FROM
                            [DS_OLSP].[dbo].[{db_name}] 
                        WHERE
                            platform_id = '1' 
                            AND MONTH = '{tm_day}' '''
                    
            column=['时间','品类id','品牌id','店铺id','url_id','评论数','销量','平均成交价']
            df=sql_connect.select('192.168.0.15','DS_OLSP','sqlsever',sql1,column)
            print(sql1)
            df_list.append(df)

        result_dblist=['syntun_ice','syntun_mp','syntun_592','syntun_593','syntun_594','syntun_cmp','syntun_nl',
                       'syntun_ny','syntun_hy','syntun_gtrj','syntun_shuiyin','syntun_dwytn','syntun_cwytn']

        for db_name in result_dblist:
            sql=f'''
                        SELECT 
                            SUBSTRING( REPLACE( month, '-', '' ), 0, 7 ) AS 时间, 
                            CAST (CATEGORY_ID AS nvarchar),
                            CAST (BRAND_ID AS nvarchar),
                            CAST (SHOP_ID AS nvarchar),
                            url_id,
                            评论数,
                            销量,
                            平均成交价 
                        FROM
                            [syntun_item].[dbo].[{db_name}] 
                        WHERE
                            platform_id = '1' 
                            AND MONTH = '{lm_day}' '''
            print(sql)
            column=['时间','品类id','品牌id','店铺id','url_id','评论数','销量','平均成交价']
            df=sql_connect.select('192.168.0.15','DS_OLSP','sqlsever',sql,column)
            df_list.append(df)

        df_output=pd.concat(df_list).reset_index(drop=True)
        return df_output

    elif key_word=='益海嘉里':
        sql=f''' select 
                SUBSTRING( REPLACE( month, '-', '' ), 0, 7 ) ,产线,品牌,shop_code,url_id,volume, CAST (final_price AS float),\
                shop_name,客户店铺类型,下属分类,锁定 from item_temp.dbo.f_analysis_yhjl_京东 \
                where SUBSTRING( REPLACE( month, '-', '' ), 0, 7 ) in ('{time_tm}','{time_lm}') \
                and 客户店铺类型 in ('京东自营','京东分销','京东旗舰店') and  (标识 is null or 标识 <>'脏数据')
            '''
        column=['时间','产线','品牌','店铺id','url_id','销量','平均成交价','店铺名称','店铺类型','下属分类','锁定']
        df=sql_connect.select('192.168.0.15','item_temp','sqlsever',sql,column)

        return df
    
    elif key_word=='轮胎':
        sql1=f'''
                        SELECT 
                        SUBSTRING( REPLACE( month, '-', '' ), 0, 7 ) AS 时间, 
                        CAST (CATEGORY_ID AS nvarchar),
                        CAST (BRAND_ID AS nvarchar),
                        CAST (SHOP_ID AS nvarchar),
                        url_id,
                        销量,
                        平均成交价 
                    FROM
                        syntun_item.dbo.syntun_luntai 
                    WHERE
                        platform_id = '1' 
                        AND MONTH = '{lm_day}' '''
        
        sql2=f'''
                        SELECT 
                        SUBSTRING( REPLACE( month, '-', '' ), 0, 7 ) AS 时间, 
                        CAST (CATEGORY_ID AS nvarchar),
                        CAST (BRAND_ID AS nvarchar),
                        CAST (SHOP_ID AS nvarchar),
                        url_id,
                        销量,
                        平均成交价 
                    FROM
                        [item_temp].[dbo].[LUNTAICHUSHU_{time_tm}] 
                    WHERE
                        platform_id = '1' 
                        AND MONTH = '{tm_day}' '''
        column=['时间','品类id','品牌id','店铺id','url_id','销量','平均成交价']
        df1=sql_connect.select('192.168.0.15','item_temp','sqlsever',sql1,column)
        df2=sql_connect.select('192.168.0.15','item_temp','sqlsever',sql2,column)
        df=pd.concat([df1,df2]).reset_index(drop=True)

        return df



def main (key_word,time_tm,time_lm):

    df_select=sql_select(key_word,time_tm,time_lm)
    df_comment=comment_select([time_tm,time_lm])
    df_test=pd.merge(df_select,df_comment,how='left',on=['时间','url_id']).reset_index(drop=True)
    return df_test


if __name__ =="__main__":
    key_word='乳品'
    time_tm='202403'
    time_lm='202402'
    df_test=main(key_word,time_tm,time_lm)
