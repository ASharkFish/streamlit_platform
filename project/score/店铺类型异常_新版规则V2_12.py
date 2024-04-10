from openpyxl import load_workbook
import pandas as pd
import numpy as np
from openpyxl import load_workbook
import json  
import warnings
from fuzzywuzzy import fuzz,process

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common



pd.set_option('display.max_info_columns', 0)
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', None) 

#20230413
#消除如（新希望-白帝）的影响
#20230414
#修复了抛出为空时报错的问题
#20230427
#修改子品牌问题
#20230710
#修改品牌旗舰店和卖场旗舰店判断条件
#新增品牌异常和类型异常判断
#20230712
#修复卖场型旗舰店判断错误的问题
#优化程序运行时间
#20230717
#调整品牌旗舰店判断系数
def mkdir(path):
        folder = os.path.exists(path)
        if not folder:    
                os.makedirs(path)           
        else:
                pass



def sql_whole():
    sql1 = '''select CAST ( 客户品类 AS nvarchar ( 500 ) ),CAST ( 数据库名 AS nvarchar ( 500 ) ),CAST ( 平台名称 AS nvarchar ( 500 ) ),\
            CAST ( 店铺名称 AS nvarchar ( 500 ) ),CAST ( 判断制造商 AS nvarchar ( 500 ) ),CAST ( 判断品牌 AS nvarchar ( 500 ) ),\
            CAST ( 判断子品牌 AS nvarchar ( 500 ) ),CAST ( 店铺类型 AS nvarchar ( 500 ) ),CAST ( 规则 AS nvarchar ( 500 ) ),\
            CAST ( 混合店铺类型 AS nvarchar ( 500 ) ),CAST ( 新店铺类型 AS nvarchar ( 500 ) ),CAST ( 判断月份 AS nvarchar ( 500 ) )  from QC.dbo.店铺名称判断店铺类型异常_对照表'''
    
    sql2 = '''select CAST ( 客户 AS nvarchar ( 2500 ) ),CAST ( 店铺名称 AS nvarchar ( 2500 ) ),CAST ( 店铺类型 AS nvarchar ( 2500 ) ) from QC.dbo.店铺名称判断店铺类型异常_平台自营判断'''

    sql3 = '''select CAST ( 规则名称 AS nvarchar ( 2500 ) ),CAST ( 备注 AS nvarchar ( 2500 ) ) ,CAST ( 规则类型 AS nvarchar ( 2500 ) )from QC.dbo.店铺名称判断店铺类型异常_整体映射规则'''
    
    sql4 = '''select CAST ( 客户 AS nvarchar ( 2500 ) ),CAST ( 平台 AS nvarchar ( 2500 ) ),CAST ( 店铺名称 AS nvarchar ( 2500 ) ),CAST ( 店铺类型 AS nvarchar ( 2500 ) )  from QC.dbo.店铺名称判断店铺类型异常_独立映射规则'''

    sql5=''' select CAST ( 制造商 AS nvarchar ( 2500 ) ),CAST ( 相关制造商 AS nvarchar ( 2500 ) ),CAST ( 品牌 AS nvarchar ( 2500 ) )\
            from QC.dbo.店铺类型_品牌关系'''
    sql6='''select CAST ( 客户品类 AS nvarchar ( 2500 ) ),CAST ( 平台名称 AS nvarchar ( 2500 ) ),CAST ( 包含内容1 AS nvarchar ( 2500 ) ),\
            CAST ( 包含内容2 AS nvarchar ( 2500 ) ),CAST ( 不包含内容 AS nvarchar ( 2500 ) ),CAST ( 店铺类型 AS nvarchar ( 2500 ) ), \
            CAST ( 优先级 AS nvarchar ( 2500 ) )from QC.dbo.店铺名称判断店铺类型异常_桂格shop_info    '''
    print(sql1,'\n',sql2,'\n',sql3,'\n',sql4,'\n',sql5)


    df_compare =sql_connect.select('192.168.0.15','QC','sqlsever',sql1,['客户','数据库名','平台名称','店铺名称','判断制造商','判断品牌','判断子品牌','店铺类型','规则','混合店铺类型','新店铺类型','判断月份'])
    
    df_self_support = sql_connect.select('192.168.0.15','QC','sqlsever',sql2,['客户','店铺名称','店铺类型'])

    df_whole= sql_connect.select('192.168.0.15','QC','sqlsever',sql3,['规则名称','备注','规则类型'])
   
    df_independence = sql_connect.select('192.168.0.15','QC','sqlsever',sql4,['客户名','平台名称','店铺名称','店铺类型'])
    
    df_relationship = sql_connect.select('192.168.0.15','QC','sqlsever',sql5, ['匹配制造商','数据库制造商','匹配品牌'])
    
    df_relationship['数据库制造商']=df_relationship['数据库制造商'].apply(lambda x : x.split(','))

    df_relationship=df_relationship.explode('数据库制造商')
    rule = {}
    for i in range(len(df_whole)):
        rule.update(json.loads(df_whole['备注'][i]))

    df_shop_info=sql_connect.select('192.168.0.15','QC','sqlsever',sql6, ['客户','平台名称','包含内容1','包含内容2','不包含内容','店铺类型','优先级'])
    return df_compare,df_self_support,df_whole,rule,df_independence,df_relationship,df_shop_info


def sql_type():
    sql=f'''
        select CAST ( 客户 AS nvarchar ( 500 ) ),CAST ( 平台名称 AS nvarchar ( 500 ) ),\
        CAST ( 标准店铺类型 AS nvarchar ( 500 ) ),CAST ( 新店铺类型 AS nvarchar ( 500 ) ),\
        CAST ( 混合店铺类型 AS nvarchar ( 500 ) ) from QC.dbo.店铺名称判断判断店铺类型异常_混合店铺类型对照关系
    '''
    column=['客户','平台名称','标准店铺类型','新店铺类型','混合店铺类型']
    df=sql_connect.select('192.168.0.15','QC','sqlsever',sql,column)
    df['标准店铺类型']=df['标准店铺类型'].apply(lambda x : x.split(','))
    return df

def sql_check(kehuname,list_input,time_start,time_end,server,mix_kehu_list1,mix_kehu_list2):
    if kehuname in mix_kehu_list1 : 
        sql=f'''
                select distinct
                SUBSTRING(REPLACE({list_input[-1]}, '-', ''), 0, 7),\
                CAST ( {list_input[2]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[3]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[4]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[5]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[6]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[7]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[9]} AS nvarchar ( 500 ) )
                FROM {list_input[1]} \
                where {list_input[7]} is not null and {list_input[7]} != '海外购' and {list_input[5]} != '未知'\
                and SUBSTRING(REPLACE({list_input[-1]}, '-', ''), 0, 7) between {time_start} and {time_end}
            '''
        print(sql)
        database=list_input[1].split('.')[0].replace('[','').replace(']','')
        columns = ['时间','平台名称','店铺名称','数据库制造商','数据库品牌','数据库子品牌','数据库店铺类型','数据库混合店铺类型']
        df_check=sql_connect.select(server,database,'sqlsever',sql,columns)

    elif kehuname in mix_kehu_list2:
        sql=f'''
                select distinct
                SUBSTRING(REPLACE({list_input[-1]}, '-', ''), 0, 7),\
                CAST ( {list_input[2]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[3]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[4]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[5]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[6]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[7]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[9]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[10]} AS nvarchar ( 500 ) )
                FROM {list_input[1]} \
                where {list_input[7]} is not null and {list_input[7]} != '海外购' and {list_input[5]} != '未知'\
                and SUBSTRING(REPLACE({list_input[-1]}, '-', ''), 0, 7) between {time_start} and {time_end}
            '''
        print(sql)
        database=list_input[1].split('.')[0].replace('[','').replace(']','')
        columns = ['时间','平台名称','店铺名称','数据库制造商','数据库品牌','数据库子品牌','数据库店铺类型','数据库混合店铺类型','数据库新店铺类型']
        df_check=sql_connect.select(server,database,'sqlsever',sql,columns)

    else:
        sql=f'''
                select distinct
                SUBSTRING(REPLACE({list_input[-1]}, '-', ''), 0, 7),\
                CAST ( {list_input[2]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[3]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[4]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[5]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[6]} AS nvarchar ( 500 ) ),\
                CAST ( {list_input[7]} AS nvarchar ( 500 ) )\
                FROM {list_input[1]} \
                where {list_input[7]} is not null and {list_input[7]} != '海外购' and {list_input[5]} != '未知'\
                and SUBSTRING(REPLACE({list_input[-1]}, '-', ''), 0, 7)  between {time_start} and {time_end}
            '''
        print(sql)
        database=list_input[1].split('.')[0].replace('[','').replace(']','')
        columns = ['时间','平台名称','店铺名称','数据库制造商','数据库品牌','数据库子品牌','数据库店铺类型']
        df_check=sql_connect.select(server,database,'sqlsever',sql,columns)
    return df_check


def shop_info_check(row,shop_info_rule,platform_name):
    if platform_name == '苏宁易购':
        input_platform='苏宁'
    elif platform_name == '抖音商城':
        input_platform='抖音'
    else:
        input_platform=platform_name
    shop_name=row['店铺名称']

    mark_list=row['匹配制造商'].unique().tolist()

    brand_list=list(set(','.join(list(row['匹配品牌'].unique())).lower().split(',')))

    include1='、'.join(shop_info_rule['包含内容1'].tolist()).split('、')
    include2='、'.join(shop_info_rule['包含内容2'].tolist()).split('、')
    while '' in include1:
        include1.remove('')
    while '' in include2:
        include2.remove('')


    shopname_compare=shop_name.replace('食品旗舰店','').replace('官方旗舰店','').replace('品牌旗舰店','').replace('旗舰店','').lower()

    if  any(item in shop_name for item in include1) and any(item in shop_name for item in include2) :
        return f'{input_platform}自营海外购'
    
    elif  (platform_name=='抖音商城') and any(item in shop_name for item in include2) and not any(item in shop_name for item in ['山姆代购']):
        return f'抖音海外购'

    elif  any(item in shop_name for item in include2):
        return f'{input_platform}海外购'
    
    elif ((shop_name.endswith('旗舰店'))|(shop_name.endswith('旗舰')))&(any( brand_name in shopname_compare.lower() for brand_name in brand_list))&(len(mark_list)==1):
        return '品牌旗舰店'
        
    elif  any(item in shop_name for item in ['天猫超市']):
        return f'天猫超市'

    elif  any(item in shop_name for item in include1):
        return f'{input_platform}自营'
    
    else:
        return f'{input_platform}POP'






def judement_check(df,rule,df_self_support,independent_rules,kehuname,df_shop_info):

    spicel_shop_info=list(df_shop_info['客户'].unique())
    df_group=df.groupby(['时间','平台名称'],as_index=True)
    result_list=[]

    df_self=df_self_support[(df_self_support['客户'].str.contains(kehuname))|(df_self_support['客户']=='整体')].reset_index(drop=True)
    list_self_support=df_self['店铺名称'].tolist()

    if kehuname in spicel_shop_info:
        for key,group in df_group:

            platform_name=key[1]
            group=group.reset_index(drop=True)
            shop_info_rule=df_shop_info[df_shop_info['平台名称']==platform_name].reset_index(drop=True)

            include1='、'.join(shop_info_rule['包含内容1'].tolist()).split('、')
            include2='、'.join(shop_info_rule['包含内容2'].tolist()).split('、')
            while '' in include1:
                include1.remove('')
            while '' in include2:
                include2.remove('')

            if platform_name == '苏宁易购':
                input_platform='苏宁'
            elif platform_name == '抖音商城':
                input_platform='抖音'
            else:
                input_platform=platform_name

            for i in range(len(group)):
                shop_name=group.loc[i,'店铺名称']

                mark_list=group[group['店铺名称']==shop_name]['匹配制造商'].unique().tolist()

                brand_list=list(set(','.join(list(group[group['店铺名称']==shop_name]['匹配品牌'].unique())).lower().split(',')))

                shopname_compare=shop_name.replace('食品旗舰店','').replace('官方旗舰店','').replace('品牌旗舰店','').replace('旗舰店','').replace(' ','').lower()




                if  any(item in shop_name for item in include1) and any(item in shop_name for item in include2) :
                    group.loc[i,'判断类型']= f'{input_platform}自营海外购'



                elif  any(item in shop_name for item in ['天猫超市']):
                    group.loc[i,'判断类型']= f'天猫超市'

                elif  any(item in shop_name for item in ['抖音超市']):
                    group.loc[i,'判断类型']= f'抖音超市'

                elif  (platform_name=='抖音商城') and any(item in shop_name for item in include2) and not any(item in shop_name for item in ['山姆代购']):
                    group.loc[i,'判断类型']= f'抖音海外购'


 

                elif  any(item in shop_name for item in include2):
                    group.loc[i,'判断类型']= f'{input_platform}海外购'
                




                elif ((shop_name.endswith('旗舰店'))|(shop_name.endswith('旗舰')))&(any( brand_name in shopname_compare.lower() for brand_name in brand_list))&(len(mark_list)==1):
                    group.loc[i,'判断类型']= '品牌旗舰店'
                    




                elif  any(item in shop_name for item in include1):
                    group.loc[i,'判断类型']= f'{input_platform}自营'
                
                elif shop_name in list_self_support:
                    group.loc[i,'判断类型']=df_self[df_self['店铺名称']==shop_name]['店铺类型'].values[0]




                else:
                    group.loc[i,'判断类型']= f'{input_platform}POP'

            result_list.append(group)


    else:

        for key,group in df_group:
            platform_name=key[1]
            group=group.reset_index(drop=True)

            list_flagship = independent_rules[(independent_rules['店铺类型'] == '品牌旗舰店')&(independent_rules['平台名称']==platform_name)]['店铺名称'].tolist()
            list_market = independent_rules[(independent_rules['店铺类型'] == '卖场型旗舰店')&(independent_rules['平台名称']==platform_name)]['店铺名称'].tolist()
            list_exclusive_shop = independent_rules[(independent_rules['店铺类型'] == '专卖店')&(independent_rules['平台名称']==platform_name)]['店铺名称'].tolist()
            list_exclusive = independent_rules[(independent_rules['店铺类型'] == '专营店')&(independent_rules['平台名称']==platform_name)]['店铺名称'].tolist()


            for i in range(len(group)):
                shop_name=group.loc[i,'店铺名称']

                if shop_name in list_self_support:
                    group.loc[i,'判断类型']=df_self[df_self['店铺名称']==shop_name]['店铺类型'].values[0]

                elif shop_name in list_flagship:
                    group.loc[i,'判断类型']=','.join(rule['2'])

                elif shop_name in list_market:
                    group.loc[i,'判断类型']=','.join(rule['3'])

                elif shop_name in list_exclusive_shop:                          
                    group.loc[i,'判断类型']=','.join(rule['4'])

                elif shop_name in list_exclusive:
                    group.loc[i,'判断类型']=','.join(rule['5'])
                else:
                    group.loc[i,'判断类型']='未匹配'
            
            
            group_first=group[group['判断类型']!='未匹配']
            group_first['匹配类型']='独立映射'
            df_second=group[group['判断类型']=='未匹配']

            group_second=df_second.groupby(['店铺名称'],as_index=False)

            if df_second.empty:
                result_list.append(group)
            else:
                second_list=[]
                for key2,group2 in group_second:
                    
                    group2=group2.reset_index(drop=True)
                    shop_name=key2[0]
                    shopname_compare=shop_name.replace('食品旗舰店','').replace('官方旗舰店','').replace('品牌旗舰店','').replace('旗舰店','').replace(' ','').lower()
                    mark_list=group2['匹配制造商'].unique().tolist()
                    brand_list=list(set(','.join(group2['匹配品牌'].unique().tolist()).lower().split(',')))

                    if ((shop_name.endswith('旗舰店'))|(shop_name.endswith('旗舰')))&(any( brand_name in shopname_compare.lower() for brand_name in brand_list))&(len(mark_list)==1):
                        group2['判断类型']=','.join(rule['2'])
                    elif ((shop_name.endswith('旗舰店'))|(shop_name.endswith('旗舰'))):
                        group2['判断类型']=','.join(rule['3'])
                    elif '专卖' in shop_name:
                        group2['判断类型']=','.join(rule['4'])
                    elif '专营' in shop_name:
                        group2['判断类型']=','.join(rule['5'])
                    else:
                        group2['判断类型']=','.join(rule['10'])
                    second_list.append(group2)
                second_output=pd.concat(second_list)
                second_output['匹配类型']='定义规则'
                group_output=pd.concat([group_first,second_output]).drop_duplicates().reset_index(drop=True)
                result_list.append(group_output)
    
    output=pd.concat(result_list).drop_duplicates().reset_index(drop=True)
    return output



def has_common_element(listA, listB):
    for element in listA:
        if element in listB:
            return True
    return False



def store_type(df_input_type,row,kehuname,mix_kehu_list1,mix_kehu_list2):

    mix_store_type=''

    if kehuname in mix_kehu_list1:
        platform_name=row['平台名称']
        determine_type=row['判断类型'].split(',')

        for num in range(len(df_input_type)):
            platform_rule=df_input_type.loc[num,'平台名称']
            standard_type=df_input_type.loc[num,'标准店铺类型']
            mix_type=df_input_type.loc[num,'混合店铺类型']


            if (platform_name==platform_rule)&(has_common_element(determine_type,standard_type)):
                mix_store_type=mix_type
                break

    elif kehuname in mix_kehu_list2:
        platform_name=row['平台名称']
        determine_type=row['判断类型'].split(',')
        shop_type=row['数据库新店铺类型']

        for num in range(len(df_input_type)):
            platform_rule=df_input_type.loc[num,'平台名称']
            standard_type=df_input_type.loc[num,'标准店铺类型']
            mix_type=df_input_type.loc[num,'混合店铺类型']
            new_type=df_input_type.loc[num,'新店铺类型'].split(',')

            if (platform_name==platform_rule)&(shop_type in new_type)&(has_common_element(determine_type,standard_type)):
                mix_store_type=mix_type
                break

    return mix_store_type




def main(kehuname,time_start,time_end):
    df_compare,df_self_support,df_whole,rule_whole,df_independence,df_relationship,df_shop_info=sql_whole()
    df_type=sql_type()
    shop_info_rule=df_shop_info[df_shop_info['平台名称']=='京东']
    include1='、'.join(shop_info_rule['包含内容1'].tolist()).split('、')
    include2='、'.join(shop_info_rule['包含内容2'].tolist()).split('、')

    while '' in include1:
        include1.remove('')
    while '' in include2:
        include2.remove('')

    mix_kehu_list1=list(df_type[df_type['新店铺类型']=='']['客户'].unique())
    mix_kehu_list2=list(df_type[df_type['新店铺类型']!='']['客户'].unique())


    df_input=df_compare[df_compare['客户']==kehuname].reset_index(drop=True)
    independence=df_independence[df_independence['客户名']==kehuname].reset_index(drop=True)

    list_input=df_input.loc[0].tolist()
    rule=rule_whole[list_input[8]]
    if '21库' in kehuname :
        server='192.168.0.21'
    else:
        server='192.168.0.15'
    df_whole=sql_check(kehuname,list_input,time_start,time_end,server,mix_kehu_list1,mix_kehu_list2)

    df_whole=pd.merge(df_whole,df_relationship,how='left',on=['数据库制造商'])

    df_whole['匹配制造商']=df_whole['匹配制造商'].fillna(df_whole['数据库制造商'])
    df_whole['匹配品牌']=df_whole['匹配品牌'].fillna(df_whole['数据库品牌'])

    result=judement_check(df_whole,rule,df_self_support,independence,kehuname,df_shop_info)

    df_input_type=df_type[df_type['客户']==kehuname].reset_index(drop=True)
    if kehuname in ['可口可乐','客户_可口可乐']:
        result['判断类型']=result.apply(lambda row:'其他'if row['平台名称']=='拼多多' else row['判断类型'],axis=1)


    if kehuname in mix_kehu_list1+mix_kehu_list2:
        result['判断混合店铺类型']=result.apply(lambda row: store_type(df_input_type,row,kehuname,mix_kehu_list1,mix_kehu_list2),axis=1)
        result['标准店铺类型是否错误']=result.apply(lambda row: '否' if row['数据库店铺类型'] in row['判断类型'].split(',') else '是',axis=1)
        result['混合店铺类型是否错误']=result.apply(lambda row: '否' if row['数据库混合店铺类型'] in row['判断混合店铺类型'].split(',') else '是',axis=1)
        wrong=result[(result['标准店铺类型是否错误']=='是')|(result['混合店铺类型是否错误']=='是')].reset_index(drop=True)
    else:
        result['标准店铺类型是否错误']=result.apply(lambda row: '否' if row['数据库店铺类型'] in row['判断类型'].split(',') else '是',axis=1)
        wrong=result[result['标准店铺类型是否错误']=='是'].reset_index(drop=True)


    if wrong.empty:
        return wrong
    else:
        grouped_df = wrong.groupby(['时间','平台名称','店铺名称'],as_index=False).agg(lambda x: ','.join(x.astype(str).unique()))
        grouped_df['错误类型']=grouped_df.apply(lambda row: '品牌错误' if ('旗舰'in row['店铺名称'])&(len(row['匹配制造商'].split(','))==1)&(row['数据库店铺类型'] in rule['2']) else '类型错误',axis=1)
        return grouped_df


