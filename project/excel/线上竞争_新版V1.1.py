import pandas as pd 
import numpy as np
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from tqdm import tqdm
from time import sleep
import datetime

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from main import sql_connect
from main import common


def sql_nailao(time1,time2):
    name_list=['D_T_MONTH','奶酪一级分类','D_P_BRAND','M_S_PHYSICALVOLUME','M_S_SALES','API.dbo.奶酪所有平台']
    sql=f'''select
    cast({name_list[0]} as nvarchar),cast({name_list[1]} as nvarchar),cast({name_list[2]} as nvarchar),cast({name_list[3]} as float),\
    cast({name_list[4]} as float) from {name_list[5]} where {name_list[0]} between {time1} and {time2}
    '''
    print(sql)
    database='API'
    column=['时间','奶酪品类','品牌','销量','销售额']
    df=sql_connect.select('192.168.0.15',database,'sqlsever',sql,column)
    result=df.groupby(['时间','奶酪品类','品牌'],as_index=False).sum()
    return result


def sql_hourulao(time1,time2):
    name_list=['D_T_MONTH','奶酪一级分类','D_P_BRAND','M_S_PHYSICALVOLUME','M_S_SALES','API.dbo.厚乳酪所有平台']
    sql=f'''select
    cast({name_list[0]} as nvarchar),cast({name_list[2]} as nvarchar),cast({name_list[3]} as float),\
    cast({name_list[4]} as float) from {name_list[5]} where {name_list[0]} between {time1} and {time2} and {name_list[2]} ='伊利'
    '''
    print(sql)
    database='API'
    column=['时间','奶酪品类','销量','销售额']
    df=sql_connect.select('192.168.0.15',database,'sqlsever',sql,column)
    df['奶酪品类']=df['奶酪品类'].replace('伊利','伊利厚乳酪')

    result=df.groupby(['时间','奶酪品类'],as_index=False).sum()

    return result


def df_time(num,list,time):
    for m in range(num):
        list.append((pd.Timestamp(time)+pd.DateOffset(n=-m, months=1)).strftime("%Y-%m").replace('-',''))
    return list 


def time_input(time):
    time_ty=time[:4]+'-'+time[-2:]
    time_ly=(pd.Timestamp(time_ty)+pd.DateOffset(n=-1, years=1)).strftime("%Y-%m")
    time_ly_start=time_ly[:4]+'-'+'12'
    time_lm=(pd.Timestamp(time_ty)+pd.DateOffset(n=-1, months=1)).strftime("%Y-%m")

    time_bl=(pd.Timestamp(time_ty)+pd.DateOffset(n=-2, years=1)).strftime("%Y-%m")
    time_bl_start=time_bl[:4]+'-'+'12'

    num=int(time[-2:])

    YTD_TY=[];YTD_LY=[];YTD_BL=[]
    YTD_TY=df_time(num,YTD_TY,time_ty)
    YTD_LY=df_time(num,YTD_LY,time_ly)
    YTD_BL=df_time(num,YTD_BL,time_bl)

    ytd_ty=[];ytd_ly=[]
    ytd_ty=df_time(num,ytd_ty,time_ly_start)
    ytd_ly=df_time(num,ytd_ly,time_bl_start)


    time_tm_list=[];time_ty_list=[];time_ly_list=[];time_total=[]
    time_lm_list=df_time(24,time_tm_list,time_lm)
    time_ty_list=df_time(24,time_ty_list,time_ty)
    time_ly_list=df_time(24,time_ly_list,time_ly)
    time_total=df_time(36,time_total,time_ty)
    time_ty_list.reverse()
    time_ly_list.reverse()
    time_total.reverse()
    time_lm_list.reverse()

    return YTD_TY,YTD_LY,YTD_BL,ytd_ty,ytd_ly,time_ty_list,time_ly_list,time_lm_list,time_total


def get_variable_name(var):
    for k, v in globals().items():
        if v is var:
            return str(k)
    return ""


def count(df_input,time_total,time_list,order,name,text_name):

    df_group=df_input[['时间',name,'销售额','销量']].groupby(['时间',name],as_index=False).sum()
    
    df_group['单价']=df_group['销售额']/df_group['销量']

    df_group_sales=df_group.pivot(index=[name],columns='时间',values='销售额').rename_axis(columns=None).reset_index()

    df_group_sales= df_group_sales.set_index(name)
    df_group_sales = df_group_sales.reindex(order)
    
    for column_name in time_total:
        if column_name not in df_group_sales.columns:
            df_group_sales[column_name] = np.nan
    

    df_share = (df_group_sales.div(df_group_sales.iloc[0])*100)[time_list[0]].reset_index()
    df_mom=(df_group_sales[time_list[0]].div(df_group_sales[time_list[1]].values)-1).reset_index()
    df_yoy=(df_group_sales[time_list[0]].div(df_group_sales[time_list[2]].values)-1).reset_index()

    share_output = df_share.melt(id_vars=[name], value_vars=df_share.columns[1:], var_name='时间', value_name='销售额份额')

    sales_output = df_group[df_group['时间'].isin(time_list[0])][['时间',name,'销售额']]
    sales_output['销售额']=sales_output['销售额'].div(100)

    mom_output = df_mom.melt(id_vars=[name], value_vars=df_mom.columns[1:], var_name='时间', value_name='销售额环比')
    mom_output['销售额环比']=mom_output['销售额环比']*100
    yoy_output = df_yoy.melt(id_vars=[name], value_vars=df_yoy.columns[1:], var_name='时间', value_name='销售额同比')
    yoy_output['销售额同比']=yoy_output['销售额同比']*100

    # return df_group_sales,df_share,sales_output,share_output

    price_output=df_group[df_group['时间'].isin(time_list[0])][['时间',name,'单价']]
    price_output['单价']=price_output['单价']*10000

    volume_output=df_group[df_group['时间'].isin(time_list[0])][['时间',name,'销量']]
    volume_output['销量']= volume_output['销量'].div(1000)


    output=pd.merge(share_output,sales_output,how='left',on=[name,'时间'])

    output=pd.merge(output,mom_output,how='left',on=[name,'时间'])

    output=pd.merge(output,yoy_output,how='left',on=[name,'时间'])  

    output=pd.merge(output,price_output,how='left',on=[name,'时间'])

    output=pd.merge(output,volume_output,how='left',on=[name,'时间'])

    output = output.replace(np.nan, 'NA')
    output = output.replace(np.inf, 'NA')

    output['奶酪竞争表现']=text_name
    output=output.rename(columns={name:'name'})
    output=output[['奶酪竞争表现','时间','name','销售额份额','销售额','销售额环比','销售额同比','单价','销量']]

    return output


def category(df,time_total,time_list,ytd_list,category_list):

    order = ['奶酪品类','儿童奶酪','佐餐奶酪','成人零食奶酪']

    df_input=df[['时间','奶酪品类','销量','销售额']].groupby(['时间','奶酪品类'],as_index=False).sum()
    concat_list=[]
    df_total=df_input[df_input['奶酪品类'].isin(category_list)][['时间','销量','销售额']].groupby(['时间'],as_index=False).sum()
    df_total['奶酪品类']='奶酪品类'
    df_input=pd.concat([df_total,df_input])

    for ytd in ytd_list:
        df_ytd=df_input[df_input['时间'].isin(ytd)][['奶酪品类','销量','销售额']].groupby(['奶酪品类'],as_index=False).sum()
        if df_ytd.empty:
            df_ytd['奶酪品类']=order
            df_ytd['销量']=0
            df_ytd['销售额']=0
        ytd_name=get_variable_name(ytd)
        df_ytd['时间']=ytd_name
        concat_list.append(df_ytd)

    df_input=pd.concat(concat_list+[df_input]).reset_index(drop=True)
    
    
    output=count(df_input,time_total,time_list,order,'奶酪品类','奶酪品类及子类概览')


    return output


    
def brand(df,time_total,time_list,ytd_list,rule):
   
    output_list=[]
    for num in range(len(rule)):

        category_list=rule.loc[num,'category_list']
        brand_list=rule.loc[num,'brand_list']
        text_name=rule.loc[num,'text_name']

        concat_list=[]
  
        df_input=df[df['奶酪品类'].isin(category_list)][['时间','品牌','销量','销售额']].groupby(['时间','品牌'],as_index=False).sum()

        if category_list==['儿童奶酪','佐餐奶酪','成人零食奶酪']:
            category_name='奶酪品类'
        else:
            category_name=category_list[0]

        order=[category_name]+brand_list

        df_total=df_input[['时间','销量','销售额']].groupby(['时间'],as_index=False).sum()
        df_total['品牌']=category_name
        df_input=pd.concat([df_total,df_input])

        for ytd in ytd_list:
            df_ytd=df_input[df_input['时间'].isin(ytd)][['品牌','销量','销售额']].groupby(['品牌'],as_index=False).sum()
            if df_ytd.empty:
                df_ytd['品牌']=[category_name]+list(df['品牌'].unique())
                df_ytd['销量']=0
                df_ytd['销售额']=0
            ytd_name=get_variable_name(ytd)
            df_ytd['时间']=ytd_name
            concat_list.append(df_ytd)

        df_input=pd.concat(concat_list+[df_input]).reset_index(drop=True)

        df_output=count(df_input,time_total,time_list,order,'品牌',text_name)
        
        output_list.append(df_output)
    output=pd.concat(output_list).reset_index(drop=True)

    return output



def category_hourulao(df,time_total,time_list,ytd_list):

    order = ['伊利厚乳酪']

    df_input=df[['时间','奶酪品类','销量','销售额']].groupby(['时间','奶酪品类'],as_index=False).sum()
    concat_list=[]

    for ytd in ytd_list:
        df_ytd=df_input[df_input['时间'].isin(ytd)][['奶酪品类','销量','销售额']].groupby(['奶酪品类'],as_index=False).sum()
        if df_ytd.empty:
            df_ytd['奶酪品类']=order
            df_ytd['销量']=0
            df_ytd['销售额']=0
        ytd_name=get_variable_name(ytd)
        df_ytd['时间']=ytd_name
        concat_list.append(df_ytd)

    df_input=pd.concat(concat_list+[df_input]).reset_index(drop=True)
    
    
    output=count(df_input,time_total,time_list,order,'奶酪品类','伊利厚乳酪销售概览')

    return output





def main (time):
    python_path,project_path,template_path,result_path=common.path()

    start  = datetime.datetime.now()

    YTD_TY,YTD_LY,YTD_BL,ytd_ty,ytd_ly,time_ty_list,time_ly_list,time_lm_list,time_total_list=time_input(time)
    time_start=time_total_list[0]
    time_end=time_total_list[-1]

    timeinput_yoy=[YTD_TY,YTD_LY,YTD_BL,ytd_ty,ytd_ly]

    time_list=[['YTD_LY','YTD_TY']+time_ty_list,['ytd_ly','ytd_ty']+time_lm_list,['YTD_BL','YTD_LY']+time_ly_list]

    time_total=['YTD_TY','YTD_LY','YTD_BL','ytd_ty','ytd_ly']+time_total_list


    df_nailao=sql_nailao(time_start,time_end)
    df_hourulao=sql_hourulao(time_start,time_end)


    rule_nailao=pd.DataFrame({
        'category_list':[['儿童奶酪','佐餐奶酪','成人零食奶酪'],['儿童奶酪'],['佐餐奶酪'],['成人零食奶酪']],

        'text_name':['奶酪品类竞争格局','儿童奶酪竞争格局','佐餐奶酪竞争格局','成人零食奶酪竞争格局'],

        'brand_list':[['伊利','妙可蓝多','百吉福','妙飞','奶酪博士','吉士丁','蒙牛','光明','安佳','乐芝牛','总统','凯芮','多美鲜','爱氏晨曦','认养一头牛','展艺'],
                      ['伊利','妙可蓝多','百吉福','妙飞','奶酪博士','吉士丁','蒙牛','爱氏晨曦'],
                      ['伊利','妙可蓝多','百吉福','妙飞','奶酪博士','吉士丁','蒙牛','光明','安佳','乐芝牛','总统','凯芮','多美鲜','爱氏晨曦','展艺'],
                      ['伊利','妙可蓝多','百吉福','吉士丁','乐芝牛','凯芮','奶酪博士']]
    }
    )

    name_mapping=pd.DataFrame({
        'number':['L1','L2','L3','L4','A1','A2','A3','A4','A5','A6','A7','B1','B3','B5','B8','C1','C2','C3','Y1','Y2','L5'],
        'name':['奶酪品类','儿童奶酪','佐餐奶酪','成人零食奶酪','伊利','妙可蓝多','百吉福','妙飞','奶酪博士','吉士丁','蒙牛','光明'
                ,'安佳','乐芝牛','总统','凯芮','多美鲜','爱氏晨曦','认养一头牛','展艺','伊利厚乳酪'],
    })
    name_mapping['子类/品牌']=name_mapping.apply(lambda row: row['number']+'.'+row['name'],axis=1)



    df_group=df_nailao[['时间','奶酪品类','销售额','销量']].groupby(['时间','奶酪品类'],as_index=False).sum()


    result_total=category(df_group,time_total,time_list,timeinput_yoy,['儿童奶酪','佐餐奶酪','成人零食奶酪'])
    result_total=pd.merge(result_total,name_mapping[['name','子类/品牌']],how='left',on=['name'])

    result_nailao=brand(df_nailao,time_total,time_list,timeinput_yoy,rule_nailao)
    result_nailao=pd.merge(result_nailao,name_mapping[['name','子类/品牌']],how='left',on=['name'])

    result_hourulao=category_hourulao(df_hourulao,time_total,time_list,timeinput_yoy)
    result_hourulao=pd.merge(result_hourulao,name_mapping[['name','子类/品牌']],how='left',on=['name'])

    result=pd.concat([result_total,result_nailao,result_hourulao]).reset_index(drop=True)

    result=result[['奶酪竞争表现','时间','子类/品牌','销售额份额','销售额','销售额环比','销售额同比','单价','销量']]

    current_time = datetime.datetime.now()
    formatted_time = current_time.strftime("%Y-%m-%d %H_%M_%S")

    template=f'{template_path}/伊利奶酪竞争格局改版-模板.xlsx'
    filename = f'{result_path}/伊利奶酪竞争格局改版-模板{time}_{formatted_time}.xlsx'
    wb = openpyxl.load_workbook(template)

    start_col=1
    start_row=2
    ws = wb['简化模板']
    for r in dataframe_to_rows(result, index=False, header=False):
        for col_idx, cell_value in enumerate(r, start_col):
            ws.cell(row=start_row, column=col_idx, value=cell_value)
        start_row += 1
    wb.save(filename)
    end  = datetime.datetime.now()
    print("程序运行时间："+str((end-start).seconds)+"秒")


if __name__ =="__main__":

    received_arguments = sys.argv[1:]
    time_begin=received_arguments[0]
    time_end=received_arguments[1]
    main(time_begin)
