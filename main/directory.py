import pandas as pd 
import numpy as np 
import sys
import main.sql_connect as sql_connect
pd.set_option('display.max_info_columns', 0)

def sql_select():
    sql=''' select distinct CAST ( 客户 AS nvarchar ( 500 ) ),CAST ( 品类 AS nvarchar ( 500 ) ),\
        CAST ( 客户品类 AS nvarchar ( 500 ) ) from QC.dbo.客户品类对照表
    '''
    column=['客户','品类','客户品类']
    df=sql_connect.select('192.168.0.15','QC','sqlsever',sql,column)
    return df




def name_to_list(name_input,df):
    name_list=name_input.split(',')
    kehu=[]
    for name in name_list:
        try:
            name=int(name)-1
            kehu.append(df.tolist()[name])
        except:
            if name == 'all':
                kehu=kehu+df.tolist()
            else:
                kehu.append(name)
    return kehu



def main():
    df=sql_select()
    kehu=[]
    df_output1=df['客户'].drop_duplicates().reset_index(drop=True)
    df_output1.index += 1

    print('客户清单:\n',df_output1)

    name_input1=input('请输入客户')

    name_input1_list=name_to_list(name_input1,df_output1)

    df_output2=df[(df['客户'].isin(name_input1_list))]['品类'].drop_duplicates().reset_index(drop=True)
    df_output2.index += 1

    print('品类清单:\n',df_output2)

    name_input2=input('请输入品类')

    name_input2_list=name_to_list(name_input2,df_output2)

    ke_hu=df[(df['客户'].isin(name_input1_list))&(df['品类'].isin(name_input2_list))]['客户品类'].drop_duplicates().reset_index(drop=True)
    ke_hu.index += 1

    print('客户品类清单:\n',ke_hu)

    if ke_hu.empty:
        print('无对应客户品类,请重新输入')
        sys.exit()
   

    number_kehu=input('请选择输入客户品类(名称或序号)')

    number_kehu=number_kehu.split(',')
    for name in number_kehu:
        try:
            name=int(name)-1
            kehu.append(ke_hu.tolist()[name])
        except:
            if name == 'all':
                kehu=kehu+ke_hu.tolist()
            else:
                kehu.append(name)

    print(kehu)
    return kehu

if __name__ =="__main__":
    kehu=main()