import pandas as pd 
import numpy as np 
import sys
import sql_connect
pd.set_option('display.max_info_columns', 0)

def sql_select():
    sql=''' select distinct CAST ( 客户 AS nvarchar ( 500 ) ),CAST ( 品类 AS nvarchar ( 500 ) ),\
        CAST ( 客户品类 AS nvarchar ( 500 ) ) from QC.dbo.客户品类对照表 where [客户] LIKE N'%恒天然%'
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
    category=[]

    df_output2=df['品类'].drop_duplicates().reset_index(drop=True)
    df_output2.index += 1

    print('品类清单:\n',df_output2)

    name_input2=input('请输入品类')

    name_input2_list=name_to_list(name_input2,df_output2)

    ke_hu=df[df['品类'].isin(name_input2_list)][['客户品类','品类']].drop_duplicates().reset_index(drop=True)
    
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
            kehu.append(ke_hu['客户品类'].tolist()[name])
            category.append(ke_hu['品类'].tolist()[name])
        except:
            if name == 'all':
                kehu=kehu+ke_hu['客户品类'].tolist()
                category=category+ke_hu['客户品类'].tolist()
            else:
                kehu.append(name)
                category.append(ke_hu[ke_hu['客户品类']==name]['品类'].values)


    print(kehu,'\n',category)
    return kehu,category

if __name__ =="__main__":
    kehu=main()