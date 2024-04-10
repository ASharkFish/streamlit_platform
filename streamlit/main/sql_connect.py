import pyodbc
import pymysql
import pandas as pd
import datetime

from sqlalchemy import create_engine, text, types



def connect(db_host,db_name,db_type):
    if db_host=='192.168.0.20':
        db_user = 'liang  '
        db_password = 'liangjianqing'

    elif db_host=='192.168.0.213':
        db_user = 'syntun_csc'
        db_password = 'SYNTUN@syntun'

    else:
        db_user = 'zhongxin_bao'
        db_password = 'Bao_Zxin_0227'
        
    driver='SQL Server'
    if db_type=='sqlsever':
        connection_string = f'mssql+pyodbc://{db_user}:{db_password}@{db_host}/{db_name}?driver={driver}'    
    elif db_type=='mysql':
        connection_string = f'mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}'   

    engine = create_engine(connection_string)
    return engine




def select(db_host,db_name,db_type,sql,column):
    
    engine=connect(db_host,db_name,db_type)
    conn = engine.connect()
    result = conn.execute(text(sql))
    rows = result.fetchall()
    conn.close()
    engine.dispose()    
    df_output=pd.DataFrame(rows,columns=column)
    
    return df_output



def input(db_host,db_name,db_type,df_input,table_name):
    engine=connect(db_host,db_name,db_type)

    dtype_dict = {col_name: types.NVARCHAR(length=2000) for col_name in df_input.select_dtypes(include='object').columns}
    dtype_dict.update({col_name: types.Float(precision=53) for col_name in df_input.select_dtypes(include='float').columns})
    dtype_dict.update({col_name: types.INT for col_name in df_input.select_dtypes(include='int').columns})
    df_input.to_sql(table_name,engine,if_exists='replace', index=False,dtype=dtype_dict)
    print('插入完成')
    engine.dispose()

if __name__ == "__main__":
    db_host='192.168.0.213'
    db_name='API'
    db_type='sqlsever'


    engine=connect(db_host,db_name,db_type)
    print(engine)
    conn = engine.connect()
