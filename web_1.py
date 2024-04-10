import subprocess
import pandas as pd
import numpy as np
from datetime import datetime

import yaml
from yaml.loader import SafeLoader

import streamlit as st
import streamlit_authenticator as stauth

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from main import sql_connect
from main import common
from main import directory

import codecs
current_time = datetime.now()

# 将时间转换为 YYYYMM 格式




sys_type='wndows'
sys_login='no'



python_path,file_total=common.path()
project_path=f'{file_total}/project'
template_path=f'{file_total}/template'
result_path=f'{file_total}/result'
main_path=f'{file_total}/main'
doc_path=f'{file_total}/document/doc'
update_path=f'{file_total}/document/updates'







def sql_select():
    sql=''' select distinct CAST ( 客户 AS nvarchar ( 500 ) ),CAST ( 品类 AS nvarchar ( 500 ) ),\
        CAST ( 客户品类 AS nvarchar ( 500 ) ) from QC.dbo.客户品类对照表
    '''
    column=['客户','品类','客户品类']
    df=sql_connect.select('192.168.0.15','QC','sqlsever',sql,column)
    return df


def directory_main(df):

    selected_customer = st.selectbox("选择客户：", df['客户'].unique())
    st.write(f"已选择的客户：{selected_customer}")

    filtered_by_customer = df[df['客户'] == selected_customer]

    selected_category = st.selectbox("选择品类：", filtered_by_customer['品类'].unique())
    st.write(f"已选择的品类：{selected_category}")

    filtered_by_category = filtered_by_customer[filtered_by_customer['品类'] == selected_category]

    selected_customer_name = st.multiselect("选择客户名（注：若选择项目为数据库评分系统则仅支持单选客户，若多选则仅输出第一选择）：", filtered_by_category['客户品类'])
    st.write(f"最终选择的客户名：{selected_customer_name}")
    return selected_customer_name


def df_time(num,list,time):
    
    for m in range(num):
        list.append((pd.Timestamp(time)+pd.DateOffset(n=-m, months=1)).strftime("%Y-%m").replace('-',''))

    return list 


def time_to_now(time,input):
    list=[]
    m=0
    input='2017-01'
    output=''
    while output < time :
        output=(pd.Timestamp(input)+pd.DateOffset(n=m, months=1)).strftime("%Y-%m")
        list.append(output.replace('-',''))
        m+=1
    return list 


def process_info(process):
    # 实时读取输出并显示
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            st.markdown(f'<p style="font-family: 宋体; font-size: small; line-height: 1.2 ;color: #FFFFFF;">{output.strip()}</p>',
                        unsafe_allow_html=True)


def previous_month(year_month):
    year = int(year_month[:4])
    month = int(year_month[4:])
    
    if month == 1:
        previous_year = year - 1
        previous_month = 12
    else:
        previous_year = year
        previous_month = month - 1
        
    return f'{previous_year}{previous_month:02}'


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:    
        os.makedirs(path) 


def os_path(directory):

    py_files = []

    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            # 检查文件是否以.py为扩展名
            if filename.endswith('.py'):
                # 如果是，将文件名添加到列表中
                py_files.append(filename.replace('.py',''))
    return py_files


def find_files_with_keyword(directory, keyword):

    py_files = []
    
    for dirpath, dirnames, filenames in os.walk(directory):
        for filename in filenames:
            if keyword in filename:
                file_path = os.path.join(dirpath, filename)
                py_files.append(filename.replace('.py',''))
    
    return py_files






def project_main (variable_list,filename,key_word):
    if key_word=='检查程序':

        path_file=f"{project_path}/check/{filename}.py"

    elif key_word=='评分程序':

        path_file=f"{project_path}/score/{filename}.py"

    elif key_word=='推算程序':

        path_file=f"{project_path}/molde/{filename}.py"

    elif key_word=='报表程序':

        path_file=f"{project_path}/excel/{filename}.py"

    variable_list=[python_path,path_file]+variable_list

    st.subheader(filename+':')

    with st.spinner("运行中..."):
        process_main=subprocess.Popen(variable_list, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, universal_newlines=True)
        process_info(process_main)







def download(result_folder):
    with st.sidebar:
        st.title('下载平台')
        file_list = os.listdir(result_folder)
        if file_list==[]:
            write=pd.ExcelWriter(f'{result_folder}/临时占位文件.xlsx')
            df_linshi=pd.DataFrame()
            df_linshi.to_excel(write)
            write.close()

        selected_file = st.selectbox('选择要下载的文件', file_list)
        file_path = os.path.join(result_folder, selected_file)
        
        with open(file_path, 'rb') as file:
            file_content = file.read()
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(label='点击下载', data=file_content, file_name=selected_file, mime='application/octet-stream')
            with col2 :
                button_click=st.button('点击刷新')
        if button_click:
            file_list = os.listdir(result_folder)



def sql_login():

    sql=f'''
        select ID,user_name,name,password from [predict].[dbo].[User] 
    '''
    db_name='predict'
    column=['ID','user','name','password']
    df=sql_connect.select('192.168.0.15',db_name,'sqlsever',sql,column)
    return df 


def login(platform_name):

    df_login= sql_login()
    
    credentials_list={}
    for num in range(len(df_login)):
        account=df_login.loc[num,'user']
        name=df_login.loc[num,'name']
        password=df_login.loc[num,'password']

        dict_linshi={'email': account,
                    'name': name,
                    'password':password }
        credentials_list[account]=dict_linshi

    config={'credentials':{'usernames':credentials_list,},
            'cookie': {'expiry_days': 30,
                        'key': 'some_signature_key',
                        'name': 'some_cookie_name'},
            }


    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
    )

    dict={'Form name':platform_name, 'Username':'账户', 'Password':'密码','Login':'登录'}

    authenticator.login(fields=dict)



    permission=st.session_state["authentication_status"]

    if permission is False:
        st.error('用户名或密码输入错误')
        platform_list=[]


    elif permission is None:
        st.warning('请输入用户名和密码')
        platform_list=[]

    elif permission:


        st.title(platform_name)
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"<p style='font-size: 24px; font-weight: bold;'>欢迎 {st.session_state['name']}</p>", unsafe_allow_html=True)
        with col2:
            authenticator.logout(button_name='退出')


        platform_total=['检查及评分平台','模型推算平台','excel报表平台']


        if st.session_state["name"]=='管理员':   

            platform_list=platform_total

        elif st.session_state['name']=='数据分析':

            platform_list=['检查及评分平台']

        elif st.session_state['name']=='商业分析':

            platform_list=['excel报表平台']

        elif st.session_state['name']=='数据科学':

            platform_list=['模型推算平台']

        else:

            platform_list=['登录错误或无权限']
    
    return platform_list,permission




def run (choice_platform):

    if choice_platform=='检查及评分平台':
        directory = f'{project_path}/check'

        directory_score = f'{project_path}/score'

        matching_files=find_files_with_keyword(directory_score, '数据库检查评分')

        project_total=matching_files+os_path(directory)


    elif choice_platform=='excel报表平台':

        directory = f'{project_path}/molde'
        project_total=os_path(directory)
        key_word='报表程序'


    elif choice_platform=='模型推算平台':

        directory = f'{project_path}/excel'
        project_total=os_path(directory)
        key_word='推算程序'

    with st.sidebar:
        st.title(choice_platform)

        filename = st.selectbox("选择检查项目", project_total)
        time_list=common.time_to_now('2024-01')

        time_begin=st.selectbox("选择开始时间：",time_list)

        indices_a = [i for i, x in enumerate(time_list) if x == time_begin]

        new_list = []
        for index_a in indices_a:
            if index_a < len(time_list):
                new_list.extend(time_list[index_a:])
        time_end=st.selectbox("选择结束时间：",new_list)

        df=sql_select()

        kehu_list=directory_main(df) 
        
        button_click=st.button('确认')
        download(result_path)
    return button_click,filename,kehu_list,time_begin,time_end,matching_files






def main():

    mkdir(result_path)

    if sys_type=='wndows':

        pagetitle="Syntun 数据流集成平台 DIPFS(Data Integration Platform For Syntun)"
        platform_name='Syntun 数据流集成平台'

    elif sys_type=='linux':

        pagetitle="检查程序测试平台"
        platform_name='检查程序测试平台'


    st.set_page_config(
        page_title=pagetitle,
        page_icon="🌎",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'About': "Syntun 数据流集成平台 V0.1.0"
        }
    )

    
    if sys_login=='yes':

        platform_list,permission=login(sys_type,platform_name)

    elif sys_login=='no':

        permission=True
        st.title(platform_name)
        st.write(f"<p style='font-size: 24px; font-weight: bold;'>欢迎</p>", unsafe_allow_html=True)
            

    if permission:
        tab1, tab2, tab3,tab4 = st.tabs(["运行过程", "项目说明", "更新记录", "关于"])


        if sys_type=='wndows':
            platform_list=['检查及评分平台','模型推算平台','excel报表平台']
        else:
            platform_list=['检查及评分平台']


        choice_platform = st.sidebar.radio("选择一个步骤", platform_list)
        
        button_click1,filename,kehu_list,time_begin,time_end,matching_files=run (choice_platform)

        if button_click1:
            with tab1:
                st.write(f'检查程序: {filename}')
                st.write(f'检查客户：{",".join(kehu_list)}')
                st.write(f'检查时间: 从{time_end}到{time_begin}')

                with st.expander("运行详情(请点击查看)"):

                    start  = datetime.now()
                    variable_list=[",".join(kehu_list),time_begin,time_end]
                    try:
                        if choice_platform=='检查及评分平台':
                            if filename in matching_files:
                                key_word='评分程序'
                            else:
                                key_word='检查程序'
                        project_main(variable_list,filename,key_word)

                    except Exception as error:
                        try_word=False
                        st.code(error)

                    end  = datetime.now()
                    try_word=True
                    st.write(f'输出结束，运行时间{str((end-start).seconds)}秒')

                if try_word:
                    st.write("运行结束,详细信息请打开折叠框查看。")
                else:
                    st.write("ERROR,报错信息请打开折叠框查看。")      
            with tab2:
                    1111



if __name__ == "__main__":
    main()