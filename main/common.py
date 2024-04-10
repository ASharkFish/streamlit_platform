import  time
from datetime import timedelta
from datetime import datetime
import pandas as pd 
import  os
from dateutil.relativedelta import relativedelta

def project_path():
    # 获取当前 Python 文件的绝对路径
    file_path = os.path.abspath(__file__)
    path = os.path.dirname(file_path).split('scripts')[0]
    return path

def  total_time(start_time):
    # 计算整个过程的运行时间
    total_time = time.time() - start_time
    total_time = str(timedelta(seconds=int(total_time)))

    return total_time


def time_to_time(date1, date2):
    start_date = datetime.strptime(date1, "%Y%m")
    end_date = datetime.strptime(date2, "%Y%m")

    all_months = []

    while start_date <= end_date:
        all_months.append(start_date.strftime("%Y%m"))
        start_date += relativedelta(months=1)

    return all_months


def year_time(input_date, num):
    input_dt = datetime.strptime(input_date, "%Y%m")

    # 计算向前推 num 年后的时间
    previous_years_date = input_dt - relativedelta(years=num)

    return previous_years_date.strftime("%Y%m")


#计算mat和ytd
def mat_ytd(input_time,num):
    input_date = year_time(input_time, num)

    input_dt = datetime.strptime(input_date, "%Y%m")
    # 当年
    current_year = input_dt.year

    # 当年MAT
    mat_start_date = input_dt
    mat = [mat_start_date.strftime("%Y%m")]
    
    # 计算前11个月的MAT
    for i in range(1, 12):
        mat_month = mat_start_date - relativedelta(months=i)
        mat.append(mat_month.strftime("%Y%m"))

    # 当年YTD
    ytd_start_date = datetime(current_year, 1, 1)
    ytd = [ytd_start_date.strftime("%Y%m")]
    
    # 计算前面所有月份的YTD
    while ytd_start_date < input_dt:
        ytd_start_date += relativedelta(months=1)
        ytd.append(ytd_start_date.strftime("%Y%m"))

    # 去年
        
    return mat, ytd


def year_time(input_date, num):
    input_dt = datetime.strptime(input_date, "%Y%m")

    previous_years_date = input_dt - relativedelta(years=num)

    return previous_years_date.strftime("%Y%m")


#向前滚动13个月
def month_time(start_date, num):
    start_date = datetime.strptime(start_date, '%Y%m')
    previous_months = [start_date.strftime('%Y%m')]
    
    for i in range(num-1):
        previous_date = start_date - timedelta(days=start_date.day)
        previous_months.append(previous_date.strftime('%Y%m'))
        start_date = previous_date
    previous_months.reverse()
    return previous_months

# 示例用法

def path():
    interpreter_ytpe='windows'

    if interpreter_ytpe=='windows':
        file_total=f'D:/工作/python/streamlit平台'
        python_path=f'D:/工作/python/streamlit平台/streamlit_platfrom/Scripts/python.exe'

    elif interpreter_ytpe=='linux':
        file_total=f'/data/python_projects/zichanPro'
        python_path=f'/data/local/miniconda3/bin/python'
    
    return python_path,file_total



def time_to_now(time_input):
    now = datetime.now()  # 获取当前日期和时间
    formatted_time = now.strftime("%Y-%m")
    list=[]
    m=0
    output=''
    while output < formatted_time :
        output=(pd.Timestamp(time_input)+pd.DateOffset(n=m, months=1)).strftime("%Y-%m")
        list.append(output.replace('-',''))
        m+=1
    return list 








if __name__ == "__main__":
    time_list=time_to_now('2019-01')