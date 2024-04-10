import sys
import os

# 添加 main 文件夹到 Python 解释器的搜索路径中
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from main import sql_connect