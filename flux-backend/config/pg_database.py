# postgresql 数据库操作
'''
**************************************************
@File   ：flux-backend -> datameta_service
@IDE    ：PyCharm
@Author ：robin
@Date   ：2025/8/6 11:58
**************************************************
'''
# /****************************************************************************
#
#  类名：      pgMaster.py
#  描述：      执行对posygresql数据库的相关操作
#  作者：      robin
#  版本：      1.0
#  日期：      2025-07-01
#
#  文件修改历史：
#  --------------------------------------------------------------------------------------------------
#  |     <时间>    |     <版本>        |    <作者>    |            更新说明                           |
#  |   2025-07-01 |        1.0       |    robin     |    初始化postgreSql 数据库操作基类              |
#  --------------------------------------------------------------------------------------------------
# ******************************************************************************/
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime

from psycopg2.pool import SimpleConnectionPool


# @brief: 建立数据库连接（数仓数据库连接）
# @param[in]:void
# @retval:conn->数据库连接对象
#         cur->执行数据库操作的游标
def connect_postgreSQL():
    conn = psycopg2.connect(database="jsw_data",
                            user="ops",
                            password="Jsw123%^",
                            host="dev.jinshuwan.com",
                            port="5432")
    cur = conn.cursor()
    print('connect database jsw_data successful!')
    return conn, cur


# @brief: 关闭与数据库的连接
# @param[in]:dataBase->要关闭连接的数据库
#            conn->数据库连接对象
#         cur->执行数据库操作的光标
# @retval:void
def close_postgreSQL(conn, cur):
    cur.close()
    conn.commit()
    conn.close()
    print('database jsw_data has been closed!')


##############################  连接池配置  #########################
connection_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dbname="jsw_data",
    user="ops",
    password="Jsw123%^",
    host="dev.jinshuwan.com",
    port="5432"
)


# 从连接池中获取连接
def get_conn_pool():
    return connection_pool.getconn()


# 归还连接回连接池
def turn_conn_to_pool(conn: SimpleConnectionPool):
    return connection_pool.putconn(conn)


##############################  连接池配置  #########################

###############################  基础操作和配置    ###################
# 定义一个转换函数
def datetime_converter(o):
    if isinstance(o, datetime):
        return o.isoformat()

############################### 以下为数据库操作  ####################
