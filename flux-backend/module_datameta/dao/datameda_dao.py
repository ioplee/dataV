# 数仓 数据库操作类
# coding:utf-8
'''
**************************************************
@File   ：flux-backend -> datameta_service
@IDE    ：PyCharm
@Author ：robin
@Date   ：2025/8/6 11:58
**************************************************
'''
import logging
import math

from psycopg2.extras import RealDictCursor

from config.constant import BizConstant
from module_datameta.entity.vo.ods_table_vo import OdsTableQueryModel, OdsTablePageQueryModel
import config.pg_database as pgMaster
from utils.page_util import PageUtil
from utils.log_util import logger


class DataMetaDao:
    """
    数仓 数据库操作类
    """

    @classmethod
    def get_ods_table_page(cls, query_object: OdsTablePageQueryModel):
        logger.info("进入 get_ods_table_page")
        try:
            # 获取连接
            connection, cursor = pgMaster.connect_postgreSQL()
            # 获取总记录数
            pageNum: int = query_object.getPageNum()
            pageSize: int = query_object.getPageSize()
            index: int = (query_object.page_num - 1) * query_object.page_size
            result_sql = [
                " SELECT n.nspname AS schema_name,c.relname AS table_name,obj_description(c.oid) AS table_comment, ",
                " (SELECT COUNT(*) FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0)   AS column_count, ",
                " c.reltuples::BIGINT AS estimated_row_count, pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size ",
                " FROM pg_class c ",
                "  JOIN pg_namespace n ON c.relnamespace = n.oid ",
                " WHERE c.relkind = 'r' ",
                "  AND n.nspname =  '" + BizConstant.ODS_SPACE_NAME + "'",
                " ORDER BY n.nspname, c.relname ",
                " limit " + str(query_object.page_size) + " offset " + str(index)
            ]

            query = " ".join(result_sql)
            logger.info(query)
            cursor.execute(query)
            record_list = cursor.fetchall()
            # for record in record_list:
            #     print(record)
            # logger.info("记录为：")
            # logging.info(record_list)
            # 获取记录总数
            count_sql = [
                " SELECT count(1) as records ",
                " FROM pg_class c  JOIN pg_namespace n ON c.relnamespace = n.oid ",
                " WHERE c.relkind = 'r' ",
                "  AND n.nspname = '" + BizConstant.ODS_SPACE_NAME + "'"
            ]
            query = " ".join(count_sql)

            cursor.execute(query)
            logger.info(query)
            record_count: int = cursor.fetchone()[0]
            print("总数", record_count)
            # 封装分页列表
            table_list = PageUtil.paginateBySql(record_count, record_list, query_object.page_num,
                                                query_object.page_size)
            # 关闭游标和数据库连接
            pgMaster.close_postgreSQL(connection, cursor)
            logger.info("结束 get_ods_table_page")
            return table_list
        except Exception as e:
            pass
            print("发生异常")
            logger.exception(e)

    # 按schema 读取其下所辖的表清单(分页）
    @classmethod
    def get_tables_byschema_page(cls, query_object: OdsTablePageQueryModel):
        try:
            conn = pgMaster.get_conn_pool()
            print("Connection pool created successfully")
            cursor = conn.cursor()
            # 查询记录
            index: int = (query_object.page_num - 1) * query_object.page_size
            result_sql = [
                " select json_agg(row_to_json(t))",
                # " select row_to_json(t) ",
                " from( ",
                " SELECT n.nspname AS schema_name,c.relname AS table_name,obj_description(c.oid) AS table_comment, ",
                " (SELECT COUNT(*) FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0)   AS column_count ",
                # " ,c.reltuples::BIGINT AS estimated_row_count, pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size ",
                " FROM pg_class c ",
                "  JOIN pg_namespace n ON c.relnamespace = n.oid ",
                " WHERE c.relkind = 'r' ",
                "  AND n.nspname =  '" + BizConstant.ODS_SPACE_NAME + "'",
                " ORDER BY n.nspname, c.relname ",
                " limit " + str(query_object.page_size) + " offset " + str(index),
                " ) t ",
            ]
            query = " ".join(result_sql)
            cursor.execute(query)
            record_list = cursor.fetchone()[0]
            print(record_list)

            # 查询记录总数
            count_sql = [
                " SELECT count(1) as records ",
                " FROM pg_class c  JOIN pg_namespace n ON c.relnamespace = n.oid ",
                " WHERE c.relkind = 'r' ",
                "  AND n.nspname = '" + BizConstant.ODS_SPACE_NAME + "'"
            ]
            query = " ".join(count_sql)
            cursor.execute(query)
            record_count: int = cursor.fetchone()[0]
            has_next = math.ceil(record_count / query_object.page_size) > query_object.page_num
            result = {
                "rows": record_list,
                "pageNum": query_object.page_num,
                "pageSize": query_object.page_size,
                "total": record_count,
                "hasNext": has_next,
            }
            return result
        except Exception as e:
            logger.info(f"get_tables_byschema_page error:{e}")
            pass
        finally:
            pgMaster.turn_conn_to_pool(conn)
            pass
