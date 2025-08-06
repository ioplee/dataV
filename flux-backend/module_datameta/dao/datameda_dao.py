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
from psycopg2.extras import RealDictCursor
from sqlalchemy import text
from config.constant import BizConstant
from module_datameta.entity.vo.ods_table_vo import OdsTableQueryModel, OdsTablePageQueryModel
import config.pg_database as pgMaster
from utils.page_util import PageUtil


class DataMetaDao:
    """
    数仓 数据库操作类
    """

    @classmethod
    def get_ods_table_page(cls, query_object: OdsTablePageQueryModel):
        # 获取连接
        connection, cursor = pgMaster.connect_postgreSQL()
        # 获取总记录数
        result_sql = text(
            """
            SELECT n.nspname              AS schema_name,
                   c.relname              AS table_name,
                   obj_description(c.oid) AS table_comment,
                   (SELECT COUNT(*)
                    FROM pg_attribute a
                    WHERE a.attrelid = c.oid
                      AND a.attnum > 0)   AS column_count,
                   c.reltuples::BIGINT AS estimated_row_count, pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
            FROM pg_class c
                     JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'r'                           -- 仅普通表
              AND n.nspname NOT IN ('pg_catalog', :nspname) -- 查询指定数据空间的库表清单
            ORDER BY n.nspname, c.relname -- 按表空间、表明排序
                limit :pageSize
            offset :pageNum
            """
        )
        record_count = connection.execute(result_sql, {'nspname': BizConstant.ODS_SPACE_NAME,
                                                       'pageSize': query_object.page_size,
                                                       'pageNum': query_object.page_num})

        # 获取记录总数
        count_sql = text(
            """
            SELECT count(1)
            FROM pg_class c
                     JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'r' -- 仅普通表
              AND n.nspname = :nspname;
            """
        )
        record_count = connection.execute(count_sql, {'nspname': BizConstant.ODS_SPACE_NAME})

        # 封装分页列表
        table_list = PageUtil.paginateBySql(record_count, record_count, query_object.page_num, query_object.page_size)
        # 关闭游标和数据库连接
        pgMaster.close_postgreSQL(connection, cursor)
        return table_list
