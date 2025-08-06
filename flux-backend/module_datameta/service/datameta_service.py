# coding:utf-8
'''
**************************************************
@File   ：flux-backend -> datameta_service
@IDE    ：PyCharm
@Author ：robin
@Date   ：2025/8/6 11:58
**************************************************
'''
from module_datameta.entity.vo.ods_table_vo import OdsTablePageQueryModel
from utils.common_util import CamelCaseUtil, export_list2excel
from module_datameta.dao.datameda_dao import DataMetaDao


class DatametaService:
    """
    元数据管理service类
    """

    @classmethod
    async def get_ods_tables_page(cls, query_object: OdsTablePageQueryModel):
        """
        获取源数据库的库表分页记录
        """
        list_result = await DataMetaDao.get_ods_table_page(query_object)
        return list_result
