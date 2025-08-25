# 元数据管理 请求类
# coding:utf-8
'''
**************************************************
@File   ：flux-backend -> datameta_controller
@IDE    ：PyCharm
@Author ：robin
@Date   ：2025/8/6 13:59
**************************************************
'''
from fastapi import APIRouter, Depends, Request
from pydantic import Field

from utils.log_util import logger
from module_admin.service.login_service import LoginService
from module_datameta.entity.vo.ods_table_vo import OdsTablePageQueryModel
from module_datameta.service.datameta_service import DatametaService
from utils.page_util import PageResponseModel
from utils.response_util import ResponseUtil

# datametaController = APIRouter(prefix='/datameta', dependencies=[Depends(LoginService.get_current_user)])
datametaController = APIRouter(prefix='/datameta')


@datametaController.get(
    path='/odslist', response_model=PageResponseModel
)
def get_ods_table_list(
        request: Request,
        page_num: int = 0,
        page_size: int = 10,
):
    page_query = OdsTablePageQueryModel()
    page_query.set(page_num, page_size)
    ods_table_list = DatametaService.get_ods_tables_page(page_query)
    logger.info('获取 ods 库分页记录成功')
    return ResponseUtil.success(rows=ods_table_list)


@datametaController.get("/table_list_page")
def get_table_list_page_by_schema(
        request: Request,
        page_num: int = 0,
        page_size: int = 10,
        schema: str = 'jsw_data_ods'
):
    page_query = OdsTablePageQueryModel()
    page_query.set(page_num, page_size)
    page_query.setSchema(schema)
    table_list_page = DatametaService.get_schema_tables_list_page(page_query)
    return table_list_page
