# ods库 库表VO对象
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from module_admin.annotation.pydantic_annotation import as_query


class OdsTableModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, from_attributes=True)

    schema_name: Optional[str] = Field(default=None, description='数据域')
    table_name: Optional[str] = Field(default=None, description='数据库表名')
    table_comment: Optional[str] = Field(default=None, description='描述说明')
    column_count: Optional[int] = Field(default=None, description='字段数量')
    total_size: Optional[int] = Field(default=None, description='容量')


class OdsTableQueryModel():
    """
    查询条件封装
    """
    table_name: Optional[str] = Field(default=None, description='数据库表名')


class OdsTablePageQueryModel(OdsTableQueryModel):
    """
    分页查询模型
    """

    page_num: int = Field(default=1, description='当前页码')
    page_size: int = Field(default=10, description='每页记录数')
    schema: str = Field(default='jsw_data_ods', description='schema名')

    def getPageNum(self):
        return self.page_num

    def getPageSize(self):
        return self.page_size

    def getSchema(self):
        return self.schema

    def set(self, pageNum: int, pageSize: int):
        self.page_num = pageNum
        self.page_size = pageSize

    def setSchema(self, schema: str):
        self.schema = schema
