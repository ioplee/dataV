# ods库 库表VO对象
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


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


class OdsTablePageQueryModel(OdsTableQueryModel):
    """
    分页查询模型
    """

    page_num: int = Field(default=1, description='当前页码')
    page_size: int = Field(default=10, description='每页记录数')
