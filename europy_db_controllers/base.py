from __future__ import annotations
import datetime, typing
import sqlalchemy
from sqlalchemy import orm as sqlalchemy_orm


class Base(sqlalchemy_orm.DeclarativeBase):
  # source: https://medium.com/@alanhamlett/part-1-sqlalchemy-models-to-json-de398bc2ef47
  __abstract__ = True
  _exclude_from_json: typing.List[str] = []
  _display_lists: typing.List[str] = []
  _is_part_of_list_of: typing.List[str] = [] 
  _show_id_of: typing.List[str] = []
  _sorted_by: typing.List[str] = None
  _hyb_props_replacing_columns: typing.Dict[str, str] = {}

  modified_at: sqlalchemy_orm.Mapped[datetime.date] = sqlalchemy_orm.mapped_column(
                                          type_ = sqlalchemy.TIMESTAMP,
                                          nullable = False,
                                          default = datetime.datetime.now)
  created_at: sqlalchemy_orm.Mapped[datetime.datetime] = sqlalchemy_orm.mapped_column(
                                          type_ = sqlalchemy.TIMESTAMP,
                                          nullable = False,
                                          default = datetime.datetime.now)
  _changeTrackFields = ['modified_at', 'created_at'] 




