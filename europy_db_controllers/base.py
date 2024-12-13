from __future__ import annotations
import datetime, typing
import sqlalchemy
from sqlalchemy import orm as sqlalchemy_orm


PARENT_ATTRIBUTE_TO_DELETE_IF_NO_CHILD = "parent_attribute"
CHILDREN_LIST_ATTRIBUTE_OF_PARENT_TO_DELETE_IF_NO_CHILD = "children_list_attribute"


class Base(sqlalchemy_orm.DeclarativeBase):
  # source: https://medium.com/@alanhamlett/part-1-sqlalchemy-models-to-json-de398bc2ef47
  __abstract__ = True
  _exclude_from_json: typing.List[str] = []
  _display_lists: typing.List[str] = []
  _is_part_of_list_of: typing.List[str] = [] 
  _show_id_of: typing.List[str] = []
  _sorted_by: typing.List[str] = None 
  # pls provide description
  _hyb_props_replacing_columns: typing.Dict[str, str] = {}
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # Dictionary of attributes of parent entities that will be deleted upon deletion of 
  #       the entity if the entity is the parent's last child
  #       E.g.: A parent has a list of children. There is just one child present.
  #             If this only child is deleted, the parent is deleted as well.
  #       Usage: (a) In case of a lot - if there is only one (opening) transaction is defined,
  #              the deletion of such transaction makes the lot itself obsolete.
  #              (b) There are assets defined and projects using these assets. The usage of 
  #              the assets by a project is documented in a separate project asset table.
  #              There may be 'n' transactions in a project on some action. If there is only
  #              one transaction of some asset on a project, the deletion of such transaction 
  #              results in the project asset being obsolete as the project doesn't use it
  #              any longer.
  #       Definition standard: 
  #              keys: consecutive integer numbers (first missing integer in line interrupts the application)
  #                    The first key is '0' by standard.
  #              values: Dictionary of two string values - 
  #                         [PARENT_ATTRIBUTE_TO_DELETE_IF_NO_CHILD] the name of the attribute of the parent 
  #                                                                  within the class of the children
  #                         [CHILDREN_LIST_ATTRIBUTE_OF_PARENT_TO_DELETE_IF_NO_CHILD] the name of the attribute
  #                                                                  of the list of children within the class
  #                                                                  of the parents
  #              comments: - The defined parent/children relationships are worked off by the position of the 
  #                          integer key of their entry. This ensures that those relationships are removed first,
  #                          that could cause integrity issues when deleting others.
  #      Example:
  #             _parents_to_delete_if_no_child = {0: {base.PARENT_ATTRIBUTE_TO_DELETE_IF_NO_CHILD: "project_asset_position",
  #                                                   base.CHILDREN_LIST_ATTRIBUTE_OF_PARENT_TO_DELETE_IF_NO_CHILD: "market_transactions"},
  #                                               1: {base.PARENT_ATTRIBUTE_TO_DELETE_IF_NO_CHILD: "project_asset",
  #                                                   base.CHILDREN_LIST_ATTRIBUTE_OF_PARENT_TO_DELETE_IF_NO_CHILD: "market_transactions"}}
  #      Relevant code: see method 'deleteFromDb' as specified on class 'CapsuleBase' in module 'entity_capsules._capsule_base'.
  _parents_to_delete_if_no_child: typing.Dict[int, typing.Dict[str, str]] = {}


  modified_at: sqlalchemy_orm.Mapped[datetime.date] = sqlalchemy_orm.mapped_column(
                                          type_ = sqlalchemy.TIMESTAMP,
                                          nullable = False,
                                          default = datetime.datetime.now)
  created_at: sqlalchemy_orm.Mapped[datetime.datetime] = sqlalchemy_orm.mapped_column(
                                          type_ = sqlalchemy.TIMESTAMP,
                                          nullable = False,
                                          default = datetime.datetime.now)
  _changeTrackFields = ['modified_at', 'created_at'] 




