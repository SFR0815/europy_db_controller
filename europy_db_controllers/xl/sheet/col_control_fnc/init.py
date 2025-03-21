from __future__ import annotations

import typing, enum

from europy_db_controllers import _controller_base
from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base
from europy_db_controllers.xl.sheet import row_control
from europy_db_controllers.xl.validation import validation_sht as io_val

from europy_db_controllers.xl.sheet.col_control_fnc import base_class

from europy_db_controllers.xl.sheet.col_control_fnc.debug_cntr import debug_control

CT = typing.TypeVar("CT", bound=_capsule_base.CapsuleBase)

def initFnc(self,
            subControllerKey: _controller_base.BaseControllerKeyEnum,
            controllerKeyEnum: enum.Enum,
            capsuleType: type[CT],
            capsuleList: list[type[CT]],
            rowControl: row_control._rowControl,
            validations: io_val.ValidationSheet,
            firstCol: int = 1,
            parentColControl: base_class.ColControl = None,
            isList: bool = False,
            relationshipKey: str = None) -> None:
  
  self.subControllerKey = subControllerKey
  self.controllerKeyEnum = controllerKeyEnum
  self._capsuleType = capsuleType
  self._capsuleList = capsuleList
  self._sqlalchemyType = self._capsuleType.sqlalchemyTableType
  self._table = self._sqlalchemyType.__table__
  self._capsuleKey = self._capsuleType._key()    
  self._relationships = self._sqlalchemyType.__mapper__.relationships

  self.firstCol = firstCol
  self.validations = validations
  self.subColControls= {}
  self.columns = {}

  self._isList = isList
  self._rowControl = rowControl
  self._parentColControl = parentColControl
  self._relationshipKey = relationshipKey

  self.sht = None

  if self._parentColControl is None:
    debug_control.setDoDebug(capsuleType)
    # print(f"[{self._capsuleKey}.__init__] setting doDebug to {debug_control.DO_DEBUG}")

  self.__addDeleteControlColumn()
  #FIXME: update this iteration using the _capsule_utils.getCapsuleInitColumnsAndColumnLikeProperties
  sqlalchemyTableType = self._capsuleType.sqlalchemyTableType
  columnsAndAlikeInfo = _capsule_utils.getCapsuleInitColumnsAndColumnLikeProperties(
                      capsuleType = capsuleType)
  for column_name, column_info in columnsAndAlikeInfo.items():
    if column_name in self._sqlalchemyType._changeTrackFields:
      pass # internal change control only
    elif _capsule_utils.isRelationshipIdColumnName(columnName=column_name):
      self.__addSingleRelationship(columnOrAlikeInfo = column_info)
    else:
      column_of_name = sqlalchemyTableType.__table__.columns[column_name]
      self._addColumn(label = column_name,
                      validation=None,
                      unique = True if column_name == 'id' else column_of_name.unique,
                      sqlalchemyDataType = str(column_of_name.type))
  for relationship in self._relationships:
    if relationship.uselist:
      self.__addListRelationship(relationship = relationship)
  self.__updateRowControl()