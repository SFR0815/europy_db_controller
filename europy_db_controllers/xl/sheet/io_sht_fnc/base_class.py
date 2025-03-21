from __future__ import annotations

import sys, typing, enum

from sqlalchemy import schema as sqlalchemy_schema
from sqlalchemy.ext import declarative as sqlalchemy_decl

import openpyxl as pxl
from openpyxl.worksheet import worksheet as pxl_sht

from europy_db_controllers import _controller_base
from europy_db_controllers.entity_capsules import _capsule_base

from europy_db_controllers.xl.sheet import col_control, row_control
from europy_db_controllers.xl.validation import validation_sht as io_val


CT = typing.TypeVar("CT", bound=_capsule_base.CapsuleBase)

class IoWorkSheet():
  def __init__(self,
               subControllerKey: _controller_base.ControllerKeyEnum,
               controllerKeyEnum: enum.Enum,
               capsuleType: type[CT],
               capsuleList: list[type[CT]],
               validations: io_val.ValidationSheet
               # FIXME pass list of validations to be treated in wkb here
               ) -> None:
    self.subControllerKey = subControllerKey
    self.controllerKeyEnum = controllerKeyEnum
    self.validations = validations
    self._capsuleType: type[CT] = capsuleType
    self._capsuleList: list[type[CT]] = capsuleList
    self._sqlalchemyType: sqlalchemy_decl.DeclarativeMeta = self._capsuleType.sqlalchemyTableType
    self._table: sqlalchemy_schema.Table = self._sqlalchemyType.__table__
    self._capsuleKey = self._capsuleType._key()    

    self.rowControl: row_control.RowControl = row_control.RowControl()
    self.colControl: col_control.ColControl = col_control.ColControl(
                  subControllerKey = subControllerKey,
                  controllerKeyEnum = self.controllerKeyEnum,
                  capsuleType = self._capsuleType,
                  capsuleList = self._capsuleList,
                  rowControl = self.rowControl,
                  validations=self.validations)
    self.rowControl.colControl = self.colControl

    self.wkb: pxl.Workbook = None
    self.sht: pxl_sht.Worksheet = None
    self.capsulesDict: typing.Dict = None
