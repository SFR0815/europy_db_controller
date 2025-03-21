from __future__ import annotations

import sys, typing, enum

import sqlalchemy
from sqlalchemy import orm as sqlalchemy_orm
from sqlalchemy import schema as sqlalchemy_schema
from sqlalchemy.ext import declarative as sqlalchemy_decl

import openpyxl as pxl
from openpyxl import styles as pxl_sty 
from openpyxl.worksheet import worksheet as pxl_sht
from openpyxl.cell import cell as pxl_cell
from openpyxl.worksheet import cell_range as pxl_rng


from europy_db_controllers import _controller_base
from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base
from europy_db_controllers.xl.sheet import row_control, data_column, utils, \
                                     delete_cntr_column, col_control_data
from europy_db_controllers.xl.sheet import data_block
from europy_db_controllers.xl.validation import validation_sht as io_val
from europy_db_controllers.xl.validation import validation_column as io_val_col

if typing.TYPE_CHECKING:
  from europy_db_controllers import controller

debug_rec_count: int = 0

DC = typing.TypeVar("DC", bound=data_column.DataColumn)



class ColControl():
  _backgroundColor = pxl_sty.Color(rgb="EBD334")
  _patternFill = pxl_sty.PatternFill(start_color="EBD334", end_color="EBD334", fill_type = 'solid')
  _borderStyle = pxl_sty.Side(style='thick', color='000000')
  _border = pxl_sty.Border(left=_borderStyle, right=_borderStyle,
                           top=_borderStyle, bottom=_borderStyle,
                           vertical=None, horizontal=None)
  _labelAlignment = pxl_sty.Alignment(horizontal = 'center') 
  _labelFont = pxl_sty.Font(size = 14, bold = True)

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Properties et al
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  @property
  def isList(self) -> bool:
    return self._isList
  @property
  def tableName(self) -> str:
    return str(self._capsuleKey)
  @property
  def label(self) -> str:
    if self._relationshipKey is None: return self.tableName
    return self._relationshipKey


  def _width(self) -> int:
    result = len(self.columns) 
    for subColControl in self.subColControls.values():
      result = result + subColControl._width()
    return result