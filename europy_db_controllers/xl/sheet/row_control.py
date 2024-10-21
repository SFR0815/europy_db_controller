from __future__ import annotations

import sys, typing

from openpyxl.worksheet import cell_range as pxl_rng

sys.path.insert(0, '..\..\..')

from europy_db_controllers.xl.sheet import data_block

if typing.TYPE_CHECKING:
  from europy_db_controllers.xl.sheet import col_control

class RowControl():
  def __init__(self) -> None:
    self.columnLableRow = 0
    self.lastDataRow = 0
    self._colControl: col_control.ColControl = None
    self.dataList: data_block.DataBlock = None

  @property
  def colControl(self) -> col_control.ColControl:
    return self._colControl
  @colControl.setter
  def colControl(self, colControl: col_control.ColControl) -> None:
    self._colControl = colControl
    self.dataList: data_block.DataBlock = data_block.DataBlock(
                                        dataRow = self.nextDataRow,
                                        isList=True,
                                        minCol=self._colControl.firstCol,
                                        maxCol = self._colControl.lastCol,
                                        colBlockName = self._colControl.label,
                                        colBlockTableName = self._colControl.tableName)
  @property
  def firstDataRow(self) -> int:
    return self.columnLableRow + 1
  @property 
  def nextDataRow(self) -> int:
    if self.lastDataRow == 0:
      return self.firstDataRow
    else:
      return  self.lastDataRow + 1

  def updateDataRow(self, dataRow) -> None:
    if dataRow > self.lastDataRow:
      self.lastDataRow = dataRow



  def updateColumnLabelRow(self, columnLableRow) -> None:
    if columnLableRow > self.columnLableRow:
      self.columnLableRow = columnLableRow