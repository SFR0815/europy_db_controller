import typing

from openpyxl.cell import cell as pxl_cell
from openpyxl.worksheet import cell_range as pxl_rng

from europy_db_controllers.xl.sheet import utils
def firstRowFnc(self) -> int:
  return self.__getHeadDepth()

def lastRowFnc(self) -> int:
  return self._rowControl.columnLableRow - 1

def lastColFnc(self) -> int:
  return self.firstCol + self._width() - 1

def labelCellAddressFnc(self) -> str:
  return utils.getCellAddress(
                  row = self.firstRow, 
                  col = self.firstCol)

def labelRangeAddressFnc(self) -> str:
  return utils.getRangeAddress(
                  luRow = self.firstRow, luCol = self.firstCol,
                  rlRow = self.firstRow, rlCol = self.lastCol)

def labelCellFnc(self) -> pxl_cell.Cell:
  return self.sht.cell(row = self.firstRow, column = self.firstCol)

def fullRangeFnc(self) -> pxl_rng.CellRange:
  return pxl_rng.CellRange(title=self.sht.title, 
                            min_row=self.firstRow, min_col=self.firstCol,
                            max_row=self.lastRow, max_col=self.lastCol)

def internalWidthFnc(self) -> int:
  result = len(self.columns) 
  for subColControl in self.subColControls.values():
    result = result + subColControl._width()
  return result

def getColControlRowDelimitersFnc(self,
                                row: int) -> typing.Dict[str, int]:
  result = dict[str, int]()
  result['min_row'] = row
  result['max_row'] = row
  result['min_col'] = self.firstCol
  result['max_col'] = len(self.columns) + self.firstCol - 1
  return result

