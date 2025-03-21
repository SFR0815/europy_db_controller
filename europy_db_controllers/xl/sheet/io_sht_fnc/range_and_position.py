import typing

def isEmptyRowFnc(self,
                 row: int) -> bool:
    for cellTuple in self.sht.iter_cols(**self.getDataRowDelimiters(row = row)):
      if cellTuple[0].value is not None: return False
    return True

def firstEmptyRowFnc(self):
    for row in range(self.rowControl.firstDataRow, self.rowControl.lastDataRow + 1):
        if self.isEmptyRow(row = row):
        # the first empty row
            return row
        elif row == self.rowControl.lastDataRow and not self.isEmptyRow(row = row): 
            # the first empty row is the row after the last data row (all data rows populated)
            return row + 1

def getDataRowDelimitersFnc(self,
                         row: int) -> typing.Dict[str, int]:
    result = dict[str, int]()
    result['min_row'] = row
    result['max_row'] = row
    result['min_col'] = self.colControl.firstCol
    result['max_col'] = self.colControl._width()
    return result