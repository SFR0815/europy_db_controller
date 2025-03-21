from openpyxl.worksheet import worksheet as pxl_sht

def setupLabelRangeFnc(self) -> None:
  labelCell: pxl_sht.Cell = self.labelCell
  labelCell.alignment = self._labelAlignment
  labelCell.font = self._labelFont
  labelCell.fill = self._patternFill
  labelCell.value = self.tableName \
                    if self._parentColControl is None \
                    else self._relationshipKey
  self.sht.merge_cells(self.labelRangeAddress)