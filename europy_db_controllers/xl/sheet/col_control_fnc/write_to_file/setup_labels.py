from openpyxl.worksheet import worksheet as pxl_sht

def setupLabelsFnc(self,
                   sht: pxl_sht.Worksheet) -> None:
  self.sht = sht
  self._setupFullRange()
  self._setupLabelRange()
  for dataColumn in self.columns.values():
    dataColumn.setupLabel(sht = self.sht)
  for subColControl in self.subColControls.values():
    subColControl.setupLabels(sht = self.sht)