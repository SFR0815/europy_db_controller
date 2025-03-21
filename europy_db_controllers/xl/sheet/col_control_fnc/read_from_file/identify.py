from openpyxl.worksheet import worksheet as pxl_sht

def identifyFnc(self,
                sht: pxl_sht.Worksheet) -> None:
  self.sht = sht
  for dataColumn in self.columns.values():
    dataColumn.identify(sht = self.sht)
  for subColControl in self.subColControls.values():
    subColControl.identify(sht = self.sht)