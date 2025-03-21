from openpyxl.worksheet import worksheet as pxl_sht

from europy_db_controllers.xl.sheet.col_control_fnc.debug_cntr import debug_control

def setFormatsAndValidationsFnc(self,
                                sht: pxl_sht.Worksheet):
  # FIXME: set cell formats
  self.sht = sht
  for dataColumn in self.columns.values():
    dataColumn.setFormatsAndValidations(sht = self.sht)
  for subColControl in self.subColControls.values():
    subColControl.setFormatsAndValidations(sht = self.sht)