
import sys, typing

import openpyxl as pxl
from openpyxl import styles as pxl_sty 
from openpyxl.worksheet import cell_range as pxl_rng
from openpyxl.cell import cell as pxl_cell
from openpyxl.worksheet import worksheet as pxl_sht

sys.path.insert(0, '..\..\..')

from europy_db_controllers.xl.validation import validation_column as io_val_col

from europy_db_controllers import _controller_base
from europy_db_controllers.entity_capsules import _capsule_base

CT = typing.TypeVar("CT", bound=_capsule_base.CapsuleBase)

class ValidationSheet():
  _labelRow = 1
  _label = "Validations"
  def __init__(self,
               subControllerKey: _controller_base.ControllerKeyEnum,
               validationLocators: typing.List[tuple[str, str]]) -> None:
    self.subControllerKey = subControllerKey
    self.validationLocators = validationLocators
    self.validationColumns: typing.Dict[str, io_val_col.ValidationColumn] = \
                            dict[str, io_val_col.ValidationColumn]()

    self.wkb: pxl.Workbook = None
    self.sht: pxl_sht = None

    for validationLocator in validationLocators:
      self.__addValidationColumn(validationLocator)
  
  def getValidationOfCapsuleKey(self,
                                capsuleKey: str) -> io_val_col.ValidationColumn:
    for validationLocator in self.validationLocators:
      if validationLocator[1] == capsuleKey:
        return self.getValidationColumn(validationLocator=validationLocator)


  def hasValidation(self):
    return len(self.validationColumns) > 0
  def hasValidationColumn(self, 
                          validationLocator: typing.Tuple[str, str]
                          ) -> bool:
    if not self.hasValidation: return False
    columnLabel = io_val_col.ValidationColumn.getColumnLabel(
                      validationLocator = validationLocator)
    return columnLabel in self.validation  

  def _getMaxColumnNumber(self):
    result = 0
    for validationColumn in self.validationColumns.values():
      validationColumnNumber = validationColumn.columnNumber
      if validationColumnNumber > result: 
        result = validationColumnNumber
    return result

  def __addValidationColumn(self,
                           validationLocator: typing.Tuple[str, str]
                           ) -> None:
    validationSubControllerKey = validationLocator[0]
    validationColumnNumber: int = -1
    if validationSubControllerKey != self.subControllerKey.value:
      validationColumnNumber = self._getMaxColumnNumber() + 2
    validationColumn = io_val_col.ValidationColumn(
                            columnNumber=validationColumnNumber,
                            subControllerKey=self.subControllerKey,
                            validationLocator=validationLocator)
    self.validationColumns[validationColumn.label] = validationColumn
    

  def getValidationColumn(self,
                          validationLocator: typing.Tuple[str, str]
                          ) -> None:
    label = io_val_col.ValidationColumn.getColumnLabel(
                      validationLocator = validationLocator)
    return self.validationColumns[label]

  def setup(self,
            wkb: pxl.Workbook,
            controllerDict: typing.Dict) -> None:
    self.wkb = wkb
    self.wkb.create_sheet(self._label)
    self.sht = self.wkb[self._label]
    for validationColumn in self.validationColumns.values():
      if validationColumn.isInternalValidation: 
        validationColumn.wkb = self.wkb
      else:  
        subControllerId = validationColumn.validationSubControllerId
        capsuleId = validationColumn.validationCapsuleId
        validationDict = controllerDict[subControllerId][capsuleId]
        validationColumn.setup(validationDict=validationDict,
                              wkb = self.wkb,
                              sht = self.sht)