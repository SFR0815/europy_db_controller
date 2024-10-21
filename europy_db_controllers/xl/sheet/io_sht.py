from __future__ import annotations

import sys, typing, datetime

import sqlalchemy
from sqlalchemy import schema as sqlalchemy_schema
from sqlalchemy.ext import declarative as sqlalchemy_decl

import openpyxl as pxl
from openpyxl import styles as pxl_sty 
from openpyxl.worksheet import worksheet as pxl_sht
from openpyxl.worksheet import cell_range as pxl_rng

sys.path.insert(0, '..\..\..')

from europy_db_controllers import _controller_base
from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils

from europy_db_controllers.xl.sheet import col_control, row_control, data_column, utils
from europy_db_controllers.xl.validation import validation_sht as io_val

if typing.TYPE_CHECKING:
  from europy_db_controllers.xl import io_wkb

CT = typing.TypeVar("CT", bound=_capsule_base.CapsuleBase)

class IoWorkSheet():
  def __init__(self,
               subControllerKey: _controller_base.ControllerKeyEnum,
               capsuleType: type[CT],
               validations: io_val.ValidationSheet
               # FIXME pass list of validations to be treated in wkb here
               ) -> None:
    self.subControllerKey = subControllerKey
    self.validations = validations
    self._capsuleType: type[CT] = capsuleType
    self._sqlalchemyType: sqlalchemy_decl.DeclarativeMeta = self._capsuleType.sqlalchemyTableType
    self._table: sqlalchemy_schema.Table = self._sqlalchemyType.__table__
    self._capsuleKey = self._capsuleType._key()    

    self.rowControl: row_control.RowControl = row_control.RowControl()
    self.colControl: col_control.ColControl = col_control.ColControl(
                  subControllerKey = subControllerKey,
                  capsuleType = self._capsuleType,
                  rowControl = self.rowControl,
                  validations=self.validations)
    self.rowControl.colControl = self.colControl

    self.wkb: pxl.Workbook = None
    self.sht: pxl_sht.Worksheet = None
    self.capsulesDict: typing.Dict = None

  @property
  def name(self):
    return self._capsuleKey

  def setup(self,
            wkb:  pxl.Workbook,
            controllerDict: typing.Dict):
    self.wkb = wkb
    self.capsulesDict = controllerDict[self.subControllerKey.value][self._capsuleKey]
    if utils.hasSheetOfName(wkb = self.wkb,
                            shtName = self.name):
      raise Exception(f"Duplicate definition of sheet with name '{self.name}'.")
    self.wkb.create_sheet(self.name)
    self.sht = self.wkb[self.name]
    self.colControl.setupLabels(sht=self.sht)
    capsuleCount = 0
   
    while capsuleCount in self.capsulesDict:
      capsuleDict = self.capsulesDict[capsuleCount]
      self.colControl.writeValues(colControlDict = capsuleDict,
                                  dataBlock = self.rowControl.dataList.nextListElement())
      capsuleCount += 1
    self.colControl.setFormatsAndValidations(sht = self.sht)


  def getDataRowDelimiters(self,
                                 row: int) -> typing.Dict[str, int]:
    result = dict[str, int]()
    result['min_row'] = row
    result['max_row'] = row
    result['min_col'] = self.colControl.firstCol
    result['max_col'] = self.colControl._width()
    return result
  def isEmptyRow(self,
                 row: int) -> bool:
    for cellTuple in self.sht.iter_cols(**self.getDataRowDelimiters(row = row)):
      if cellTuple[0].value is not None: return False
    return True
  @property
  def firstEmptyRow(self):
    for row in range(self.rowControl.firstDataRow, self.rowControl.lastDataRow + 1):
      if self.isEmptyRow(row = row):
        return row

  def identify(self,
               wkb:  pxl.Workbook):
    try:
      self.sht = wkb[self.name]
    except:
      raise Exception(f"Can't identify ioWorksheet on ioWorkbook: {self.name}")
    self.colControl.identify(sht = self.sht)
    self._identifyData()
    # print(f"Worksheet '{self.sht.title}' identified at {datetime.datetime.now()}")

  def _identifyData(self) -> None:
    mainDataList = self.rowControl.dataList
    # firstEmptyRow used as Flag for reading mode
    #     passed through all sub-DataBlocks
    mainDataList.firstEmptyRow = self.firstEmptyRow 
    self.colControl._identifyData(dataBlock=mainDataList)

  def toDict(self) -> typing.Dict[str, dict]:
    result: typing.Dict[str, dict] = dict[str, dict]()
    result[self.name] = {}
    nameDict = result[self.name]
    mainDataList = self.rowControl.dataList
    for entryCount in range(0, len(mainDataList.listElements)):
      dataEntry = mainDataList.listElements[entryCount]
      entryDict = self.colControl.toDict(dataEntry=dataEntry)
      nameDict[entryCount] = entryDict
    deleteDict = mainDataList.getDeleteDict()
    for key, value in deleteDict.items():
      result[key] = value    
    # print(f"Worksheet '{self.sht.title}' converted to dict at {datetime.datetime.now()}")
    return result  


    
    

    

    

