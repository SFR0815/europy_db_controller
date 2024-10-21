import typing

from openpyxl.worksheet import worksheet as pxl_sht

from europy_db_controllers.xl.sheet import delete_cntr_column

class ColControlData():
  def __init__(self,
               colControlLabel: str,
               sht: pxl_sht.Worksheet,
               row: int,
               valueDict: typing.Dict[str, any]) -> None:
    self.colControlLabel = colControlLabel
    self.sht = sht
    self.row = row
    self.valueDict = valueDict
    # for key, value in self.valueDict.items():
    #   print(f"ColControlData {self.colControlLabel:<30} - key {key} - value: {value}")

  @property
  def hasNameAttribute(self) -> bool:
    return 'name' in self.valueDict
  @property
  def hasDeleteControlAttribute(self) -> bool:
    return delete_cntr_column.DELETE_CONTROL_LABEL in self.valueDict
  @property
  def hasId(self) -> bool:
    return not self.valueDict['id'] is None
  @property
  def hasRequiredName(self) -> bool:
    if not self.hasNameAttribute: return True
    return not self.valueDict['name'] is None
  @property
  def hasDeleteMarker(self) -> bool:
    if self.hasDeleteControlAttribute: return False
    return not self.valueDict[delete_cntr_column.DELETE_CONTROL_LABEL] is None
  @property
  def isEmpty(self) -> bool:
    for key, value in self.valueDict.items():
      if not value is None: return False
    return True
  @property
  def hasValues(self) -> bool:
    for key, value in self.valueDict.items():
      if key != delete_cntr_column.DELETE_CONTROL_LABEL:
        if not value is None: return True
    return False

  def deleteSubDict(self):
    return self.valueDict['id']
  def hasKey(self, key: str) -> any:
    return key in self.valueDict
  def getValue(self, key: str) -> any:
    return self.valueDict[key]
  

  def ensureConsistency(self) -> bool:
    if self.hasDeleteControlAttribute:
      if self.hasDeleteMarker and not self.hasValues:
        raise Exception("Inconsistent delete without any data detected.\n" + \
                        f"Sheet  : {self.sht.title}\n" + \
                        f"Section: {self.colControlLabel}\n" + \
                        f"row    : {self.row}\n")
    if not (self.isEmpty or self.hasRequiredName):  
        raise Exception("Name missing.\n" + \
                        f"Sheet  : {self.sht.title}\n" + \
                        f"Section: {self.colControlLabel}\n" + \
                        f"row    : {self.row}\n")
  def ensureEmpty(self) -> bool:
    if not self.isEmpty:
      raise Exception("Inconsistent non-list sub-DataBlock.\n" + \
                      f"Sheet  : {self.sht.title}\n" + \
                      f"Section: {self.colControlLabel}\n" + \
                      f"row    : {self.row}\n")
