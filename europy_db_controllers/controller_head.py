from __future__ import annotations

import sys, typing, datetime, enum, os, re
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from europy_db_controllers.entity_capsules import  _capsule_utils
from europy_db_controllers import _controller_base, _controller_attr
from europy_db_controllers.xl import io_wkb 


#{'upload': {'type': 'asset', 'data': transaction_type}}
class Actions(enum.Enum):
  GET_INFO = 'get_info'
  UPLOAD = 'upload'
  EDIT = 'edit'

class ControllerHeadBase():
  @property
  def isLoaded(self) -> bool:
    return not self.action is None

class Gui(ControllerHeadBase):
  def __init__(self):
    self.action = None #action to be performed
    self.type = None # sqlalchemy table type or control\capsule type
    self.data = None #dictionary
  def fromDict(self, uiDataHeader: dict[str, dict[str, any]]):
    # data directly via dict


    # get the action to be performed  with the json data
    for key, val in uiDataHeader.items():
      self.action = key
    # get the datatype and data for the (controllerType) to be created
    actionData = uiDataHeader[self.action]
    #actionData[type] -> e.g: transaction_type, asset, asset_class
    self.type = actionData['type']
    # name and id (if exist) corresponding to the entity in the body of the json
    self.data = actionData['data']
  def getCapsuleClassName(self) -> str:
    baseName = _capsule_utils.getBaseNameFromString(name = self.type) 
    capsuleClassName = _capsule_utils.getCapsuleClassNameFromBaseName(baseName = baseName)
    return capsuleClassName
  # @classmethod
  # def fromDict(specDict: dict) -> Gui:
  #   print(specDict)
  #   if len(specDict) == 0:
  #     return Gui()
  #   else:
  #     return Gui(action = specDict['subControllerKey'],
  #                type = specDict['type'],
  #                data = specDict['data'])  
  def toDict(self) -> dict:
    result = {}
    result['action'] = self.action
    result['type'] = self.type
    result['data'] = self.data
 

class Excel(ControllerHeadBase):
  def __init__(self, 
               subControllerKey: _controller_base.ControllerKeyEnum = None,
               action: _controller_base.XlControllerAction = None,
               filePath: str = None,
               fileName: str = None,
               mostRecentFileName: str = None,
               scope: _controller_base.ControllerDataScopes = \
                      _controller_base.ControllerDataScopes.STORED_ON_DB) -> None:
    self.subControllerKey: _controller_base.ControllerKeyEnum = subControllerKey
    self.filePath: str = filePath
    self.fileName: str = fileName
    self.mostRecentFileName: str = mostRecentFileName
    self.scope: _controller_base.ControllerDataScopes = scope
    self.action: _controller_base.XlControllerAction = action
    self.ioWkb: io_wkb.IoWorkbook = None
    if self.action == _controller_base.XlControllerAction.UPLOAD:
      self._identifySubControllerKey()
  def ensureConsistency(self):
    if self.subControllerKey == _controller_base.ControllerKeyEnum.BASIC_SPECIFICATION \
          and not self.action == _controller_base.XlControllerAction.DOWNLOAD:
      raise Exception("Excel controller inconsistent: Basic specifications are available for download only.")
    # if self.subControllerKey == _controller_base.ControllerKeyEnum.ASSET_CLASSIFICATION \
    #       and self.action == _controller_base.XlControllerAction.GET_INPUT_TEMPLATE:
    #   raise Exception("Excel controller inconsistent: asset classification is applicable across all clients.\n" + \
    #                   "No blank input template available.")
  @classmethod
  def fromDict(specDict: dict) -> Excel:
    if len(specDict) == 0:
      return Excel()
    else:
      return Excel(subControllerKey = specDict['subControllerKey'],
                   action = specDict['subControllerKey'],
                   filePath = specDict['filePath'],
                   scope = specDict['scope'])  
  def toDict(self) -> dict:
    result = {}
    result['subControllerKey'] = self.subControllerKey
    result['filePath'] = self.filePath
    result['scope'] = self.scope
    result['action'] = self.action
  @property
  def downloadFilePath(self):
    if self.fileName is None:
      fileName = datetime.datetime.now().strftime("%y%m%d_%H%M%S_") + \
                self.subControllerKey.value + "_" + \
                "data.xlsx"
    else:
      fileName = self.fileName
    if not fileName.endswith(".xlsx"): fileName += ".xlsx"
    self.mostRecentFileName = fileName
    return os.path.join(self.filePath, fileName)
  @property
  def mostRecentDownloadFilePath(self):
    fileName = self.mostRecentFileName
    self.mostRecentFileName = fileName
    return os.path.join(self.filePath, fileName)
  
  def _getUploadFilePath(self) -> str:
    return os.path.join(self.filePath, self.fileName)
  @property
  def uploadFilePath(self) -> str:
    self._identifySubControllerKey()
    return self._getUploadFilePath()

    return os.path.join(self.filePath, self.fileName)
  def _identifySubControllerKey(self) -> None:
    pattern = r"[\\\/]\d{6}_\d{6}_(.*?)_data\.xlsx"
    match = re.search(pattern, self._getUploadFilePath())
    if not match:
      raise Exception(f"Could not identify upload key from filepath: \n{self._getUploadFilePath()}\n" + \
                      f"File name must have the format 'YYMMDD_hhmmss_<controller_key>_data.xlsx'")
    identifiedSubControllerKey: _controller_base.ControllerKeyEnum = None
    try: 
      identifiedSubControllerKey = _controller_base.ControllerKeyEnum.from_str(label = match.group(1))
    except:
      raise Exception(f"Irregular subControllerKey identified in upload file path.\n" + \
                      f"subControllerKey: {match.group(1)}\n" + \
                      f"in file path: {self._getUploadFilePath()}\n" + \
                      f"File name must have the format 'YYMMDD_hhmmss_<controller_key>_data.xlsx'")
    if not self.subControllerKey is None:
      if not self.subControllerKey == identifiedSubControllerKey:
        raise Exception(f"Inconsistent definition of subControllerKey in Excel controller head.\m" + \
                        f"Subcontroller key specified upon head specification: {self.subControllerKey}\n" + \
                        f"Subcontroller identified on upload file name       : {identifiedSubControllerKey}\n" + \
                        f"Upload file name                                          : {self.fileName}\n")
    self.subControllerKey = identifiedSubControllerKey


class ControllerHead():
  def __init__(self):
    self.gui = Gui()
    self.excel = Excel()
  @property
  def isLoaded(self):
    return self.gui.isLoaded or self.excel.isLoaded
  @property
  def isAmbiguous(self):
    return self.gui.isLoaded and self.excel.isLoaded
  @classmethod
  def fromDict(specDict: dict) -> Excel:
    output = ControllerHead()
    if 'excel' in specDict:
      output.excel = Excel.fromDict(specDict = specDict['excel'])
    if 'gui' in specDict:
      output.gui = Gui.fromDict(specDict = specDict['gui'])
  def toDict(self) -> dict:
    result = {}
    result['excel'] = self.excel.toDict()
    result['gui'] = self.gui.toDict()
