from __future__ import annotations

import typing

from openpyxl.worksheet import cell_range as pxl_rng
from europy_db_controllers.xl.sheet import col_control_data

class DataBlock():
  def __init__(self,
               dataRow: int,
               isList: bool,
               minCol: int,
               maxCol: int,
               colBlockName: str,
               colBlockTableName: str,
               parent: DataBlock = None,
               previousSibling: DataBlock = None,
               firstEmptyRow: int = None) -> None:
    self.dataRow = dataRow # different treatment for read&write, see below
    self.firstEmptyRow = firstEmptyRow # used for reading data
    self.minCol = minCol
    self.maxCol = maxCol
    self.colBlockName = colBlockName
    self.colBlockTableName = colBlockTableName
    self.parent = parent
    self.previousSibling: DataBlock = previousSibling
    self.nextSibling: DataBlock = None
    self.listElements: typing.List[DataBlock] = list[DataBlock]() if isList \
                                                                  else None
    self.subBlocks: typing.List[DataBlock] = list[DataBlock]() if not isList \
                                                                 else None
    self.colControlData: col_control_data.ColControlData = None

  @property
  def isList(self):
    return not self.listElements is None
  @property
  def numberOfListChildren(self):
    if self.isList: return len(self.listElements)
    else: return 0
  @property
  def maxRow(self):
    result = self.dataRow
    if self.firstEmptyRow is None: # Flag for writing data
      if self.isList:
        for listElement in self.listElements:
          if listElement.maxRow > result:
            result = listElement.maxRow
      else:
        for subBlock in self.subBlocks:
          if subBlock.maxRow > result:
            result = subBlock.maxRow
      return result
    else: # case of reading data
      if self.parent is None: # self is main data list
        return self.firstEmptyRow - 1
      if self.parent.isList:
        if self.nextSibling is None:
          return self.parent.maxRow
        else:
          return self.nextSibling.dataRow - 1
      else:
        if self.isList:
          return self.parent.maxRow
        else:
          return self.dataRow
  @property  
  def rangeDelimiters(self) -> typing.Dict[str, int]:
    result = dict[str, int]()
    result['min_row'] = self.dataRow 
    result['max_row'] = self.maxRow
    result['min_col'] = self.minCol
    result['max_col'] = self.maxCol
    return result
  @property
  def cellRange(self) -> pxl_rng.CellRange:
    return pxl_rng.CellRange(**self.rangeDelimiters)

  def nextListElement(self,
                      dataRow: int = None,
                      firstEmptyRow: int = None) -> DataBlock:
    #self.firstEmptyRow = firstEmptyRow
    if not self.isList:
      raise Exception("Can't add list element to data block that is not a list.")
    if firstEmptyRow is None: # used for reading data
      if dataRow is None:
        dataRow = self.dataRow
        if self.numberOfListChildren > 0:
          dataRow = self.maxRow + 1
    else:
      if dataRow is None:
        raise Exception(f"'dataRow' parameter must be provided if data is to be read from spreadsheet.")  
    #   dataRow = self.dataRow
    #   previousSibling = None
    #   if self.numberOfListChildren > 0:
    #     dataRow = self.maxRow + 1
    # else: # used for reading data
    previousSibling: DataBlock = None
    if self.numberOfListChildren > 0:
      previousSibling = self.listElements[self.numberOfListChildren - 1]
    
    minCol = self.minCol
    maxCol = self.maxCol
    colBlockName = self.colBlockName
    # List elements can't be lists themselves
    child = DataBlock(dataRow=dataRow,
                      isList = False,
                      minCol = minCol,
                      maxCol = maxCol,
                      colBlockName = colBlockName,
                      colBlockTableName = self.colBlockTableName,
                      parent = self,
                      previousSibling = previousSibling,
                      firstEmptyRow = self.firstEmptyRow)
    if self.numberOfListChildren > 0:
      self.listElements[self.numberOfListChildren - 1].nextSibling = child
    self.listElements.append(child)
    return child
  
  def nextSubBlock(self,
                   isList: bool,
                   minCol: int,
                   maxCol: int,
                   colBlockName: str,
                   colBlockTableName: str) -> DataBlock:
    subBlock = DataBlock(dataRow = self.dataRow,
                         isList = isList,
                         minCol = minCol,
                         maxCol = maxCol,
                         colBlockName = colBlockName,
                         colBlockTableName = colBlockTableName,
                         parent = self,
                         firstEmptyRow = self.firstEmptyRow)
    self.subBlocks.append(subBlock)
    return subBlock
  
  def getSubBlockOfName(self,
                        colBlockName: str) -> DataBlock:
    for subBlock in self.subBlocks:
      if subBlock.colBlockName == colBlockName:
        return subBlock
    raise Exception(f"No subBlock with name {colBlockName}")
    
  def getDeleteDict(self) -> typing.Dict[str, dict]:   
    result: typing.Dict[str, dict] = dict[str, dict]()
    #######################
    def __getDeleteDictKey(colBlockTableName: str) -> str:
      return colBlockTableName + '_delete'
    def __ensureDictInResult(colBlockTableName: str) -> None:
      deleteSubDictName = __getDeleteDictKey(colBlockTableName)
      if not deleteSubDictName in result:
        result[deleteSubDictName] = {}
    def __addToDeleteDict(colBlockTableName: str,
                          value: any) -> None:
      deleteSubDictName = __getDeleteDictKey(colBlockTableName)
      deleteSubDict = result[deleteSubDictName]
      deleteSubDictLen = len(deleteSubDict)
      deleteSubDict[deleteSubDictLen] = value
    def __deleteIfMarked(colControlData: col_control_data.ColControlData,
                         colBlockTableName: str):
      if colControlData.hasDeleteMarker:
        if colControlData.hasId:
          __addToDeleteDict(colBlockTableName = colBlockTableName,
                            value = colControlData.getId())

        return True
      return False
    def __addDeleteDict(key: str,
                        deleteDict: dict[int, any]) -> None:
      if not key in result:
        result[key] = {}
      for deleteItem in deleteDict.values():
        __addToDeleteDict(colBlockTableName=colBlockTableName,
                          value=deleteItem)
    #######################
    colBlockTableName = self.colBlockTableName
    __ensureDictInResult(colBlockTableName = colBlockTableName)
    if self.isList:
      for listElement in self.listElements:
        colControlData = listElement.colControlData
        deleted = __deleteIfMarked(colControlData = colControlData,
                                   colBlockTableName = colBlockTableName)
        # no further delete required - cascade delete must be defined on relationships
        if not deleted:
          subResult = listElement.getDeleteDict()
          for key, deleteDict in subResult.items():
            __addDeleteDict(key = key,
                            deleteDict = deleteDict)
    else:
      colControlData = self.colControlData
      deleted = __deleteIfMarked(colControlData = colControlData,
                                 colBlockTableName = colBlockTableName)
      # no further delete required - cascade delete must be defined on relationships
      if not deleted:
        for subBlock in self.subBlocks:
          subResult = subBlock.getDeleteDict()
          for key, deleteDict in subResult.items():
            __addDeleteDict(key = key,
                            deleteDict = deleteDict)
    return result
  