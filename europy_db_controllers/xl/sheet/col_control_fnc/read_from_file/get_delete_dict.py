import typing

from europy_db_controllers.xl.sheet import data_block
from europy_db_controllers.xl.sheet import col_control_data 

def getDeleteDictFnc(self,
                    dataEntry: data_block.DataBlock) -> typing.Dict[str, list]:   
  result: typing.Dict[str, list] = dict[str, list]()
  def __ensureListInResult(colBlockName: str) -> None:
    if not colBlockName in result:
      result[colBlockName] = []
  def deleteIfMarked(colControlData: col_control_data.ColControlData,
                      colBlockName: str):
    if colControlData.hasDeleteMarker:
      if colControlData.hasId:
        __ensureListInResult(colBlockName=colBlockName)
        result[colBlockName].append(colControlData.getId())
      return True
    return False
        
    
  if dataEntry.isList:
    colBlockName = dataEntry.colBlockName
    for listElement in dataEntry.listElements:
      colControlData = listElement.colControlData
      deleted = deleteIfMarked(colControlData = colControlData,
                                colBlockName = colBlockName)
      # no further delete required - cascade delete must be defined on relationships
      if not deleted:
        subResult = listElement.getDeleteDict()
        # iterate the subBlocks
        pass
  else:
    colControlData = dataEntry.colControlData
    deleted = deleteIfMarked(colControlData = colControlData,
                              colBlockName = colBlockName)
    # no further delete required - cascade delete must be defined on relationships
  return result