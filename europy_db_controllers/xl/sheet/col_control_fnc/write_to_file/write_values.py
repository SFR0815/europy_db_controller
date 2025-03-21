import typing

from europy_db_controllers.xl.sheet import data_block
from europy_db_controllers.xl.sheet import delete_cntr_column

from europy_db_controllers.xl.sheet.col_control_fnc.debug_cntr import debug_control

def writeValuesFnc(self,
                   colControlDict: typing.Dict,
                   dataBlock: data_block.DataBlock):
  for dataColumn in self.columns.values():
    if isinstance(dataColumn, delete_cntr_column.DeleteControlColumn): continue 
    cellValue = colControlDict[dataColumn.label]
    dataColumn.writeValue(cellValue = cellValue,
                          dataBlock=dataBlock)
  for subColControl in self.subColControls.values():
    subDataBlock = dataBlock.nextSubBlock(isList=subColControl.isList,
                                        minCol=subColControl.firstCol,
                                        maxCol=subColControl.lastCol,
                                        colBlockName=subColControl.label,
                                        colBlockTableName=subColControl.tableName)
    
    if not subColControl._relationshipKey in colControlDict: continue
    thisColControlDict = colControlDict[subColControl._relationshipKey]
    if thisColControlDict is None: continue
    if len(thisColControlDict) == 0: continue
    if subColControl.isList:
      listCount: int = 0
      while listCount in thisColControlDict:
        thisDataBlock = subDataBlock.nextListElement()
        thisColControlItemDict = thisColControlDict[listCount]
        subColControl.writeValues(colControlDict = thisColControlItemDict,
                                  dataBlock=thisDataBlock)
        listCount += 1
    else:
      subColControl.writeValues(colControlDict = thisColControlDict,
                                dataBlock=subDataBlock)
  self._rowControl.updateDataRow(dataRow = dataBlock.maxRow)