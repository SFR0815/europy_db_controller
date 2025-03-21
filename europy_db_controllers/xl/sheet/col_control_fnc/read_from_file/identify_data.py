from europy_db_controllers.xl.sheet import data_block
from europy_db_controllers.xl.sheet import col_control_data

def identifyDataFnc(self,
                    dataBlock: data_block.DataBlock) -> None:
  if dataBlock.isList:
    for row in range(dataBlock.dataRow, dataBlock.maxRow + 1):
      # if doPrint: print(f"  row: {row} - isEmpty: {self.isEmptyColControlRow(row = row)}")  
      # if not self.isEmptyColControlRow(row = row) and doPrint:
      #   for cellTuple in self.sht.iter_cols(**self.getColControlRowDelimiters(row = row)):
      #     print(f"    row: {cellTuple[0].row} - column: {cellTuple[0].column} - cellTuple[0].value: {cellTuple[0].value}")
      if self.isEmptyColControlRow(row = row): continue
      valueDict = self.getColControlContent(row = row)
      # if self._capsuleType.__name__.startswith('MarketTransaction'):
      #   print(f"\n[_col_control._identifyData] colControlData.valueDict [dataBlock.isList]: {valueDict}\n")
      colControlData = col_control_data.ColControlData(colControlLabel = self.label,
                                                      sht = self.sht,
                                                      row = row,
                                                      valueDict=valueDict)
      colControlData.ensureConsistency()
      nextListBlock = dataBlock.nextListElement(dataRow=row)
      nextListBlock.colControlData = colControlData
    for listElement in dataBlock.listElements:
      for subColControl in self.subColControls.values():
        subDataBlock = listElement.nextSubBlock(isList=subColControl.isList,
                                                minCol=subColControl.firstCol,
                                                maxCol=subColControl.lastCol,
                                                colBlockName=subColControl.label,
                                                colBlockTableName=subColControl.tableName)
        subColControl.identifyData(dataBlock=subDataBlock)
  else:
    for row in range(dataBlock.parent.dataRow, dataBlock.parent.maxRow + 1):
      valueDict = self.getColControlContent(row = dataBlock.dataRow)

      # if self._capsuleType.__name__.startswith('MarketTransaction'):
      #   print(f"\n[_col_control._identifyData] colControlData.valueDict [NOT dataBlock.isList]: {valueDict}\n")
      colControlData = col_control_data.ColControlData(colControlLabel = self.label,
                                                        sht = self.sht,
                                                        row = row,
                                                        valueDict=valueDict)
      colControlData.ensureConsistency()
      if row == dataBlock.dataRow:
        dataBlock.colControlData = colControlData
      else:
        # if not a list, all rows must be empty
        colControlData.ensureEmpty
      for subColControl in self.subColControls.values():
        subDataBlock = dataBlock.nextSubBlock(isList=subColControl.isList,
                                              minCol=subColControl.firstCol,
                                              maxCol=subColControl.lastCol,
                                              colBlockName=subColControl.label,
                                              colBlockTableName=colControlData.tableName,
                                              dataRow = listElement.dataRow)
        subColControl.identifyData(dataBlock=subDataBlock)
