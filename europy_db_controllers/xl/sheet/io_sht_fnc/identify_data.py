def identifyDataFnc(self) -> None:
    mainDataList = self.rowControl.dataList
    # firstEmptyRow used as Flag for reading mode
    #     passed through all sub-DataBlocks
    mainDataList.firstEmptyRow = self.firstEmptyRow 
    self.colControl.identifyData(dataBlock=mainDataList)