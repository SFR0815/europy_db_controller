import typing

def toDictFnc(self) -> typing.Dict[str, dict]:
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
    # if self.name == "market_transaction":
    #   print(f"\n[io_sht.toDict] - result: {result}")
    return result  