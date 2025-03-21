def internalUpdateRowControlFnc(self):
  headDepth = self.__getHeadDepth()
  self._rowControl.updateColumnLabelRow(
                columnLableRow=headDepth + 1)