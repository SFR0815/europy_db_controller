from europy_db_controllers.xl.sheet import delete_cntr_column

def internalAddDeleteControlColumnFnc(self) -> None:
  dCL = delete_cntr_column.DeleteControlColumn(columnNumber = self._width() + self.firstCol,
                                                subControllerKey = self.subControllerKey,
                                                controllerKeyEnum = self.controllerKeyEnum,
                                                rowControl = self._rowControl,
                                                colControl = self)
  self.columns[dCL.label] = dCL