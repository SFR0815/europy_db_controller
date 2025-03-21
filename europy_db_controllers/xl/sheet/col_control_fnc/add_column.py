from europy_db_controllers.xl.sheet import data_column
from europy_db_controllers.xl.validation import validation_column as io_val_col

from europy_db_controllers.xl.sheet.col_control_fnc.debug_cntr import debug_control


def addColumnFnc(self, 
                label: str,
                validation: io_val_col.ValidationColumn,
                unique: bool,
                sqlalchemyDataType: str):
  column_number = len(self.columns) + self.firstCol

  debug_control.debugPrint(msg = f"adding column with label: {column_number} {label}",
                           fncName = "_addColumn")

  column = data_column.DataColumn(label = label,
                                  column_number = column_number,
                                  subControllerKey = self.subControllerKey,
                                  controllerKeyEnum = self.controllerKeyEnum,
                                  validation=validation,
                                  rowControl = self._rowControl,
                                  colControl = self,
                                  unique = unique,
                                  sqlalchemyDataType = sqlalchemyDataType)
  self.columns[column.label] = column