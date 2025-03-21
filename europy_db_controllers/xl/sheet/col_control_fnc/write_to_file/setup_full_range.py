from europy_db_controllers.xl.sheet import utils

def setupFullRangeFnc(self) -> None:
  fullRange = self.fullRange
  utils.updateFill(sht = self.sht,
                    cellRange=fullRange,
                    patternFill=self._patternFill,
                    ifHasPatternType=False)    
  utils.updateBorderAround(sht = self.sht,
                            cellRange = fullRange,
                            border=self._border) 