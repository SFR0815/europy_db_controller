from __future__ import annotations

import typing

import openpyxl as pxl

from europy_db_controllers.xl.sheet import utils


def setupFnc(self,
            wkb:  pxl.Workbook,
            controllerDict: typing.Dict):
    self.wkb = wkb
    self.capsulesDict = controllerDict[self.subControllerKey.value][self._capsuleKey]
    if utils.hasSheetOfName(wkb = self.wkb,
                            shtName = self.name):
      raise Exception(f"Duplicate definition of sheet with name '{self.name}'.")
    self.wkb.create_sheet(self.name)
    self.sht = self.wkb[self.name]
    for name in self.sht.defined_names:
        destinations = list(self.sht.defined_names[name].destinations)
        for sheet, coord in destinations:
            if sheet == self.name:
                print(f"    {name}: {coord}")
    self.colControl.setFormatsAndValidations(sht = self.sht)
    self.colControl.setupLabels(sht=self.sht)
    capsuleCount = 0
   
    while capsuleCount in self.capsulesDict:
      capsuleDict = self.capsulesDict[capsuleCount]
      self.colControl.writeValues(colControlDict = capsuleDict,
                                  dataBlock = self.rowControl.dataList.nextListElement())
      capsuleCount += 1
