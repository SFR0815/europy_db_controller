import openpyxl as pxl


def identifyFnc(self,
               wkb:  pxl.Workbook):
    try:
      self.sht = wkb[self.name]
    except:
      err_msg = f"Can't identify ioWorksheet on ioWorkbook by it's name: {self.name}"
      err_msg += f"\n   workbook: {wkb.properties.title}"
      err_msg += "\nSheets in workbook:"
      for sheet in wkb.sheetnames:
          err_msg += f"\n    {sheet}"
      raise Exception(err_msg)
    
    self.colControl.identify(sht = self.sht)
    self.identifyData()
    # print(f"Worksheet '{self.sht.title}' identified at {datetime.datetime.now()}")