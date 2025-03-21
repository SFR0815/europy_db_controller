def isEmptyColControlRowFnc(self,
                            row: int) -> bool:
  for cellTuple in self.sht.iter_cols(**self.getColControlRowDelimiters(row = row)):
    if cellTuple[0].value is not None: return False
  return True