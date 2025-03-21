import typing

def getColControlContentFnc(self,
                            row: int) -> typing.Dict[str, any]:
  result = dict[str, any]()
  for column in self.columns.values():
    key, value = column.getLabelAndValueOfRow(row = row)
    result[key] = value
  return result