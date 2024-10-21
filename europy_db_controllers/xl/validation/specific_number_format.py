def getNumberFormatParameters(sqlalchemyDataType) -> str:
  if sqlalchemyDataType == 'UUID' or sqlalchemyDataType.startswith('VARCHAR'):
    return '@'
  elif sqlalchemyDataType == 'FLOAT':
    return '#,##0.0000'
  elif sqlalchemyDataType == 'BOOLEAN':
    return 'TRUE;TRUE;FALSE'
  elif sqlalchemyDataType == 'DATETIME':
    return '@' # enters as a text
  else:
    return None
