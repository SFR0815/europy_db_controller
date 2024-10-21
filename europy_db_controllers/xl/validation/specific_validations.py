import typing

import openpyxl as pxl
from openpyxl.worksheet import worksheet as pxl_sht
from openpyxl.worksheet import cell_range as pxl_rng
from openpyxl.worksheet import datavalidation as pxl_dv

from europy_db_controllers.xl.sheet import utils as sht_ut

def getUniqueValidationParameters(cellAddress: str,
                                  cellRangeCoord: str,
                                  unique: bool) -> typing.Tuple[str, str, str, str]: 
  if unique:
    return(f'COUNTIF({cellRangeCoord}, {cellAddress})<=1',
            'custom', 
            'not unique', 
            'Duplicate entry on unique value column.')
  else:
    return None

def getTypeValidationParameters(cellAddress: str,
                                sqlalchemyDataType: str) -> typing.Tuple[str, str, str, str]: 
  if sqlalchemyDataType == 'UUID':
    return (f'NOT(ISERROR(SUMPRODUCT(SEARCH(MID({cellAddress},ROW(INDIRECT("1:"&LEN({cellAddress}))),1),"abcdefghijklmnopqrstuvwxyz0123456789-")))),' + \
                  f'MID({cellAddress},9,1)="-",' + \
                  f'MID({cellAddress},14,1)="-",' + \
                  f'MID({cellAddress},19,1)="-",' + \
                  f'MID({cellAddress},24,1)="-",' + \
                  f'LEN({cellAddress})-LEN(SUBSTITUTE({cellAddress},"-",""))=4,' + \
                  f'LEN({cellAddress})=36',
            'custom',
            'not a valid UUID string',
            'UUIDs must be defined as xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxx.\n' + \
              'With "x" being either a digit or a lowercase letter.')

  if sqlalchemyDataType == 'BOOLEAN':
    return (f'NOT(ISERROR({cellAddress}*1)),NOT(ISNUMBER({cellAddress}))', 
            'custom', 
            'not a valid boolean', 
            'Must be FALSE or TRUE.')

  if sqlalchemyDataType == 'DATETIME':
    return (f'NOT(ISERROR(SUMPRODUCT(SEARCH(MID({cellAddress},ROW(INDIRECT("1:"&LEN({cellAddress}))),1),"0123456789- :")))),' + \
                  f'MID({cellAddress},5,1) = "-",' + \
                  f'MID({cellAddress},8,1) = "-",' + \
                  f'MID({cellAddress},11,1) = " ",' + \
                  f'MID({cellAddress},14,1) = ":",' + \
                  f'MID({cellAddress},17,1) = ":",' + \
                  f'LEN({cellAddress}) = 19,' + \
                  f'LEN({cellAddress})-LEN(SUBSTITUTE({cellAddress},"-","")) = 2,' + \
                  f'LEN({cellAddress})-LEN(SUBSTITUTE({cellAddress},":","")) = 2,' + \
                  f'LEN({cellAddress})-LEN(SUBSTITUTE({cellAddress}," ","")) = 1',
              'custom',
              'not a valid datetime format',
              'Datetime must be defined in the form of "YYYY-MM-DD hh:mm:ss".')
  if sqlalchemyDataType.startswith('VARCHAR'):
    lenInPar = sqlalchemyDataType.removeprefix('VARCHAR')
    length = lenInPar[1:-1]
    return(f'LEN({cellAddress})<={length}',
            'custom', 
            'too long', 
            f'Cell entry must not exceed maximum of {length} characters permitted.')

  # if sqlalchemyDataType == 'BOOLEAN':
  #   return(f'if(isError(1*{cellAddress}),FALSE,AND(NOT(ISNUMBER({cellAddress})),OR(1*{cellAddress}=1,1*{cellAddress}=0)))',
  #           'custom', 
  #           'not a valid boolean', 
  #           'Cell must contain either FALSE or TRUE.')

  if sqlalchemyDataType == 'FLOAT':
    return(f'ISNUMBER({cellAddress})', 
            'custom', 
            'not a number', 
            'Cell must contain a number.')
  return None

def appendValidationParameters(validationParameters: typing.Tuple[str, str, str, str],
                               previousValidationParameters: typing.Tuple[str, str, str, str]) -> typing.Tuple[str, str, str, str]:
  result: typing.Tuple[str, str, str, str] = tuple[str, str, str, str]
  if previousValidationParameters is None:
    if not validationParameters is None:
      return (validationParameters[0],
             'custom',
             'Value is ' + validationParameters[2],
             '' + validationParameters[3])
    else:
      return None
  else:
    if not validationParameters is None: 
      return (previousValidationParameters[0] + ',' + validationParameters[0],
              'custom',
              previousValidationParameters[2] + ' or ' + validationParameters[2],
              previousValidationParameters[3] + '\nAND\n' + validationParameters[3])
    else:
      return previousValidationParameters
    
def finalizeValidationParameters(validationParameters: typing.Tuple[str, str, str, str]) -> typing.Tuple[str, str, str, str]:
  if validationParameters is None: return None
  return ('=AND(' + validationParameters[0] + ')',
          'custom',
          validationParameters[2],
          validationParameters[3])

def getValidationParameters(cellAddress: str,
                            sqlalchemyDataType: str,
                            cellRangeCoord: str,
                            unique: bool) -> typing.Tuple[str, str, str, str]: 
  result: typing.Tuple[str, str, str, str] = None
  uniqueValidationParameters = getUniqueValidationParameters(cellAddress = cellAddress,
                                                             cellRangeCoord = cellRangeCoord,
                                                             unique = unique)
  result = appendValidationParameters(validationParameters = uniqueValidationParameters,
                                                            previousValidationParameters = result)
  typeValidationParameters = getTypeValidationParameters(cellAddress = cellAddress,
                                                         sqlalchemyDataType = sqlalchemyDataType)
  result = appendValidationParameters(validationParameters = typeValidationParameters,
                                                            previousValidationParameters = result)
  result = finalizeValidationParameters(validationParameters = result)
  return result

def getValidation(sht: pxl_sht.Worksheet,
                  cellRow: int,
                  cellCol: int,
                  cellRange: pxl_rng.CellRange,
                  unique: bool,
                  sqlalchemyDataType: str) -> pxl_dv.DataValidation:
  cellAddress = sht_ut.getCellAddress(row = cellRow, col = cellCol)
  cell = sht[cellAddress]
  cellRangeCoord = pxl.utils.absolute_coordinate(cellRange.coord)
  validationParameters = getValidationParameters(cellAddress = cellAddress,
                                                 sqlalchemyDataType = sqlalchemyDataType,
                                                 cellRangeCoord = cellRangeCoord,
                                                 unique = unique)
  if validationParameters is None: return None
  return pxl_dv.DataValidation(type=validationParameters[1], 
                              formula1=validationParameters[0], 
                              allow_blank=True,
                              error = validationParameters[3],
                              errorTitle = validationParameters[2],
                              showErrorMessage=True)

def uniqueValidation(sht: pxl_sht.Worksheet,
                     cellRow: int,
                     cellCol: int,
                     cellRange: pxl_rng.CellRange) -> pxl_dv.DataValidation:
  cellAddress = sht_ut.getCellAddress(row = cellRow, col = cellCol)
  cell = sht[cellAddress]
  cellRangeCoord = pxl.utils.absolute_coordinate(cellRange.coord)
  formula = f'=COUNTIF({cellRangeCoord}, {cellAddress})<=1'
  return pxl_dv.DataValidation(type='custom', 
                              formula1=formula, 
                              allow_blank=True,
                              error = 'Please provide unique entry.',
                              errorTitle = 'Duplicate entry on unique value column.',
                              showErrorMessage=True)

def dataTypeValidation(sht: pxl_sht.Worksheet,
                       cellRow: int,
                       cellCol: int,
                       sqlalchemyDataType: str) -> pxl_dv.DataValidation:
  cellAddress = sht_ut.getCellAddress(row = cellRow, col = cellCol)
  cell = sht[cellAddress]
  validationParameters = getTypeValidationParameters()
  if validationParameters is None: return None
  return pxl_dv.DataValidation(type=validationParameters[1], 
                              formula1=validationParameters[0], 
                              allow_blank=True,
                              error = validationParameters[2],
                              errorTitle = validationParameters[3],
                              showErrorMessage=True) 