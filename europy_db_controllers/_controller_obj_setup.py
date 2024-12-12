import sys, typing
from sqlalchemy.ext import declarative as sqlalchemy_decl
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils 
from europy_db_controllers import _controller_base, _controller_utils

T = typing.TypeVar("T", bound=_controller_base.ControllerBase)
CT = typing.TypeVar("CT", bound=_capsule_base.CapsuleBase)

# spacing parameter for the indent of input parameter lines: 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
INPUTS_LINE_PREFIX = " " * 20

DEBUG_CAPSULE_TYPE = "MarketTransactionCapsule"
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Function providing a input parameter line (general)
def getBasicInputLine(varName: str, 
                      typeName: str = "", 
                      default: str = "", 
                      isEnd: bool = False) -> str:
  output = f"{INPUTS_LINE_PREFIX}{varName}"
  output = output + (f": {typeName}" if len(typeName) > 0 else "")
  output = output + (f" = {default}" if len(default) > 0 else "")
  output = output + ("," if not isEnd else ") -> None:") + "\n" 
  return output

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Function providing a input parameter line (general)
def isColumnAttributeNameToAdd(columnAttributeName: str,
                               key: str) -> bool: 
  isNone = columnAttributeName is None 
  isInternalNameAttribute = key == _capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME
  isIdFieldAttribute = key == _capsule_utils.REL_ATTR_DICT_KEY_ID
  return not (isNone or isInternalNameAttribute or isIdFieldAttribute)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def __getObjectSetupCode(capsuleType: type[CT],
                         setupFncName: str,
                         callingGlobals) -> str:
  # if capsuleType.__name__ == DEBUG_CAPSULE_TYPE:
  #   print(f"sqlalchemyTableType: {capsuleType.sqlalchemyTableType.__name__}")
  #   columns = capsuleType.sqlalchemyTableType.__table__.columns 
  #   for column in columns:
  #     print(f"        column: {column.name}")
  tableAttrNameDict = _capsule_utils.getDictOfColumnAndAlikeAttributeNamesOfCapsule(capsuleType=capsuleType)
  # if capsuleType.__name__ == DEBUG_CAPSULE_TYPE:
  #   for key, columnOrAlikeInfo in tableAttrNameDict.items():
  #     print(f"key: {key}")
  #     print(f"          columnOrAlikeInfo: {columnOrAlikeInfo}")
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Def head and input parameter definition
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Input parameters shared by all __init__ functions
  def getCommonInputLines() -> str:
    output = getBasicInputLine(varName="self")  
    return output 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Input parameter line(s) of a specific column
  def getCustomInputLine(columnOrAlikeInfo: typing.Tuple[str, bool, bool]) -> str:
    columnOrAlikeName = columnOrAlikeInfo[0]
    columnAttrNameDict = tableAttrNameDict[columnOrAlikeName]
    output = ""
    for key, columnAttributeName in columnAttrNameDict.items():
      if isColumnAttributeNameToAdd(columnAttributeName = columnAttributeName, key = key): 
        if hasattr(capsuleType, columnAttributeName):
          output = output + getBasicInputLine(
                                varName=columnAttributeName,
                                # typeName=pythonType, FIXME: type of input will be defined when suitable
                                default="None")
    return output
  def getConditionsInputLines():
    output = ""
    output = output + getBasicInputLine(_capsule_utils.INIT_ENFORCE_NOT_NEW_OR_DIRTY_FLAG, "bool", "False", True)
    return output
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Definition of the full function head:
  def getFncHead(columnsAndAlikeInfo: typing.Dict[str, typing.Tuple[str, bool, bool]]) -> str:
    output = f"def {setupFncName}(\n" + \
             getCommonInputLines()
    inputItemNames = list(columnsAndAlikeInfo.keys())
    for inputItemNumber in range(0, len(inputItemNames)):
      inputItemName = inputItemNames[inputItemNumber]
      columnOrAlikeInfo = columnsAndAlikeInfo[inputItemName]
      # print("   column: ", column.name)
      isEnd = (inputItemNumber == len(columnsAndAlikeInfo) - 1)
      output = output + getCustomInputLine(columnOrAlikeInfo = columnOrAlikeInfo)
    output = output + getConditionsInputLines()
    return output 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # The content of the setup function:
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # 
  def getCrateCapsuleCode(columnsAndAlikeInfo: typing.Dict[str, typing.Tuple[str, bool, bool]]) -> str:
    def getParameters() -> str:
      indent = INPUTS_LINE_PREFIX + " " * 4
      output = "\n"
      output = output + f"{indent}session = self.session, \n"
      for columnOrAlikeName in columnsAndAlikeInfo.keys():
        columnAttrNameDict = tableAttrNameDict[columnOrAlikeName]
        for key, columnAttributeName in columnAttrNameDict.items():
          if isColumnAttributeNameToAdd(columnAttributeName = columnAttributeName, key = key): 
            if hasattr(capsuleType, columnAttributeName):
              parameterLine = f"{indent}{columnAttributeName} = {columnAttributeName}"
              output = output + f"{parameterLine}, \n"
      output = output + f"{indent}{_capsule_utils.INIT_ENFORCE_NOT_NEW_OR_DIRTY_FLAG} = " + \
                                f"{_capsule_utils.INIT_ENFORCE_NOT_NEW_OR_DIRTY_FLAG}"
      return output
    output = ""
    output = output + f"{' ' *2}capsule = {capsuleType.__name__}({getParameters()})\n"
    output = output + f"{' ' *2}capsule.addToSession()\n"
    output = output + f"{' ' *2}return capsule\n"
    return output
  def getCodeLines(columnsAndAlikeInfo: typing.Dict[str, typing.Tuple[str, bool, bool]]) -> str:  
    return getCrateCapsuleCode(columnsAndAlikeInfo = columnsAndAlikeInfo) 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # identification of the column not hidden to the outside
  # print("callingGlobals: ", vars(capsules).keys())
  columnsAndAlikeInfo = _capsule_utils.getCapsuleInitColumnsAndColumnLikeProperties(capsuleType = capsuleType)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # The complete function definition:
  return getFncHead(columnsAndAlikeInfo = columnsAndAlikeInfo) + \
         getCodeLines(columnsAndAlikeInfo = columnsAndAlikeInfo)

def __addSetupMethod(controllerType: type[T],
                     capsuleType: type[CT],
                     callingGlobals):
  setupFncName = _controller_utils.getCapsuleSetupFncName(capsuleType=capsuleType)
  setupCode = __getObjectSetupCode(capsuleType = capsuleType,
                                   setupFncName = setupFncName,
                                   callingGlobals = callingGlobals)
  if capsuleType.__name__ == DEBUG_CAPSULE_TYPE:
    print(f"setupCode {capsuleType.__name__}: \n{setupCode}")
  exec(setupCode, callingGlobals)
  setupMethod = callingGlobals[setupFncName]
  setupMethodDecorated = _controller_base.cleanAndCloseSession(func = setupMethod)  
  setattr(controllerType, setupFncName, setupMethodDecorated)

def addSetupMethods(controllerTypeNames: typing.List[type[T]],
                    callingGlobals):
  for controllerTypeName in controllerTypeNames:
    controllerType = callingGlobals[controllerTypeName]
    capsuleTypes = controllerType._content
    for capsuleType in capsuleTypes:
      __addSetupMethod(controllerType = controllerType,
                       capsuleType = capsuleType,
                       callingGlobals = callingGlobals)
