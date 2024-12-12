from __future__ import annotations

import typing, sys, os
from sqlalchemy.ext import declarative as sqlalchemy_decl


from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base


T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

DEBUG_CAPSULE_TYPE = "MarketTransactionCapsule"


def __getInitCode(capsuleType: type[T],
                  callingGlobals) -> str:
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Def head and input parameter definition
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # spacing parameter for the indent of input parameter lines: 
  INPUTS_LINE_PREFIX = " " * 20
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
  # Input parameters shared by all __init__ functions
  def getCommonInputLines() -> str:
    output = getBasicInputLine(varName="self")  
    output = output + getBasicInputLine(varName="session",
                                        typeName="sqlalchemy_orm.Session")
    return output 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Input parameter line(s) of a specific column
  def getCustomInputLine(columnOrAlikeInfo: typing.Tuple[str, bool, bool],  
                         isEnd: bool = False) -> str:
    # FIXME: pythonType = _capsule_utils.getPythonType(type(column.type))
    # If it is the last column, the closure of the function head 
    #    must be after the last input line of the parameters 
    #    associated with the column:
    itemName = columnOrAlikeInfo[0]
    isHybridProperty = columnOrAlikeInfo[1]
    isAtEnd: bool = isEnd
    if _capsule_utils.isRelationshipIdColumnName(columnName=itemName):
      isAtEnd = False
    # The input line associated with the value of the column itself
    output = getBasicInputLine(varName=itemName,
                              # FIXME: typeName=pythonType,
                              default="None",
                              isEnd=False)
    # If the column is a relationship column, up to two further input
    #    lines are inserted:
    #    - the name attribute line of the relationship (if linked 
    #         sqlalchemyTableType has a 'name')
    #    - the object of the relationship
    if _capsule_utils.isRelationshipIdColumnName(columnName=itemName):
      if isHybridProperty:
        relationshipName = _capsule_utils.getColumnToRelationshipName(columnName=itemName)
        relSqlaObjectTypeName = getattr(capsuleType.sqlalchemyTableType, relationshipName).fget.__annotations__['return']
        relSqlaObjectTypeName = relSqlaObjectTypeName.__name__
      else:
        relSqlaObjectTypeName = _capsule_utils.getRelationshipTypeNameOfColumnName(sqlalchemyTableType=capsuleType.sqlalchemyTableType, 
                                                                     columnName=itemName)
      relSqlaObjectType = callingGlobals[relSqlaObjectTypeName]
      if hasattr(relSqlaObjectType, 'name'):
        relNameColumnName = _capsule_utils.getColumnRelationshipNameField(columnName=itemName)
        output = output + getBasicInputLine(varName=relNameColumnName,
                                typeName="str",
                                default="None",
                                isEnd=False)
      relColumnName = _capsule_utils.getColumnToRelationshipName(columnName=itemName)
      relCapsuleObjectType = _capsule_utils.getSqlaToCapsuleName(relSqlaObjectTypeName)
      output = output + getBasicInputLine(varName=relColumnName,
                              typeName=relCapsuleObjectType,
                              default="None",
                              isEnd=False)
    return output
  def getConditionsInputLines():
    output = ""
    output = output + getBasicInputLine(_capsule_utils.INIT_ENFORCE_NOT_NEW_OR_DIRTY_FLAG, "bool", "False", True)
    return output
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Definition of the full function head:
  def getInitDef(capsuleType: typing.Type[T], 
                 columnsAndAlikeInfo: typing.Dict[str, typing.Tuple[str, bool, bool]]) -> str:
    output = f"def {_capsule_utils.getInitFncName(sqlalchemyTableType=capsuleType.sqlalchemyTableType)}(\n" + \
             getCommonInputLines()
    inputItemNames = list(columnsAndAlikeInfo.keys())
    for inputItemNumber in range(0, len(inputItemNames)):
      inputItemName = inputItemNames[inputItemNumber]
      columnOrAlikeInfo = columnsAndAlikeInfo[inputItemName]
      # print("   column: ", column.name)
      isEnd = (inputItemNumber == len(columnsAndAlikeInfo) - 1)
      output = output + getCustomInputLine(columnOrAlikeInfo = columnOrAlikeInfo,
                                            isEnd = isEnd)
    output = output + getConditionsInputLines()
    return output 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # The content of the __init_ function:
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Code shared by all __init__ functions
  #    - call of the __init__ of the super
  #    - super is called somewhat different as usual as init function is not defined within 
  #         the class' closure
  def getCommonCodeLinesAtStart() -> str:
    # See: https://stackoverflow.com/questions/71879642/how-to-pass-function-with-super-when-creating-class-dynamically
    capsuleClassName = _capsule_utils.getCapsuleClassName(sqlalchemyTableType=capsuleType.sqlalchemyTableType)
    notNewOrDirty = _capsule_utils.INIT_ENFORCE_NOT_NEW_OR_DIRTY_FLAG
    output = ""
    output = output + f"{' ' * 2}super({capsuleClassName}, self).__init__(session = session,\n"
    output = output + f"{' ' * 2}                                         {notNewOrDirty} = {notNewOrDirty})\n"
    return output
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # The code lines handling the input parameters of each column      
  def getCustomCodeLines(columnsAndAlikeInfo: typing.Dict[str, typing.Tuple[str, bool, bool]]) -> str:  
    output = ""
    for columnOrAlikeInfo in columnsAndAlikeInfo.values():
      itemName = columnOrAlikeInfo[0]
      isHybridProperty = columnOrAlikeInfo[1]
      output = output + f"{' ' * 2}self._omit_none_{itemName}({itemName})\n"    
      if _capsule_utils.isRelationshipIdColumnName(columnName=itemName):
        if isHybridProperty:
          relationshipName = _capsule_utils.getColumnToRelationshipName(columnName=itemName)
          relSqlaObjectTypeName = getattr(capsuleType.sqlalchemyTableType, relationshipName).fget.__annotations__['return']
          relSqlaObjectTypeName = relSqlaObjectTypeName.__name__
        else:
          relSqlaObjectTypeName = _capsule_utils.getRelationshipTypeNameOfColumnName(sqlalchemyTableType=capsuleType.sqlalchemyTableType, 
                                                                      columnName=itemName)
        relSqlaObjectType = callingGlobals[relSqlaObjectTypeName]
        if hasattr(relSqlaObjectType, 'name'):
          relationshipName = _capsule_utils.getColumnRelationshipNameField(columnName=itemName)
          output = output + f"{' ' * 2}self._omit_none_{relationshipName}({relationshipName})\n"
        relColumnName = _capsule_utils.getColumnToRelationshipName(columnName=itemName)
        output = output + f"{' ' * 2}if {relColumnName} is not None:\n"
        output = output + f"{' ' * 4}self.sqlalchemyTable.{relColumnName} = {relColumnName}.sqlalchemyTable\n"
    return output 
  def getCommonCodeLinesAtEnd() -> str:
    output = ""
    output = output + f"{' ' * 2}self.{_capsule_utils.getConsistencyCheckOverAllFncName()}()\n"
    output = output + f"{' ' * 2}self.{_capsule_utils.getSourceAndConsistencyCheckOverAllFncName()}()\n"
    return output
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # The complete code body of the function      
  def getCodeLines(columnsAndAlikeInfo: typing.Dict[str, typing.Tuple[str, bool, bool]]) -> str:  
    # getCommonCodeLinesAtEnd() FIXME: add consistency checks to capsule
    return getCommonCodeLinesAtStart() + \
           getCustomCodeLines(columnsAndAlikeInfo = columnsAndAlikeInfo) + \
           getCommonCodeLinesAtEnd()

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # identification of the column not hidden to the outside
  columnsAndAlikeInfo = _capsule_utils.getCapsuleInitColumnsAndColumnLikeProperties(capsuleType = capsuleType)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # The complete function definition:
  return getInitDef(capsuleType = capsuleType, 
                    columnsAndAlikeInfo = columnsAndAlikeInfo) + \
         getCodeLines(columnsAndAlikeInfo = columnsAndAlikeInfo)


def addInitMethods(capsuleList: typing.List[T],
                   callingGlobals):
  for capsuleType in capsuleList:
    sqlalchemyTableType = capsuleType.sqlalchemyTableType
    initCodeString = __getInitCode(capsuleType = capsuleType, 
                                   callingGlobals = callingGlobals)
    
    # this_file_path = os.path.dirname(__file__)
    # folder_path = 'init_codes'
    # full_file_path = os.path.join(this_file_path, folder_path)
    # file_name = full_file_path + '/' + capsuleType.__name__ + '.txt'
    # with open(file_name, 'w') as file:
    #   file.write(initCodeString)
    if capsuleType.__name__ == DEBUG_CAPSULE_TYPE:
      print(f"setupCode {capsuleType.__name__}: \n{initCodeString}")
    try:    
      exec(initCodeString , callingGlobals)
    except Exception as e: 
      print(f"initCodeString: {initCodeString}")
      print(f"Error executing init code for {capsuleType.__name__}: {e}")
      raise e
    initMethod = callingGlobals[_capsule_utils.getInitFncName(sqlalchemyTableType=sqlalchemyTableType)]
    initMethodDecorated = _capsule_base.cleanAndCloseSession(
                                      func = initMethod)
    setattr(capsuleType, "__init__", initMethodDecorated)
