from __future__ import annotations

import typing, sys


from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base


T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)


def __getInitCode(table,
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
  def getCustomInputLine(column, isEnd: bool = False) -> str:
    pythonType = _capsule_utils.getPythonType(type(column.type))
    # If it is the last column, the closure of the function head 
    #    must be after the last input line of the parameters 
    #    associated with the column:
    cntrIsEnd: bool = isEnd
    if _capsule_utils.isRelationshipIdColumn(column=column):
      cntrIsEnd = False
    # The input line associated with the value of the column itself
    output = getBasicInputLine(varName=column.name,
                              typeName=pythonType,
                              default="None",
                              isEnd=cntrIsEnd)
    # If the column is a relationship column, up to two further input
    #    lines are inserted:
    #    - the name attribute line of the relationship (if linked 
    #         table has a 'name')
    #    - the object of the relationship
    if _capsule_utils.isRelationshipIdColumn(column=column):
      relSqlaObjectTypeName = _capsule_utils.getRelationshipTypeName(table=table, 
                                                                     column=column, 
                                                                     callingGlobals = callingGlobals)
      relSqlaObjectType = callingGlobals[relSqlaObjectTypeName]
      if hasattr(relSqlaObjectType, 'name'):
        relNameColumnName = _capsule_utils.getRelationshipNameFieldOfColumn(column=column)
        output = output + getBasicInputLine(varName=relNameColumnName,
                                typeName="str",
                                default="None",
                                isEnd=cntrIsEnd)
      relColumnName = _capsule_utils.getRelationshipNameOfColumn(column=column)
      relCapsuleObjectType = _capsule_utils.getSqlaToCapsuleName(relSqlaObjectTypeName)
      output = output + getBasicInputLine(varName=relColumnName,
                              typeName=relCapsuleObjectType,
                              default="None",
                              isEnd=isEnd)
    return output
  def getConditionsInputLines():
    output = ""
    output = output + getBasicInputLine(_capsule_utils.INIT_ENFORCE_NOT_NEW_OR_DIRTY_FLAG, "bool", "False", True)
    return output
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Definition of the full function head:
  def getInitDef(table, 
                 columns) -> str:
    output = f"def {_capsule_utils.getInitFncName(table=table)}(\n" + \
             getCommonInputLines()
    for colNo in range(0, len(columns)):
      column = columns[colNo]
      output = output + getCustomInputLine(column = column)
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
    capsuleClassName = _capsule_utils.getCapsuleClassName(table=table)
    notNewOrDirty = _capsule_utils.INIT_ENFORCE_NOT_NEW_OR_DIRTY_FLAG
    output = ""
    output = output + f"{' ' * 2}super({capsuleClassName}, self).__init__(session = session,\n"
    output = output + f"{' ' * 2}                                         {notNewOrDirty} = {notNewOrDirty})\n"
    return output
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # The code lines handling the input parameters of each column      
  def getCustomCodeLines(columns) -> str:  
    output = ""
    for column in columns:
      output = output + f"{' ' * 2}self._omit_none_{column.name}({column.name})\n"    
      if _capsule_utils.isRelationshipIdColumn(column=column):
        relSqlaObjectTypeName = _capsule_utils.getRelationshipTypeName(table=table, 
                                                                       column=column, 
                                                                       callingGlobals = callingGlobals)
        relSqlaObjectType = callingGlobals[relSqlaObjectTypeName]
        if hasattr(relSqlaObjectType, 'name'):
          relationshipName = _capsule_utils.getRelationshipNameFieldOfColumn(column=column)
          output = output + f"{' ' * 2}self._omit_none_{relationshipName}({relationshipName})\n"
        relColumnName = _capsule_utils.getRelationshipNameOfColumn(column=column)
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
  def getCodeLines(columns) -> str:  
    # getCommonCodeLinesAtEnd() FIXME: add consistency checks to capsule
    return getCommonCodeLinesAtStart() + \
           getCustomCodeLines(columns=columns) + \
           getCommonCodeLinesAtEnd()

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # identification of the column not hidden to the outside
  nonChangeTrackColumns = _capsule_utils.getNonChangeTrackColumns(table=table,
                                                                  callingGlobals = callingGlobals)   
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # The complete function definition:
  return getInitDef(table = table, columns = nonChangeTrackColumns) + \
         getCodeLines(columns = nonChangeTrackColumns)


def addInitMethods(capsuleList: typing.List[T],
                   callingGlobals):
  for capsuleType in capsuleList:
    sqlalchemyTableType = capsuleType.sqlalchemyTableType
    table = sqlalchemyTableType.__table__
    initCodeString = __getInitCode(table = table, 
                                   callingGlobals = callingGlobals)
    exec(initCodeString , callingGlobals)
    initMethod = callingGlobals[_capsule_utils.getInitFncName(table=table)]
    initMethodDecorated = _capsule_base.cleanAndCloseSession(
                                      func = initMethod)
    setattr(capsuleType, "__init__", initMethodDecorated)
    # Debug output
    # if capsuleType.__name__ == "CoreAccountCapsule":
    #   print("name of table: ", getattr(table, 'name'))
    #   print("  capsuleClassName: ", capsuleType.__name__)
    #   print("  baseClass       : ", sqlalchemyTableType.__name__)
    #   print("__Init__ code: \n", __getInitCode(table=table,
    #                                            callingGlobals = callingGlobals))
