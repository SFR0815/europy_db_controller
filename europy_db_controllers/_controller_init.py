# from __future__ import annotations

# import sys, uuid, typing, datetime
# from sqlalchemy import orm as sqlalchemy_orm
# import sqlalchemy


# from europy_db_controllers import _controller_base

# T = typing.TypeVar("T", bound=_controller_base.ControllerBase)

# def __getInitCode(controllerType: type[T],
#                   callingGlobals) -> str:
#   INPUTS_LINE_PREFIX = " " * 20
#   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # Function providing a input parameter line (general)
#   def getBasicInputLine(varName: str, 
#                         typeName: str = "", 
#                         default: str = "", 
#                         isEnd: bool = False) -> str:
#     output = f"{INPUTS_LINE_PREFIX}{varName}"
#     output = output + (f": {typeName}" if len(typeName) > 0 else "")
#     output = output + (f" = {default}" if len(default) > 0 else "")
#     output = output + ("," if not isEnd else ") -> None:") + "\n" 
#     return output
#   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # Input parameters shared by all __init__ functions
#   def getCommonInputLines() -> str:
#     output = getBasicInputLine(varName="self")  
#     output = output + getBasicInputLine(varName="session",
#                                         typeName="sqlalchemy_orm.Session")
#     return output 
#   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # Input parameter line(s) of a specific column
#   def getCustomInputLines(controllerType: type[T], 
#                           isEnd: bool = False) -> str:
#     content = controllerType._content
#     cntrIsEnd: bool = isEnd
#     if _capsule_utils.isRelationshipIdColumn(column=column):
#       cntrIsEnd = False
#     # The input line associated with the value of the column itself
#     output = getBasicInputLine(varName=column.name,
#                               typeName=pythonType,
#                               default="None",
#                               isEnd=cntrIsEnd)
  

# def addInitMethods(controllerTypes: typing.List[type[T]],
#                    callingGlobals):
#   for controllerType in controllerTypes:
#     initCodeString = __getInitCode(controllerType = controllerType, 
#                                    callingGlobals = callingGlobals)
#     # exec(initCodeString , callingGlobals)
#     # initMethod = callingGlobals[_capsule_utils.getInitFncName(sqlalchemyTable=sqlalchemyTable)]
#     # initMethodDecorated = _capsule_base.cleanAndCloseSession(
#     #                                   func = initMethod)
#     # setattr(capsuleType, "__init__", initMethodDecorated)
#     # Debug output
#     print("name of sqlalchemyTable: ", controllerType.__name__)
#     print("__Init__ code: \n", initCodeString)