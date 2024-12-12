from __future__ import annotations

import sys, typing, operator

from sqlalchemy.ext import declarative as sqlalchemy_decl
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils 
from europy_db_controllers import _controller_base, _controller_utils

T = typing.TypeVar("T", bound=_controller_base.ControllerBase)
CT = typing.TypeVar("CT", bound=_capsule_base.CapsuleBase)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def __addCapsuleAttributes(controllerType: type[T],
                           capsuleType: type[CT]):
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # iterator of all capsuleTypes in session
  def iterFnc(self: T, 
              scope: _controller_base.ControllerDataScopes = 
                     _controller_base.ControllerDataScopes.NEW_AND_DIRTY,
              sortedBy: str = "",
              filterConditions: typing.Dict[str, any] = None):
    sqlalchemyTables = _controller_utils.getSqlAlchemyTablesOfScope(
                            capsuleType = capsuleType,
                            controllerType = controllerType, 
                            self = self, 
                            scope = scope,
                            filterConditions = filterConditions)
    # Sorting of sqlalchemyTables
    if not sortedBy is None: # 'None' is explicitly not sorted
      if len(sortedBy) > 0: # sort as specified on input
        sqlalchemyTables = sorted(sqlalchemyTables, 
                                  key = lambda x: getattr(x, sortedBy), 
                                  reverse=False)
      elif not capsuleType.sqlalchemyTableType._sorted_by is None: # standard sorting
        try:        
          sqlalchemyTables = sorted(sqlalchemyTables, 
                                    key = operator.attrgetter(*capsuleType.sqlalchemyTableType._sorted_by), 
                                    reverse=False)
        except Exception as e:
          print(f"[_controller_attr.__addCapsuleAttributes] {capsuleType.__name__}: sorted_by: {capsuleType.sqlalchemyTableType._sorted_by}")
          raise e
    for sqlalchemyTable in sqlalchemyTables:
      yield capsuleType.defineBySqlalchemyTable(
                session = self.session,
                sqlalchemyTableEntity = sqlalchemyTable)
  def lenOfFnc(self: T, 
               scope: _controller_base.ControllerDataScopes = 
                      _controller_base.ControllerDataScopes.NEW_AND_DIRTY) -> bool:
    sqlalchemyTables = _controller_utils.getSqlAlchemyTablesOfScope(
                            capsuleType = capsuleType,
                            controllerType = controllerType, 
                            self = self, 
                            scope = scope)
    return len(sqlalchemyTables)
  iterFncDecorated = _controller_base.cleanAndCloseSession(iterFnc)
  lenOfFncDecorated = _controller_base.cleanAndCloseSession(lenOfFnc)
  fncNameIter = _controller_utils.getCapsuleTypeIterFncName(capsuleType)
  fncNameLenOf = _controller_utils.getCapsuleTypeLenOfFncName(capsuleType)
  setattr(controllerType, fncNameIter, iterFncDecorated)     
  setattr(controllerType, fncNameLenOf, lenOfFncDecorated)     
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # the key used in json
  sqlalchemyTableType = capsuleType.sqlalchemyTableType
  key = sqlalchemyTableType.__table__.name
  keyAttrName = _controller_utils.getCapsuleTypeKeyAttrName(capsuleType)
  setattr(controllerType, keyAttrName, key)
  getattr(controllerType, _controller_utils.CONTROLLER_KEYS_ATTR_NAME).append(key)

def __addControllerTypeAttributes(controllerType: type[T]):
  capsuleTypes = controllerType._content
  def iterByKeyFnc(self: T,
                   capsuleKey: str,
                   scope: _controller_base.ControllerDataScopes = 
                          _controller_base.ControllerDataScopes.NEW_AND_DIRTY):
    capsuleType: CT = None
    if len(capsuleTypes) == 0: return None
    for thisCapsuleType in capsuleTypes:
      tableName = thisCapsuleType.sqlalchemyTableType.__table__.name  
      if tableName == capsuleKey:
        capsuleType = thisCapsuleType
        break
    capsuleIterAttributeName = _controller_utils.getCapsuleTypeIterFncName(capsuleType)
    for capsule in getattr(self, capsuleIterAttributeName)(scope = scope):
      yield capsule
  def lenOfByKeyFnc(self: T,
                    capsuleKey: str,
                    scope: _controller_base.ControllerDataScopes = 
                            _controller_base.ControllerDataScopes.NEW_AND_DIRTY):
    capsuleType: CT = None
    for thisCapsuleType in capsuleTypes:
      if thisCapsuleType.sqlalchemyTableType.__table__.name == capsuleKey:
        capsuleType = thisCapsuleType
        break
    capsuleLenOfAttributeName = _controller_utils.getCapsuleTypeLenOfFncName(capsuleType)
    return getattr(self, capsuleLenOfAttributeName)(scope = scope)
  iterByKeyFncDecorated = _controller_base.cleanAndCloseSession(iterByKeyFnc)
  lenOfByKeyFncDecorated = _controller_base.cleanAndCloseSession(lenOfByKeyFnc) 
  fncNameIterByKey = _controller_utils.getControllerIterByKeyFncName()
  fncNameLenOfByKey = _controller_utils.getControllerLenOfByKeyFncName()
  setattr(controllerType, fncNameIterByKey, iterByKeyFncDecorated)     
  setattr(controllerType, fncNameLenOfByKey, lenOfByKeyFncDecorated)   

def __addGetValidationItemsAttribute(controllerType: type[T]):
  validationItemsFncName = _capsule_utils.getValidationItemsFncName()
  def getterFnc(self,
                validationItems: typing.List[type[CT]] = list[type[CT]]()
                ) -> typing.List[type[CT]]:
    result: typing.List[type[CT]] = validationItems
    for content in self._content:
      capsuleValidationItems = getattr(content, validationItemsFncName)()
      for capsuleValidationItem in capsuleValidationItems:
        if not capsuleValidationItem in result:
          result.append(capsuleValidationItem)
    return result
  propertyGetter = classmethod(getterFnc)
  setattr(controllerType, validationItemsFncName, propertyGetter)    
  


def addAttributes(controllerTypeNames: typing.List[type[T]],
                  callingGlobals):
  for controllerTypeName in controllerTypeNames:
    controllerType = callingGlobals[controllerTypeName]
    capsuleTypes = controllerType._content
    setattr(controllerType, _controller_utils.CONTROLLER_KEYS_ATTR_NAME, [])
    for capsuleType in capsuleTypes:
      __addCapsuleAttributes(controllerType = controllerType,
                             capsuleType = capsuleType)
    __addControllerTypeAttributes(controllerType)
    __addGetValidationItemsAttribute(controllerType)
