from __future__ import annotations

import sys, typing
from sqlalchemy.ext import declarative as sqlalchemy_decl
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from db_controllers.entity_capsules import _capsule_base 
from db_controllers import _controller_base

T = typing.TypeVar("T", bound=_controller_base.ControllerBase)
CT = typing.TypeVar("CT", bound=_capsule_base.CapsuleBase)

CONTROLLER_KEYS_ATTR_NAME = "_keys"
CONTROLLER_KEY_ATTR_PREFIX = "_key_"

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# functions retrieving sqlalchemy tables from the session
def __getFilterFunction(filterConditions: typing.Dict[str, any]
                        ) -> typing.Callable[[sqlalchemy_decl.DeclarativeMeta], bool]:
  def innerFilterFunction(sqlalchemyTable: sqlalchemy_decl.DeclarativeMeta) -> bool:
    for filterAttributeName, filterAttributeValue in filterConditions.items():
      if getattr(sqlalchemyTable, filterAttributeName) != filterAttributeValue: return False
    return True
  return innerFilterFunction
def __getFilteredSqlalchemyTables(sqlalchemyTables: typing.List[sqlalchemy_decl.DeclarativeMeta],
                                  filterConditions: typing.Dict[str, any]
                                  ) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
  filterFnc = __getFilterFunction(filterConditions=filterConditions)
  return list(filter(filterFnc, sqlalchemyTables))
  
def getNewSqlalchemyTables(
                      capsuleType: type[CT], 
                      session: sqlalchemy_orm.Session,
                      filterConditions: typing.Dict[str, any] = None
                      ) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
  sqlalchemyTableType = capsuleType.sqlalchemyTableType
  result: typing.List[sqlalchemy_decl.DeclarativeMeta] = []
  for newObject in session.new:
    if isinstance(newObject, sqlalchemyTableType):
      result.append(newObject)
  if not filterConditions is None:
    result = __getFilteredSqlalchemyTables(result, filterConditions)
  return result
def getDirtySqlalchemyTables(
                      capsuleType: type[CT], 
                      session: sqlalchemy_orm.Session,
                      filterConditions: typing.Dict[str, any] = None
                      ) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
  sqlalchemyTableType = capsuleType.sqlalchemyTableType
  result: typing.List[sqlalchemy_decl.DeclarativeMeta] = []
  for dirtyObject in session.dirty:
    if isinstance(dirtyObject, sqlalchemyTableType):
      result.append(dirtyObject)
  if not filterConditions is None:
    result = __getFilteredSqlalchemyTables(result, filterConditions)
  return result
def getNewDirtySqlalchemyTables(
                      capsuleType: type[CT], 
                      session: sqlalchemy_orm.Session,
                      filterConditions: typing.Dict[str, any] = None
                      ) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
  return getNewSqlalchemyTables(capsuleType, session, filterConditions) + \
         getDirtySqlalchemyTables(capsuleType, session, filterConditions)
def hasNewAndDirtySqlalchemyTables(
                      capsuleType: type[CT], 
                      session: sqlalchemy_orm.Session,
                      filterConditions: typing.Dict[str, any] = None
                      ) -> bool:
  sqlalchemyTables = getNewDirtySqlalchemyTables(capsuleType, session, filterConditions)
  return len(sqlalchemyTables) > 0
def getMapSqlalchemyTables(
                      capsuleType: type[CT], 
                      session: sqlalchemy_orm.Session,
                      newOrDirty: typing.List[sqlalchemy_decl.DeclarativeMeta] = [],
                      filterConditions: typing.Dict[str, any] = None
                      ) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
  sqlalchemyTableType = capsuleType.sqlalchemyTableType
  filterFnc = __getFilterFunction(filterConditions=filterConditions)
  result = newOrDirty
  for mapObject in session.identity_map.items():
    if isinstance(mapObject, sqlalchemyTableType):
      if not filterFnc(mapObject): continue
      if not mapObject in result:
        result.append(mapObject)
  return result
def getAllInSessionSqlalchemyTables(
                      capsuleType: type[CT], 
                      session: sqlalchemy_orm.Session,
                      filterConditions: typing.Dict[str, any] = None
                      ) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
  newOrDirty = getNewDirtySqlalchemyTables(capsuleType, session, filterConditions)
  return getMapSqlalchemyTables(capsuleType, session, newOrDirty, filterConditions)
def getDbSqlalchemyTables(
                      capsuleType: type[CT], 
                      session: sqlalchemy_orm.Session,
                      newOrDirty: typing.List[sqlalchemy_decl.DeclarativeMeta] = [],
                      filterConditions: typing.Dict[str, any] = None
                      ) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
  result = newOrDirty
  newOrDirtyIds = [obj.id for obj in result if (not obj.id is None)] 
  sqlalchemyTableType = capsuleType.sqlalchemyTableType
  dbQuery = None
  dbQuery = session.query(sqlalchemyTableType)
  if not filterConditions is None:
    for filterAttributeName, filterAttributeValue in filterConditions.items():
      dbQuery = dbQuery.filter(getattr(sqlalchemyTableType, filterAttributeName) == filterAttributeValue)
  with session.no_autoflush:
    dbSqlalchemyTables = dbQuery.all()
  for dbSqlalchemyTable in dbSqlalchemyTables:
    if not dbSqlalchemyTable.id in newOrDirtyIds:
      result.append(dbSqlalchemyTable)
  return result
def getAllSqlalchemyTables(
                      capsuleType: type[CT], 
                      session: sqlalchemy_orm.Session,
                      filterConditions: typing.Dict[str, any] = None
                      ) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
  newOrDirty = getNewDirtySqlalchemyTables(capsuleType, session, filterConditions)
  return getDbSqlalchemyTables(capsuleType, session, newOrDirty, filterConditions)

def getSqlAlchemyTablesOfScope(capsuleType: type[CT], 
                               controllerType: type[T],
                               self: T, 
                               scope: _controller_base.ControllerDataScopes = 
                                      _controller_base.ControllerDataScopes.NEW_AND_DIRTY,
                               filterConditions: typing.Dict[str, any] = None
                               ) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
  sqlalchemyTables: typing.List[sqlalchemy_decl.DeclarativeMeta] = []
  match scope:
    case _controller_base.ControllerDataScopes.ALL_IN_SESSION:
      sqlalchemyTables = getAllInSessionSqlalchemyTables(capsuleType, self.session, filterConditions)
    case _controller_base.ControllerDataScopes.NEW_AND_DIRTY:
      sqlalchemyTables = getNewDirtySqlalchemyTables(capsuleType, self.session, filterConditions)
    case _controller_base.ControllerDataScopes.STORED_ON_DB:
      newAndDirty = getNewDirtySqlalchemyTables(capsuleType, self.session, filterConditions)
      if len(newAndDirty) > 0: 
        fncName = getCapsuleTypeIterFncName(capsuleType)
        errMsg = f"[Non committed changes in session] - {fncName} on \n" + \
                f"{controllerType.__name__}" + \
                f"No new or modified objects of type {capsuleType.__name__} allowed in session if\n" + \
                f"the date to be sourced is specified as {scope.name}.\n" + \
                f"Please commit all changes before calling {fncName}."
        self._raiseException(errMsg)
      sqlalchemyTables = getDbSqlalchemyTables(capsuleType = capsuleType, 
                                               session = self.session, 
                                               newOrDirty = [],
                                               filterConditions = filterConditions)
    case _controller_base.ControllerDataScopes.ALL:
      sqlalchemyTables = getAllSqlalchemyTables(capsuleType, self.session, filterConditions)
    case _:
        fncName = getCapsuleTypeIterFncName(capsuleType)
        errMsg = f"[Unable to identify data scope] - {fncName} on \n" + \
                f"{controllerType.__name__}" + \
                f"The data content to be sourced is specified as {scope.name}.\n" + \
                f"This is not yet implemented."
  return sqlalchemyTables      

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Function naming conventions:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def getPlural(noun: str) -> str:
  if noun.endswith("y"):
      return noun[:-1] + "ies"
  elif noun.endswith("s"):
      return noun + "es"
  else:
      return noun + "s"
def getStartsWithLowerCase(word: str) -> str:
  return f"{word[:1].lower()}{word[1:]}"

def getBaseNameOfCapsuleType(capsuleType: type[CT]) -> str:
  capsuleTypeName = capsuleType.__name__
  return capsuleTypeName.removesuffix('Capsule')
def getBasePluralNameOfCapsuleType(capsuleType: type[CT]) -> str:
  baseName = getBaseNameOfCapsuleType(capsuleType)
  return getPlural(noun = baseName)

# capsule setup methods
def getCapsuleSetupFncName(capsuleType: type[CT]) -> str:
  baseName = getBaseNameOfCapsuleType(capsuleType)
  return getStartsWithLowerCase(baseName)
# capsule iterators
def getCapsuleTypeIterFncName(capsuleType: type[CT]) -> str:
  baseName = getCapsuleSetupFncName(capsuleType)
  baseName = getPlural(noun = baseName)
  return f"{baseName}"
def getControllerIterByKeyFncName() -> str:
  return f"capsulesByKey"
# numberOfCapsules
def getCapsuleTypeLenOfFncName(capsuleType: type[CT]) -> str:
  baseName = getBasePluralNameOfCapsuleType(capsuleType)
  return f"lenOf{baseName}"
def getControllerLenOfByKeyFncName() -> str:
  return f"lenOfCapsulesByKey"
# the json key of the capsule type
def getCapsuleTypeKeyAttrName(capsuleType: type[CT]) -> str:
  sqlalchemyTableType = capsuleType.sqlalchemyTableType
  tableName = sqlalchemyTableType.__table__.name
  return f"{CONTROLLER_KEY_ATTR_PREFIX}{tableName}"


def getControllerDataToDictFncName() -> str:
  return f"_controllerDataToDict"
def getControllerDataToJsonFncName() -> str:
  return f"_controllerDataToJson"
def getControllerDataFromDictFncName() -> str:
  return f'_controllerDataFromDict'

def getToDictFncName() -> str:
  return f'toDict'
def getToJsonFncName() -> str:
  return f'toJson'
def getFromDictFncName() -> str:
  return f'fromDict'

