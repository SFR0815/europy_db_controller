from __future__ import annotations

import sys, typing
from sqlalchemy.ext import declarative as sqlalchemy_decl
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from europy_db_controllers.entity_capsules import _capsule_base 
from europy_db_controllers import _controller_base

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
      # Split attribute path by dots
      attributePath = filterAttributeName.split('.')
      
      # Start with the sqlalchemy table
      currentObj = sqlalchemyTable
      
      # Follow the attribute path except for last element
      for attr in attributePath[:-1]:
        if not hasattr(currentObj, attr):
            path = '.'.join(attributePath[:attributePath.index(attr)])
            raise AttributeError(f"Cannot find attribute '{attr}' in filter path '{filterAttributeName}'. "
                               f"Search failed at '{path}' on object of type {type(currentObj).__name__}. "
                               f"Original sqlalchemy table type: {type(sqlalchemyTable).__name__}")
        currentObj = getattr(currentObj, attr)
        if currentObj is None:
          return False
          
      # Compare final attribute value
      finalAttr = attributePath[-1]
      if getattr(currentObj, finalAttr) != filterAttributeValue:
        return False
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
      # Split attribute path by dots
      attributePath = filterAttributeName.split('.')
      currentType = sqlalchemyTableType
      currentAttr = None
      
      # Navigate through the attribute path
      for attrName in attributePath:
        try:
          currentAttr = getattr(currentType, attrName)
        except Exception as e:
          # Get all attributes of the current type
          attrs = [attr for attr in dir(currentType) if not attr.startswith('_')]
          # Build error message with indented attribute list
          error_msg = f"[_controller_utils.__getDbSqlalchemyTables] Attribute '{attrName}' not found on {currentType.__name__}.\n"
          error_msg += "Available attributes:\n"
          for attr in attrs:
              error_msg += f"  - {attr}\n"
          error_msg += f"\nOriginal error: {str(e)}\n"
          raise AttributeError(error_msg)
        # Get the type of the next level if not at end
        if hasattr(currentAttr, 'property') and hasattr(currentAttr.property, 'mapper'):
          currentType = currentAttr.property.mapper.class_
      
      dbQuery = dbQuery.filter(currentAttr == filterAttributeValue)
  with session.no_autoflush:
    import warnings
    with warnings.catch_warnings(record=True) as w:
        dbSqlalchemyTables = dbQuery.all()
        if w and any(issubclass(warning.category, sqlalchemy.exc.SAWarning) for warning in w):
            print(f"\n[_controller_utils.getDbSqlalchemyTables: line {sys._getframe().f_lineno}]" + \
                  f"\nSQLAlchemy warning occurred while querying {sqlalchemyTableType.__name__}")
            for warning in w:
                print(f"Warning: {warning.message}")
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
  
  # Check if filterConditions exist and validate all filter attributes exist on capsuleType
  if filterConditions is not None:
    # Get all attributes of the capsule type
    capsule_attrs = [attr for attr in dir(capsuleType.sqlalchemyTableType) if not attr.startswith('_')]
    
    # Track any missing attributes
    missing_attrs = []
    
    # Check each filter attribute exists
    for filter_attr in filterConditions.keys():
      # Split attribute path by dots to handle nested attributes
      attr_path = filter_attr.split('.')
      current_type = capsuleType.sqlalchemyTableType
      current_attr = None
      
      # Try to navigate through attribute path
      try:
        errMsg = "\nLog of attribute path:\n"
        for attr_name in attr_path:
          errMsg += f"[_controller_utils.__getSqlAlchemyTablesOfScope]      sqlalchemyTableType: {current_type.__name__} - attr_name: {attr_name}\n"
          errMsg += f"[_controller_utils.__getSqlAlchemyTablesOfScope]      hasattr(current_type, attr_name): {hasattr(current_type, attr_name)}\n"
          current_attr = getattr(current_type, attr_name)
          errMsg += f"[_controller_utils.__getSqlAlchemyTablesOfScope]      hasattr(current_attr, 'property'): {hasattr(current_attr, 'property')}\n"
          # Get next type if attribute is a relationship or hybrid property
          if hasattr(current_attr, 'property'):
            errMsg += f"[_controller_utils.__getSqlAlchemyTablesOfScope]         has attribute property\n" 
            if hasattr(current_attr.property, 'mapper'):
              errMsg += f"[_controller_utils.__getSqlAlchemyTablesOfScope]         has attribute property.mapper\n" 
              current_type = current_attr.property.mapper.class_
            elif isinstance(current_attr.property, sqlalchemy.ext.hybrid.hybrid_property):
              errMsg += f"[_controller_utils.__getSqlAlchemyTablesOfScope]         has attribute property.hybrid_property\n" 
              # Handle hybrid property
              continue
      except Exception as e:
        missing_attrs.append(f"{filter_attr} \n{' ' * 4}(Error: {str(e)})")
    
    # Raise error if any attributes are missing
    if missing_attrs:
      error_msg = f"[Invalid filter attributes] The following filter attributes do not exist on {capsuleType.__name__}:\n"
      for attr in missing_attrs:
        error_msg += f"  - {attr}\n"
      error_msg += f"\nAvailable attributes on {capsuleType.__name__}:\n"
      for attr in sorted(capsule_attrs):
        error_msg += f"  - {attr}\n"
      error_msg += errMsg
      raise AttributeError(error_msg)
    
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

