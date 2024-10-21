from __future__ import annotations

import sys, uuid, \
       types, datetime, sqlalchemy
from sqlalchemy.dialects import postgresql as sqlalchemy_pg
from sqlalchemy import orm as sqlalchemy_orm


from europy_db_controllers.entity_capsules import _capsule_base

PYTHON_BUILTINS_MODULE = "builtins"
INIT_ENFORCE_NOT_NEW_OR_DIRTY_FLAG = "enforceNotNewOrDirty"



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Mapping from sqlalchemy types to python types
dictSqlaToType = {
            sqlalchemy_pg.UUID: uuid.UUID,
            sqlalchemy.DateTime: datetime.datetime,
            sqlalchemy.Date: datetime.date,
            sqlalchemy.TIMESTAMP: datetime.datetime,
            sqlalchemy.FLOAT: float,
            sqlalchemy.String: str,
            sqlalchemy.BOOLEAN: bool,
            sqlalchemy.Integer: int,
            sqlalchemy.INTEGER: int}

def getPythonType(sqlalchemyType) -> str:
  try: 
     pyType = dictSqlaToType[sqlalchemyType]
     pyTypeModule = pyType.__module__
     pyTypeName = pyType.__name__
     if pyTypeModule == PYTHON_BUILTINS_MODULE:
        return pyTypeName
     else:
        return f"{pyTypeModule}.{pyTypeName}"
  except:
    raise Exception(f"[getPythonType] - " +\
                    f"Could not identify sqlalchemyType: '{str(sqlalchemyType)}'.\n" + \
                    f"Please update _capsule_utils 'dictSqlaToType'.")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Information on the set of table columns
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Test if table has a 'name' column
def hasName(table) -> bool:
  for column in table.columns:
    if column.name == "name":
      return True
  return False
# Sqlalchemy columns less those used to track changes (created & modified at)
#   - might be used to hide away further system related information (user etc.)
def getNonChangeTrackColumns(table,
                             callingGlobals):
  sqlalchemyTableType = callingGlobals[getSqlalchemyTableTypeName(table=table)]
  result = []
  for column in table.columns:
    if not column.name in sqlalchemyTableType._changeTrackFields:
      result.append(column)
  return result
# Test if sqlalchemy column is a base column (see _capsule_base '_base_columns')
def isBaseColumn(capsuleType: _capsule_base.CapsuleBase, 
                 column) -> bool:
  baseColumnNames = capsuleType._base_columns
  return column.name in baseColumnNames


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Naming conventions for capsule and sqlalchemy table objects
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The base name of the object -> derived from the table name
#     - table names must be unique
#     - no schema names used to differentiate
def getBaseNameFromString(name: str) -> str:
  result = ''.join(x.capitalize() for x in name.split("_"))
  return result
def getBaseName(table) -> str:
  return getBaseNameFromString(name = getattr(table, 'name'))
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The name of the capsule object -> base name plus 'Capsule'-suffix" "
CAPSULE_OBJECT_SUFFIX = "Capsule"
def getCapsuleClassNameFromBaseName(baseName: str) -> str:
  return baseName + CAPSULE_OBJECT_SUFFIX
def getCapsuleClassName(table) -> str:
  baseName = getBaseName(table=table) 
  return getCapsuleClassNameFromBaseName(baseName) 
def isCapsuleClassName(className: str) -> bool:
  return className.endswith(CAPSULE_OBJECT_SUFFIX)
  # more precise testing might be added later
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The name of the sqlalchemy table object -> base name plus 'Table'-suffix
SQLA_TABLE_OBJECT_SUFFIX = "Table"
def getSqlalchemyTableTypeFromBaseName(baseName: str) -> str:
  return baseName + SQLA_TABLE_OBJECT_SUFFIX
def getSqlalchemyTableTypeName(table) -> str:
  baseName = getBaseName(table=table) 
  return getSqlalchemyTableTypeFromBaseName(baseName) 
def isSqlalchemyTableTypeName(className: str) -> bool:
  return className.endswith(SQLA_TABLE_OBJECT_SUFFIX)
  # more precise testing might be added later
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Conversion between base, capsule and sqlalchemy table object names
def getCapsuleToBaseName(capsuleClassName: str) -> str:
  if not isCapsuleClassName(capsuleClassName):
    raise Exception(f"getCapsuleToBaseName - capsuleClassName '{capsuleClassName}' " + \
                    f"does not end with {CAPSULE_OBJECT_SUFFIX}")
  return capsuleClassName.removesuffix(CAPSULE_OBJECT_SUFFIX)
def getCapsuleToSqlaName(capsuleClassName: str) -> str:
  baseName = getCapsuleToBaseName(capsuleClassName)
  return getSqlalchemyTableTypeFromBaseName(baseName)
def getSqlaToBaseName(sqlaTableName: str) -> str:
  if not isSqlalchemyTableTypeName(sqlaTableName):
    raise Exception(f"getSqlaToBaseName - sqlaTableName '{sqlaTableName}' " + \
                    f"does not end with {SQLA_TABLE_OBJECT_SUFFIX}")
  return sqlaTableName.removesuffix(SQLA_TABLE_OBJECT_SUFFIX)
def getSqlaToCapsuleName(sqlaTableName: str) -> str:
  baseName = getSqlaToBaseName(sqlaTableName)
  return getCapsuleClassNameFromBaseName(baseName)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Naming conventions init function to be set as attribute to capsule class
def getInitFncName(table) -> str:
  return f"init{getBaseName(table)}"
# Naming conventions consistency check function on a relationship:
def getConsistencyCheckFncName(relationshipName: str) -> str:
  return f"_ensure{getBaseNameFromString(relationshipName)}Consistency"
# Naming conventions consistency check function on a relationship:
def getSourceAndConsistencyCheckFncName(relationshipName: str) -> str:
  return f"_sourceAndEnsure{getBaseNameFromString(relationshipName)}Consistency"
# Naming conventions consistency check function over all relationships:
def getConsistencyCheckOverAllFncName() -> str:
  return f"_ensureConsistency"
def getSourceAndConsistencyCheckOverAllFncName() -> str:
  return f"_sourceAndEnsureConsistency"
def getUpdateAllRelationshipNames() -> str:
  return f"_updateAllRelationshipNames"
def getOmitIfNoneFncName(attributeName: str) -> str:
  return f'_omit_none_{attributeName}'
def getToDictFncName() -> str:
  return f"toDict"
def getToJsonFncName() -> str:
  return f"toJson"

def getFromDictFncName() -> str:
  return f"fromDict"
def getFromJsonFncName() -> str:
  return f"fromJson"

def getValidationItemsFncName() -> str:
  return f"validationItems"


def getListOfPropertyItemName(relationshipName: str):
  relationshipItemName = relationshipName.removesuffix("s")
  return getBaseNameFromString(relationshipItemName)
def getCountOfListOfPropertyFncName(relationshipName: str) -> str:
  baseName = getBaseNameFromString(relationshipName)
  return f"countOf{baseName}"
def getAppendToListOfPropertyFncName(relationshipName: str) -> str:
  attributeBaseName = getListOfPropertyItemName(relationshipName)
  return f"append{attributeBaseName}"
def getRemoveFromListOfPropertyFncName(relationshipName: str) -> str:
  attributeBaseName = getListOfPropertyItemName(relationshipName)
  return f"remove{attributeBaseName}"
def getItemFromListOfPropertyFncName(relationshipName: str) -> str:
  attributeBaseName = getListOfPropertyItemName(relationshipName)
  return f"get{attributeBaseName}"

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Naming conventions of columns defining relationships by foreign keys:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Relationship id field convention
def convertRelationshipNameToIdField(relationshipName: str) -> str:
  return relationshipName + "_id"
# All columns defining foreign keys -> name = <name of table of foreign key>'_id'
#   - the identification of such columns:
def isRelationshipIdColumnName(columnName: str) -> bool:
  return columnName.endswith("_id")
def isRelationshipIdColumn(column: sqlalchemy.Column) -> bool:
  return isRelationshipIdColumnName(columnName=column.name)
def getRelationshipIdFieldOfColumn(column: sqlalchemy.Column):
  if isRelationshipIdColumn(column=column):
    return column.name if isRelationshipIdColumn(column=column) \
                       else None
  # more precise testing might be added later
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The name of the table of the foreign key -> <name of table of foreign key>
#   - returns 'None' if column name does not end with '_id'
def convertIdFieldToRelationshipName(relationshipIdField: str) -> str:
  return relationshipIdField.removesuffix("_id")
def getColumnToRelationshipName(columnName: str) -> str:
  if isRelationshipIdColumnName(columnName=columnName):
    return convertIdFieldToRelationshipName(
        relationshipIdField = columnName)
  return None
def getRelationshipNameOfColumn(column: sqlalchemy.Column):
  return getColumnToRelationshipName(columnName=column.name)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# All columns defining foreign keys to table with a 'name' column get a dummy
# 'name' field defined on the capsule. -> <name of table of foreign key>'_name'
def convertRelationshipNameToNameField(relationshipName: str) -> str:
  return relationshipName + "_name"
def getColumnRelationshipNameField(columnName: str) -> str:
  relName =  getColumnToRelationshipName(columnName=columnName) 
  return relName if relName is None else \
     convertRelationshipNameToNameField(relName)
def getRelationshipNameFieldOfColumn(column: sqlalchemy.Column):
  return getColumnRelationshipNameField(columnName=column.name)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# All columns defining foreign keys to table with a 'name' column get an internal
# 'name' field defined on the capsule. -> '_'<name of table of foreign key>'_name'
def convertRelationshipNameToInternalNameField(relationshipName: str) -> str:
  return "_" + convertRelationshipNameToNameField(relationshipName=relationshipName) 
def getColumnRelationshipInternalNameField(columnName: str) -> str:
  relName = getColumnToRelationshipName(columnName=columnName) 
  return relName if relName is None else \
    convertRelationshipNameToInternalNameField(relName) 
def getRelationshipNameInternaFieldOfColumn(column: sqlalchemy.Column):
  return getColumnRelationshipInternalNameField(columnName=column.name)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Get the dictionary of id, name, internal name and relationship attributes based on 
#    relationship name
COL_NAME_DICT_KEY = "columnName"
REL_ATTR_DICT_KEY_ID = "idOfRelationshipColumnAttributeName"
REL_ATTR_DICT_KEY_NAME = "nameOfRelationshipColumnAttributeName"
REL_ATTR_DICT_KEY_INTERNAL_NAME = "internalNameAttributeOfRelationship"
REL_ATTR_DICT_KEY_RELATIONSHIP = "relationshipName"
def getDictOfRelationshipAttrNameFromRelationshipName(relationshipName: str) -> dict:
  result = {}
  result[REL_ATTR_DICT_KEY_ID] = convertRelationshipNameToIdField(
                                      relationshipName = relationshipName)
  result[REL_ATTR_DICT_KEY_NAME] = convertRelationshipNameToNameField(
                                      relationshipName = relationshipName)
  result[REL_ATTR_DICT_KEY_INTERNAL_NAME] = convertRelationshipNameToInternalNameField(
                                      relationshipName = relationshipName)
  result[REL_ATTR_DICT_KEY_RELATIONSHIP] = relationshipName
  return result
def getDictOfRelationshipAttrName(column: sqlalchemy.Column) -> dict:
  # result = {}
  # result[COL_NAME_DICT_KEY] = column.name
  # result[REL_ATTR_DICT_KEY_ID] = getRelationshipIdFieldOfColumn(
  #                                     column = column)
  # result[REL_ATTR_DICT_KEY_NAME] = getRelationshipNameFieldOfColumn(
  #                                     column = column)
  # result[REL_ATTR_DICT_KEY_INTERNAL_NAME] = getRelationshipNameInternaFieldOfColumn(
  #                                     column = column)
  # result[REL_ATTR_DICT_KEY_RELATIONSHIP] = getRelationshipNameFieldOfColumn(
  #                                     column = column)
  # return result


  relName = getRelationshipNameOfColumn(column = column)
  return relName if relName is None else \
     getDictOfRelationshipAttrNameFromRelationshipName(relationshipName = relName) 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Get the dictionary of id, name, internal name and relationship attributes based on 
#    columnName
def getDictOfColumnAttributeNamesOfTable(table) -> dict:
  result = {}
  for column in table.columns:
    columnDict = {}
    columnDict[COL_NAME_DICT_KEY] = column.name
    columnDict[REL_ATTR_DICT_KEY_ID] = getRelationshipIdFieldOfColumn(
                                        column = column)
    columnDict[REL_ATTR_DICT_KEY_NAME] = getRelationshipNameFieldOfColumn(
                                        column = column)
    columnDict[REL_ATTR_DICT_KEY_INTERNAL_NAME] = getRelationshipNameInternaFieldOfColumn(
                                        column = column)
    columnDict[REL_ATTR_DICT_KEY_RELATIONSHIP] = getRelationshipNameOfColumn(
                                        column = column)
    result[column.name] = columnDict
  return result


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Identification of the relationship defined on the basis of the '_id' column
#    - RELATIONSHIP MUST BE NAMED AND DEFINED on the sqlalchemy table
def getRelationshipOfName(relationshipName: str, 
                          table: sqlalchemy.Table, 
                          callingGlobals) -> sqlalchemy.Relationship:  
  sqlalchemyTableTypeName = getSqlalchemyTableTypeName(table)
  sqlalchemyTableType = callingGlobals[sqlalchemyTableTypeName]
  # if table.name == 'asset_class':
  #   for relationship in sqlalchemyTableType.__mapper__.relationships:
  #     print(f"table name: {table.name} - relationship.key: {relationship.key}")
  try:
    return sqlalchemyTableType.__mapper__.relationships[relationshipName]
  except:
    return None
def getRelationship(table: sqlalchemy.Table, 
                    column: sqlalchemy.Column,
                    callingGlobals) -> sqlalchemy.Relationship:
  relName = getRelationshipNameOfColumn(column=column)
  if relName is None: return None
  relationship = getRelationshipOfName(relationshipName = relName,
                                       table = table, 
                                       callingGlobals = callingGlobals)
  if relationship is None:
    raise Exception(f"Could not identify relationship implicitly defined on {table.name} by " + \
                    f" column '{column.name}'.")
  return relationship
def getRelationshipTypeNameOfName(relationshipName: str, 
                                  table: sqlalchemy.Table, 
                                  callingGlobals) -> sqlalchemy.Relationship:
  relationship = getRelationshipOfName(relationshipName = relationshipName,
                                       table = table,
                                       callingGlobals = callingGlobals)
  if relationship is None:
    raise Exception(f"Could not identify relationship named '{relationshipName}' on {table.name}.")
  return relationship.mapper.class_.__name__
def getRelationshipTypeName(table: sqlalchemy.Table, 
                            column: sqlalchemy.Column,
                            callingGlobals):
  relName = getRelationshipNameOfColumn(column=column)
  if relName is None: return None
  return getRelationshipTypeNameOfName(relationshipName = relName,
                                       table = table, 
                                       callingGlobals = callingGlobals)
def getRelationshipSqlalchemyTypeOfName(relationshipName: str, 
                              table: sqlalchemy.Table, 
                              callingGlobals) -> sqlalchemy.Relationship:  
  relationshipTypeName = getRelationshipTypeNameOfName(relationshipName = relationshipName,
                                       table = table,
                                       callingGlobals = callingGlobals)
  return callingGlobals[relationshipTypeName]

def getRelationshipCapsuleTypeOfName(relationshipName: str, 
                                     table: sqlalchemy.Table, 
                                     callingGlobals) -> sqlalchemy.Relationship:  
  # print(f"getRelationshipCapsuleTypeOfName - table: {table.name:<30} - relationshipName: {relationshipName} ")
  relationshipTypeName = getRelationshipTypeNameOfName(
                                      relationshipName = relationshipName,
                                      table = table,
                                      callingGlobals = callingGlobals)
  relationshipCapsuleTypeName = getSqlaToCapsuleName(sqlaTableName = relationshipTypeName)
  return callingGlobals[relationshipCapsuleTypeName]

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Identification of the relationship properties
def isDisplayList(sqlalchemyTableType,
                  relationship: sqlalchemy.Relationship) -> bool:
  if hasattr(sqlalchemyTableType, "_display_lists"):
    displayLists = sqlalchemyTableType._display_lists
    return relationship.key in displayLists
  else:
    return False
  

