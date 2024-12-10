from __future__ import annotations

import uuid, \
       typing, datetime, sqlalchemy, enum
from sqlalchemy.dialects import postgresql as sqlalchemy_pg
from sqlalchemy import orm as sqlalchemy_orm
from sqlalchemy.ext import hybrid as sqlalchemy_hyb
from sqlalchemy.ext import declarative as sqlalchemy_decl


from europy_db_controllers.entity_capsules import _capsule_base

PYTHON_BUILTINS_MODULE = "builtins"
INIT_ENFORCE_NOT_NEW_OR_DIRTY_FLAG = "enforceNotNewOrDirty"

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)


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
            sqlalchemy.INTEGER: int,
            sqlalchemy.Enum: enum.Enum}

def getPythonType(sqlalchemyType) -> str:
    """
    Converts a SQLAlchemy type to its corresponding Python type string representation.
    
    Args:
        sqlalchemyType: SQLAlchemy column type
        
    Returns:
        str: Python type as string, with module prefix for non-builtin types
        
    Raises:
        Exception: If SQLAlchemy type is not found in mapping dictionary
    """
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
# Information on the set of sqlalchemyTableType columns
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Test if sqlalchemyTableType has a 'name' column
def hasName(sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]) -> bool:
    """
    Checks if a SQLAlchemy Declarative type has a 'name' column.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        bool: True if table has 'name' column, False otherwise
    """
    for column in sqlalchemyTableType.__table__.columns:
        if column.name == "name":
            return True
    return False
# Sqlalchemy columns less those used to track changes (created & modified at)
#   - might be used to hide away further system related information (user etc.)
def getNonChangeTrackColumns(sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]):
    """
    Returns all columns except those used for change tracking (created_at, modified_at, etc.).
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        list: List of SQLAlchemy Column objects excluding change tracking columns
    """
    result = []
    for column in sqlalchemyTableType.__table__.columns:
        if not column.name in sqlalchemyTableType._changeTrackFields:
            result.append(column)
    return result
def getNonChangeTrackColumnNames(
                          sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]
                          ) -> typing.List[str]:
    """
    Returns a list of column names excluding change tracking columns.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        callingGlobals: Global variables
        
    Returns:
        list: List of column names excluding change tracking columns
    """
    columnList = getNonChangeTrackColumns(sqlalchemyTableType = sqlalchemyTableType)
    return [column.name for column in columnList] 
# Test if sqlalchemy column is a base column (see _capsule_base '_base_columns')
def isBaseColumn(capsuleType: _capsule_base.CapsuleBase, 
                 column) -> bool:
    """
    Checks if a column is a base column.
    
    Args:
        capsuleType: Capsule class
        column: SQLAlchemy column object
        
    Returns:
        bool: True if column is a base column, False otherwise
    """
    baseColumnNames = capsuleType._base_columns
    return column.name in baseColumnNames


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Naming conventions for capsule and sqlalchemyTableType objects
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The base name of the object -> derived from the sqlalchemyTableType name
#     - sqlalchemyTableType names must be unique
#     - no schema names used to differentiate
def getBaseNameFromString(name: str) -> str:
    """
    Converts a string to its base name format.
    
    Args:
        name: String to be converted
        
    Returns:
        str: Base name formatted string
    """
    result = ''.join(x.capitalize() for x in name.split("_"))
    return result
def getBaseName(sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]) -> str:
    """
    Returns the base name of the SQLAlchemy Declarative type.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        str: Base name of the SQLAlchemy Declarative type
    """
    return getBaseNameFromString(name = getattr(sqlalchemyTableType.__table__, 'name'))
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The name of the capsule object -> base name plus 'Capsule'-suffix" "
CAPSULE_OBJECT_SUFFIX = "Capsule"
def getCapsuleClassNameFromBaseName(baseName: str) -> str:
    """
    Returns the capsule class name from the base name.
    
    Args:
        baseName: Base name of the capsule
        
    Returns:
        str: Capsule class name
    """
    return baseName + CAPSULE_OBJECT_SUFFIX
def getCapsuleClassName(sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]) -> str:
    """
    Returns the capsule class name.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        str: Capsule class name
    """
    baseName = getBaseName(sqlalchemyTableType=sqlalchemyTableType) 
    return getCapsuleClassNameFromBaseName(baseName) 
def isCapsuleClassName(className: str) -> bool:
    """
    Checks if a class name is a capsule class name.
    
    Args:
        className: Class name to be checked
        
    Returns:
        bool: True if class name is a capsule class name, False otherwise
    """
    return className.endswith(CAPSULE_OBJECT_SUFFIX)
  # more precise testing might be added later
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The name of the sqlalchemyTableType object -> base name plus 'Table'-suffix
SQLA_TABLE_OBJECT_SUFFIX = "Table"
def getSqlalchemyTableTypeFromBaseName(baseName: str) -> str:
    """
    Returns the SQLAlchemy Declarative type name from the base name.
    
    Args:
        baseName: Base name of the table
        
    Returns:
        str: SQLAlchemy Declarative type name
    """
    return baseName + SQLA_TABLE_OBJECT_SUFFIX
def getSqlalchemyTableTypeName(table: sqlalchemy.Table) -> str:
    """
    Returns the SQLAlchemy Declarative type name.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        str: SQLAlchemy Declarative type name
    """
    baseName = getBaseNameFromString(name = getattr(table, 'name'))
    return getSqlalchemyTableTypeFromBaseName(baseName) 
def isSqlalchemyTableTypeName(className: str) -> bool:
    """
    Checks if a class name is a SQLAlchemy Declarative type name.
    
    Args:
        className: Class name to be checked
        
    Returns:
        bool: True if class name is a SQLAlchemy Declarative type name, False otherwise
    """
    return className.endswith(SQLA_TABLE_OBJECT_SUFFIX)
  # more precise testing might be added later
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Conversion between base, capsule and sqlalchemyTableType object names
def getCapsuleToBaseName(capsuleClassName: str) -> str:
    """
    Converts a capsule class name to its base name.
    
    Args:
        capsuleClassName: Capsule class name
        
    Returns:
        str: Base name
    """
    if not isCapsuleClassName(capsuleClassName):
        raise Exception(f"getCapsuleToBaseName - capsuleClassName '{capsuleClassName}' " + \
                        f"does not end with {CAPSULE_OBJECT_SUFFIX}")
    return capsuleClassName.removesuffix(CAPSULE_OBJECT_SUFFIX)
def getCapsuleToSqlaName(capsuleClassName: str) -> str:
    """
    Converts a capsule class name to its SQLAlchemy Declarative type name.
    
    Args:
        capsuleClassName: Capsule class name
        
    Returns:
        str: SQLAlchemy Declarative type name
    """
    baseName = getCapsuleToBaseName(capsuleClassName)
    return getSqlalchemyTableTypeFromBaseName(baseName)
def getSqlaToBaseName(sqlaTableName: str) -> str:
    """
    Converts a SQLAlchemy Declarative type name to its base name.
    
    Args:
        sqlaTableName: SQLAlchemy Declarative type name
        
    Returns:
        str: Base name
    """
    if not isSqlalchemyTableTypeName(sqlaTableName):
        raise Exception(f"getSqlaToBaseName - sqlaTableName '{sqlaTableName}' " + \
                        f"does not end with {SQLA_TABLE_OBJECT_SUFFIX}")
    return sqlaTableName.removesuffix(SQLA_TABLE_OBJECT_SUFFIX)
def getSqlaToCapsuleName(sqlaTableName: str) -> str:
    """
    Converts a SQLAlchemy Declarative type name to its capsule class name.
    
    Args:
        sqlaTableName: SQLAlchemy Declarative type name
        
    Returns:
        str: Capsule class name
    """
    baseName = getSqlaToBaseName(sqlaTableName)
    return getCapsuleClassNameFromBaseName(baseName)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Naming conventions init function to be set as attribute to capsule class
def getInitFncName(sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]) -> str:
    """
    Returns the initialization function name for a SQLAlchemy Declarative type.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        str: Initialization function name
    """
    return f"init{getBaseName(sqlalchemyTableType=sqlalchemyTableType)}"
# Naming conventions consistency check function on a relationship:
def getConsistencyCheckFncName(relationshipName: str) -> str:
    """
    Returns the consistency check function name for a relationship.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: Consistency check function name
    """
    return f"_ensure{getBaseNameFromString(relationshipName)}Consistency"
# Naming conventions consistency check function on a relationship:
def getSourceAndConsistencyCheckFncName(relationshipName: str) -> str:
    """
    Returns the source and consistency check function name for a relationship.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: Source and consistency check function name
    """
    return f"_sourceAndEnsure{getBaseNameFromString(relationshipName)}Consistency"
# Naming conventions consistency check function over all relationships:
def getConsistencyCheckOverAllFncName() -> str:
    """
    Returns the consistency check function name for all relationships.
    
    Returns:
        str: Consistency check function name
    """
    return f"_ensureConsistency"
def getSourceAndConsistencyCheckOverAllFncName() -> str:
    """
    Returns the source and consistency check function name for all relationships.
    
    Returns:
        str: Source and consistency check function name
    """
    return f"_sourceAndEnsureConsistency"
def getUpdateAllRelationshipNames() -> str:
    """
    Returns the function name for updating all relationship names.
    
    Returns:
        str: Update function name
    """
    return f"_updateAllRelationshipNames"
def getOmitIfNoneFncName(attributeName: str) -> str:
    """
    Returns the function name for omitting an attribute if it is None.
    
    Args:
        attributeName: Attribute name
        
    Returns:
        str: Omit function name
    """
    return f'_omit_none_{attributeName}'
def getToDictFncName() -> str:
    """
    Returns the function name for converting to a dictionary.
    
    Returns:
        str: To dictionary function name
    """
    return f"toDict"
def getToJsonFncName() -> str:
    """
    Returns the function name for converting to JSON.
    
    Returns:
        str: To JSON function name
    """
    return f"toJson"

def getFromDictFncName() -> str:
    """
    Returns the function name for creating from a dictionary.
    
    Returns:
        str: From dictionary function name
    """
    return f"fromDict"
def getFromJsonFncName() -> str:
    """
    Returns the function name for creating from JSON.
    
    Returns:
        str: From JSON function name
    """
    return f"fromJson"

def getValidationItemsFncName() -> str:
    """
    Returns the function name for getting validation items.
    
    Returns:
        str: Validation items function name
    """
    return f"validationItems"


def getListOfPropertyItemName(relationshipName: str):
    """
    Returns the item name for a list of properties based on the relationship name.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: Item name
    """
    relationshipItemName = relationshipName.removesuffix("s")
    return getBaseNameFromString(relationshipItemName)
def getCountOfListOfPropertyFncName(relationshipName: str) -> str:
    """
    Returns the function name for counting items in a list of properties.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: Count function name
    """
    baseName = getBaseNameFromString(relationshipName)
    return f"countOf{baseName}"
def getAppendToListOfPropertyFncName(relationshipName: str) -> str:
    """
    Returns the function name for appending to a list of properties.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: Append function name
    """
    attributeBaseName = getListOfPropertyItemName(relationshipName)
    return f"append{attributeBaseName}"
def getRemoveFromListOfPropertyFncName(relationshipName: str) -> str:
    """
    Returns the function name for removing from a list of properties.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: Remove function name
    """
    attributeBaseName = getListOfPropertyItemName(relationshipName)
    return f"remove{attributeBaseName}"
def getItemFromListOfPropertyFncName(relationshipName: str) -> str:
    """
    Returns the function name for getting an item from a list of properties.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: Get item function name
    """
    attributeBaseName = getListOfPropertyItemName(relationshipName)
    return f"get{attributeBaseName}"

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Identification of columns and hybrid properties relevant for capsule attributes:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def getHybridPropertyNames(sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]) -> typing.List[str]:
    """
    Returns a list of hybrid property names for a SQLAlchemy Declarative type.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        list: List of hybrid property names
    """
    result = []
    for key, objType in vars(sqlalchemyTableType.__table__).items():
        if isinstance(objType, sqlalchemy_hyb.hybrid_property):
            result.append(key)
    return result
def getSqlalchemyColumnsAndColumnLikeProperties(capsuleType: T
                                                ) -> typing.Dict[str, typing.List[str, bool, bool]]:
    """
    Returns a dictionary of SQLAlchemy columns and column-like properties.
    
    Args:
        capsuleType: Capsule class
        
    Returns:
        dict: Dictionary of column names and their properties
    """
    #       -> dict{<nameOfItem>: [<nameOfItem>, <isHybridProperty>, <hasSetter]}

    doPrint = (capsuleType.__name__ == "MarketAndForwardTransactionCapsule")
    # if doPrint:
    #   print(f"\n           getSqlalchemyColumnsAndColumnLikeProperties {capsuleType.__name__}")

    result = {}
    relationshipLikeHybridPropertiesToRemove = []
    sqlalchemyTableType = capsuleType.sqlalchemyTableType
    columnNamesList = getNonChangeTrackColumnNames(sqlalchemyTableType=sqlalchemyTableType)
    for columnName in columnNamesList:
        column = sqlalchemyTableType.__table__.columns[columnName]
        if isBaseColumn(capsuleType = capsuleType,
                        column = column): continue # skips 'id' and 'name' columns
        # if doPrint:
        #   print(f"              adding to result column: {columnName}")
        isHybridProperty = False # columns are not hybrid properties
        hasSetter = True # all non-id, non-name columns have setters
        result[columnName] = [columnName, isHybridProperty, hasSetter]
    for itemName, objType in vars(sqlalchemyTableType).items():
        # if doPrint:
        #   print(f"              checking for hybrid property: {itemName} of type {type(objType)}")
        if not isinstance(objType, sqlalchemy_hyb.hybrid_property): continue # if not hybrid property do nothing
        hybridPropertyName = itemName
        if hybridPropertyName in result: continue # skip if hybrid property is an override
        if isRelationshipIdColumnName(columnName = hybridPropertyName): 
            # the relationship like hybrid properties must be taken out before return
            relationShipName = getColumnToRelationshipName(columnName = hybridPropertyName)
            relationshipLikeHybridPropertiesToRemove.append(relationShipName)            
        # if doPrint:
        #   print(f"              adding to result hybrid property: {hybridPropertyName}")
        isHybridProperty = True # all hybrid properties are hybrid, of course
        hasSetter = hasattr(objType, 'fset') and objType.fset is not None
        result[hybridPropertyName] = [hybridPropertyName, isHybridProperty, hasSetter]
    # remove the relationship like hybrid properties
    # for relationShipLikeHybridProperty in relationshipLikeHybridPropertiesToRemove:
    #     del result[relationShipLikeHybridProperty]
    return result

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Naming conventions of columns defining relationships by foreign keys:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Relationship id field convention
def convertRelationshipNameToIdField(relationshipName: str) -> str:
    """
    Converts a relationship name to its corresponding ID field name.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: ID field name
    """
    return relationshipName + "_id"
# All columns defining foreign keys -> name = <name of sqlalchemyTable of foreign key>'_id'
#   - the identification of such columns:
def isRelationshipIdColumnName(columnName: str) -> bool:
    """
    Checks if a column name is a relationship ID column name.
    
    Args:
        columnName: Column name
        
    Returns:
        bool: True if column name is a relationship ID column name, False otherwise
    """
    return columnName.endswith("_id")
def isRelationshipIdColumn(column: sqlalchemy.Column) -> bool:
    """
    Checks if a column is a relationship ID column.
    
    Args:
        column: SQLAlchemy column object
        
    Returns:
        bool: True if column is a relationship ID column, False otherwise
    """
    return isRelationshipIdColumnName(columnName=column.name)
def getRelationshipIdFieldOfColumn(column: sqlalchemy.Column):
    """
    Returns the relationship ID field of a column.
    
    Args:
        column: SQLAlchemy column object
        
    Returns:
        str: Relationship ID field name or None
    """
    if isRelationshipIdColumn(column=column):
        return column.name if isRelationshipIdColumn(column=column) \
                       else None
  # more precise testing might be added later
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The name of the sqlalchemyTable of the foreign key -> <name of sqlalchemyTable of foreign key>
#   - returns 'None' if column name does not end with '_id'
def convertIdFieldToRelationshipName(relationshipIdField: str) -> str:
    """
    Converts a relationship ID field to its corresponding relationship name.
    
    Args:
        relationshipIdField: Relationship ID field name
        
    Returns:
        str: Relationship name
    """
    return relationshipIdField.removesuffix("_id")
def getColumnToRelationshipName(columnName: str) -> str:
    """
    Returns the relationship name for a column name.
    
    Args:
        columnName: Column name
        
    Returns:
        str: Relationship name or None
    """
    if isRelationshipIdColumnName(columnName=columnName):
        return convertIdFieldToRelationshipName(
            relationshipIdField = columnName)
    return None
def getRelationshipNameOfColumn(column: sqlalchemy.Column):
    """
    Returns the relationship name of a column.
    
    Args:
        column: SQLAlchemy column object
        
    Returns:
        str: Relationship name or None
    """
    return getColumnToRelationshipName(columnName=column.name)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# All columns defining foreign keys to sqlalchemyTable with a 'name' column get a dummy
# 'name' field defined on the capsule. -> <name of sqlalchemyTable of foreign key>'_name'
def convertRelationshipNameToNameField(relationshipName: str) -> str:
    """
    Converts a relationship name to its corresponding name field.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: Name field
    """
    return relationshipName + "_name"
def getColumnRelationshipNameField(columnName: str) -> str:
    """
    Returns the relationship name field for a column name.
    
    Args:
        columnName: Column name
        
    Returns:
        str: Relationship name field or None
    """
    relName =  getColumnToRelationshipName(columnName=columnName) 
    return relName if relName is None else \
         convertRelationshipNameToNameField(relName)
def getRelationshipNameFieldOfColumn(column: sqlalchemy.Column):
    """
    Returns the relationship name field of a column.
    
    Args:
        column: SQLAlchemy column object
        
    Returns:
        str: Relationship name field or None
    """
    return getColumnRelationshipNameField(columnName=column.name)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# All columns defining foreign keys to sqlalchemyTable with a 'name' column get an internal
# 'name' field defined on the capsule. -> '_'<name of sqlalchemyTable of foreign key>'_name'
def convertRelationshipNameToInternalNameField(relationshipName: str) -> str:
    """
    Converts a relationship name to its corresponding internal name field.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        str: Internal name field
    """
    return "_" + convertRelationshipNameToNameField(relationshipName=relationshipName) 
def getColumnRelationshipInternalNameField(columnName: str) -> str:
    """
    Returns the internal name field for a column name.
    
    Args:
        columnName: Column name
        
    Returns:
        str: Internal name field or None
    """
    relName = getColumnToRelationshipName(columnName=columnName) 
    return relName if relName is None else \
        convertRelationshipNameToInternalNameField(relName) 
def getRelationshipNameInternaFieldOfColumn(column: sqlalchemy.Column):
    """
    Returns the internal name field of a column.
    
    Args:
        column: SQLAlchemy column object
        
    Returns:
        str: Internal name field or None
    """
    return getColumnRelationshipInternalNameField(columnName=column.name)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Get the dictionary of id, name, internal name and relationship attributes based on 
#    relationship name
COL_NAME_DICT_KEY = "columnName"
REL_ATTR_DICT_KEY_ID = "idOfRelationshipColumnAttributeName"
REL_ATTR_DICT_KEY_NAME = "nameOfRelationshipColumnAttributeName"
REL_ATTR_DICT_KEY_INTERNAL_NAME = "internalNameAttributeOfRelationship"
REL_ATTR_DICT_KEY_RELATIONSHIP = "relationshipName"
def getDictOfAttributeNamingConventionsFromRelationshipName(relationshipName: str) -> dict:
    """
    Returns a dictionary of attribute naming conventions based on a relationship name.
    
    Args:
        relationshipName: Relationship name
        
    Returns:
        dict: Dictionary of attribute naming conventions
    """
    result = {}
    result[REL_ATTR_DICT_KEY_ID] = convertRelationshipNameToIdField(
                                        relationshipName = relationshipName)
    result[REL_ATTR_DICT_KEY_NAME] = convertRelationshipNameToNameField(
                                        relationshipName = relationshipName)
    result[REL_ATTR_DICT_KEY_INTERNAL_NAME] = convertRelationshipNameToInternalNameField(
                                        relationshipName = relationshipName)
    result[REL_ATTR_DICT_KEY_RELATIONSHIP] = relationshipName
    return result
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Get the dictionary of id, name, internal name and relationship attributes based on 
#    columnName
def getDictOfColumnAttributeNamesOfTable(sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]) -> dict:
    """
    Returns a dictionary of column attribute names for a SQLAlchemy Declarative type.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        dict: Dictionary of column attribute names
    """
    result = {}
    for column in sqlalchemyTableType.__table__.columns:
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
#    - RELATIONSHIP MUST BE NAMED AND DEFINED on the sqlalchemyTableType
def getRelationshipOfName(relationshipName: str, 
                          sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]
                          ) -> sqlalchemy.Relationship:  
    """
    Returns the relationship object for a given relationship name.
    
    Args:
        relationshipName: Relationship name
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        sqlalchemy.Relationship: Relationship object or None
    """
    try:
        return sqlalchemyTableType.__mapper__.relationships[relationshipName]
    except:
        return None
def getRelationship(sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta], 
                    column: sqlalchemy.Column) -> sqlalchemy.Relationship:
    """
    Returns the relationship object for a given column.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        column: SQLAlchemy column object
        
    Returns:
        sqlalchemy.Relationship: Relationship object or None
    """
    relName = getRelationshipNameOfColumn(column=column)
    if relName is None: return None
    relationship = getRelationshipOfName(relationshipName = relName,
                                         sqlalchemyTableType = sqlalchemyTableType)
    if relationship is None:
        raise Exception(f"Could not identify relationship implicitly defined on {sqlalchemyTableType.__table__.name} by " + \
                        f" column '{column.name}'.")
    return relationship
def getRelationshipTypeNameOfName(relationshipName: str, 
                                  sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta]) -> sqlalchemy.Relationship:
    """
    Returns the relationship type name for a given relationship name.
    
    Args:
        relationshipName: Relationship name
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        
    Returns:
        str: Relationship type name
    """
    # for relationship in sqlalchemyTableType.__mapper__.relationships:
    #     print(f"relationship.key: {relationship.key}")
    relationship = getRelationshipOfName(relationshipName = relationshipName,
                                         sqlalchemyTableType = sqlalchemyTableType)
    if relationship is None:
        raise Exception(f"Could not identify relationship named '{relationshipName}' on {sqlalchemyTableType.__table__.name}.")
    return relationship.mapper.class_.__name__
def getRelationshipTypeName(sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta], 
                            column: sqlalchemy.Column):
    """
    Returns the relationship type name for a given column.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        column: SQLAlchemy column object
        
    Returns:
        str: Relationship type name or None
    """
    relName = getRelationshipNameOfColumn(column=column)
    if relName is None: return None
    return getRelationshipTypeNameOfName(relationshipName = relName,
                                         sqlalchemyTableType = sqlalchemyTableType)
def getRelationshipSqlalchemyTypeOfName(relationshipName: str, 
                              sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta], 
                              callingGlobals) -> sqlalchemy.Relationship:  
    """
    Returns the SQLAlchemy type of a relationship for a given relationship name.
    
    Args:
        relationshipName: Relationship name
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        callingGlobals: Global variables
        
    Returns:
        sqlalchemy.Relationship: SQLAlchemy type of the relationship
    """
    relationshipTypeName = getRelationshipTypeNameOfName(relationshipName = relationshipName,
                                         sqlalchemyTableType = sqlalchemyTableType)
    return callingGlobals[relationshipTypeName]

def getRelationshipCapsuleTypeOfName(relationshipName: str, 
                                     sqlalchemyTableType: typing.Type[sqlalchemy_decl.DeclarativeMeta],
                                     callingGlobals) -> sqlalchemy.Relationship:  
    """
    Returns the capsule type of a relationship for a given relationship name.
    
    Args:
        relationshipName: Relationship name
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        callingGlobals: Global variables
        
    Returns:
        sqlalchemy.Relationship: Capsule type of the relationship
    """
    relationshipTypeName = getRelationshipTypeNameOfName(
                                        relationshipName = relationshipName,
                                        sqlalchemyTableType = sqlalchemyTableType)
    relationshipCapsuleTypeName = getSqlaToCapsuleName(sqlaTableName = relationshipTypeName)
    return callingGlobals[relationshipCapsuleTypeName]

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Identification of the relationship properties
def isDisplayList(sqlalchemyTableType,
                  relationship: sqlalchemy.Relationship) -> bool:
    """
    Checks if a relationship is part of the display list for a SQLAlchemy Declarative type.
    
    Args:
        sqlalchemyTableType: SQLAlchemy Declarative Meta class
        relationship: SQLAlchemy relationship object
        
    Returns:
        bool: True if relationship is part of the display list, False otherwise
    """
    if hasattr(sqlalchemyTableType, "_display_lists"):
        displayLists = sqlalchemyTableType._display_lists
        return relationship.key in displayLists
    else:
        return False
  

