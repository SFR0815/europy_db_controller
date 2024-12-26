from __future__ import annotations

import typing, sqlalchemy
from sqlalchemy.ext import declarative as sqlalchemy_decl


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
