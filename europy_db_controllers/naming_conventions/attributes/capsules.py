from __future__ import annotations

import typing, sqlalchemy
from sqlalchemy.ext import declarative as sqlalchemy_decl

from europy_db_controllers.naming_conventions.methods.capsules import getBaseName
from europy_db_controllers.naming_conventions.methods.capsules import getBaseNameFromString
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
