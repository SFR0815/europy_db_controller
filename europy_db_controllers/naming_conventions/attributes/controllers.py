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