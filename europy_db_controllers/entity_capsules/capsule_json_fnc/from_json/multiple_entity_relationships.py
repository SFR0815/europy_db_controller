import typing, json, uuid

import sqlalchemy.orm as sqlalchemy_orm

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base

from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import constants, consistency_checks

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Methods covering relationships on the n-side of (1 to n) relationships
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def getIncludedInJsonMultipleRelatedEntities(
              session: sqlalchemy_orm.Session,
              capsuleType: typing.Type[T],
              capsuleDict: dict[str, any],
              relationshipName: str,
              relationshipCapsuleClass: any) -> typing.List[any]:
  consistency_checks.ensureKeyInDict(key = relationshipName,
                                    capsuleDict = capsuleDict,
                                    className = capsuleType.__name__)
  result: typing.List[any] = []
  listDictionary = capsuleDict[relationshipName]
  if len(listDictionary) == 0:
    pass
  else:
    for pos in range(0, len(capsuleDict[relationshipName])):
      capsuleDictEntity = capsuleDict[relationshipName][pos]
      relationshipEntity = getattr(relationshipCapsuleClass, constants.NAME_OF_DICT_FNC)(
                session = session,
                capsuleDict = capsuleDictEntity)
      # no adding to session as this is done in fromDict
      result.append(relationshipEntity)
  return result 

def addIncludedInJsonMultipleRelatedEntities(
              session: sqlalchemy_orm.Session,
              capsuleType: typing.Type[T],
              capsuleDict: dict[str, any],
              resultEntity: T,
              callingGlobals: typing.Dict[str, any]) -> None:
  sqlalchemyTableType = resultEntity.sqlalchemyTableType
  for relationship in sqlalchemyTableType.__mapper__.relationships:
    relationshipName = relationship.key
    sqlalchemyTableType = capsuleType.sqlalchemyTableType
    relationshipCapsuleClass = _capsule_utils.getRelationshipCapsuleTypeOfName(
                    relationshipName = relationshipName,
                    sqlalchemyTableType = sqlalchemyTableType,
                    callingGlobals = callingGlobals)
    appendToListFncName = _capsule_utils.getAppendToListOfPropertyFncName(
              relationshipName = relationshipName)
    if relationship.uselist:
      sqlalchemyTableType = capsuleType.sqlalchemyTableType
      isDisplayList = _capsule_utils.isDisplayList(sqlalchemyTableType = sqlalchemyTableType,
                                                  relationshipName = relationship.key)
      if not isDisplayList:
        relationshipEntitiesList = getIncludedInJsonMultipleRelatedEntities(
                                      session = session,
                                      capsuleType = capsuleType,
                                      capsuleDict = capsuleDict,
                                      relationshipName = relationshipName,
                                      relationshipCapsuleClass = relationshipCapsuleClass)
        for relationshipEntity in relationshipEntitiesList:
          getattr(resultEntity, appendToListFncName)(relationshipEntity)


