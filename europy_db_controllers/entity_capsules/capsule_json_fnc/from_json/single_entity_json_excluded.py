import typing, json

import sqlalchemy.orm as sqlalchemy_orm

from europy_db_controllers.entity_capsules import _capsule_base

from europy_db_controllers.entity_capsules.capsule_json_fnc.utils import uuid_encoder
from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import consistency_checks

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

DEBUG_CAPSULE_TYPE = "TxTypeToParameterMapCapsule"

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This function adds a single related entity if it is identified in the json by its name only
#     Say, the json DOES NOT provide a full specification of the relationship entity.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def getExcludedFromJsonSingleRelatedEntity(
              session: sqlalchemy_orm.Session,
              capsuleType: typing.Type[T],
              capsuleDict: typing.Dict[str, any],
              relationshipEntitiesCatalog: typing.Dict[str, dict],
              relationshipName: str,
              relationshipNameAttributeName: str,
              relationshipCapsuleClass: typing.Type
              ) -> any:
  thisRelationshipEntitiesCatalog = relationshipEntitiesCatalog[relationshipName]
  # Relationship is not available on the dict as as sub-dict
  #   but is identified by it's name
  #   Such name must be identifiable on the db (or as new/dirty)
  if hasattr(relationshipCapsuleClass, 'name'):
    consistency_checks.ensureKeyInDict(key = relationshipNameAttributeName,
                                      capsuleDict = capsuleDict,
                                      className = capsuleType.__name__)
    relationshipEntityName = capsuleDict[relationshipNameAttributeName]
    # relationshipEntityName might not be defined -> do nothing
    if relationshipEntityName is None: return None
    if not relationshipEntityName in thisRelationshipEntitiesCatalog:
      # check if an entity with the relationship's name exists on db
      #     if yes: include the the relationshipEntity in the catalog of relationship entities
      #     if  no: do not include as it will cause and error
      if not relationshipCapsuleClass.nameExists(
                        session=session, 
                        name=relationshipEntityName):
        jsonSpec = json.dumps(capsuleDict, cls=uuid_encoder.UUIDEncoder, indent = 4)
        raise Exception(f"Badly specified relationship name on {capsuleType.__name__}:\n" + \
                        f"No relationship with such name identified on db or session.\n" + \
                        f"Relationship           : {relationshipName}\n" + \
                        f"Name provided          : {relationshipEntityName}\n" + \
                        f"Provided on attribute  : {relationshipNameAttributeName}\n" + \
                        f"json spec: \n" + \
                          jsonSpec)
      else:
        result = relationshipCapsuleClass(
                        session = session, 
                        name = relationshipEntityName)
        thisRelationshipEntitiesCatalog[relationshipEntityName] = result
        return result
    else:
      return thisRelationshipEntitiesCatalog[relationshipEntityName]