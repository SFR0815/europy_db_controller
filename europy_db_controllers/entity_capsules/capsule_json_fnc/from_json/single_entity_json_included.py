import typing, json, uuid

import sqlalchemy.orm as sqlalchemy_orm
from sqlalchemy.ext.declarative import DeclarativeMeta as sqlalchemy_decl

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base

from europy_db_controllers.entity_capsules.capsule_json_fnc.utils import uuid_encoder
from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import constants, consistency_checks

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# This function adds a single related entity if it is identified in the json by all its attributes
#     Say, the json DOES provide a FULL SPECIFICATION of the relationship entity.
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def getIncludedInJsonSingleRelatedEntity(
              session: sqlalchemy_orm.Session,
              capsuleType: typing.Type[T],
              capsuleDict: dict[str, any],
              relationshipName: str,
              relationshipNameAttributeName: str,
              relationshipCapsuleClass: typing.Type,
              resultEntity: T
              ) -> any:
  # Relationship is available as a sub-dict within the capsuleDict provided
  consistency_checks.ensureKeyInDict(key = relationshipName,
                                    capsuleDict = capsuleDict,
                                    className = capsuleType.__name__)
  relationshipDict = capsuleDict[relationshipName]
  if relationshipDict is None or len(relationshipDict) == 0: return None 
  # identify if relationship has been identified in previous initialization of 
  #   the capsule, see above [result = capsuleType(**initParameters)]
  isIdentifiedRelationship = not getattr(resultEntity, relationshipName) is None
  if isIdentifiedRelationship:
    idOnRelationshipDict = relationshipDict['id']
    #fix: using 'relationshipName' for get the id field of the relationshipEntity
    #     e.g. relationship_id_field_name = relationshipName + _id
    relationship_id_field_name = relationshipName + '_id'
    relationshipIdOnMainCapsule = getattr(resultEntity, relationship_id_field_name)
    # if relationship is not identified by name but by id check id consistency
    if not idOnRelationshipDict is None:
      if type(idOnRelationshipDict) is uuid.UUID:
        idOnRelationshipDict = str(idOnRelationshipDict)
      if idOnRelationshipDict != str(relationshipIdOnMainCapsule):
        # raise exception if the id of the relationship in dict is different from the 
        #   id in the relationship's id on the identified entity
        jsonSpec = json.dumps(capsuleDict, cls=uuid_encoder.UUIDEncoder, indent = 4)
        raise Exception(f"Badly specified relationship id on {capsuleType.__name__}:\n" + \
                        f"Id on capsule: {relationshipIdOnMainCapsule} - type: {type(relationshipIdOnMainCapsule)}\n" + \
                        f"Id on relationship dict: {idOnRelationshipDict} - type: {type(idOnRelationshipDict)}\n" + \
                        f"Name of relationship: {relationshipName}\n" + \
                        f"json spec: \n" + \
                          jsonSpec)
        # FIXME: spec test for this error
    # if relationship has a name: check if name is not consistent with the relationships 
    #    name on db.
    #    Changing names of relationships is not permissible when creating the parent 
    #    entity form a dict.  
    if hasattr(relationshipCapsuleClass, 'name'):
      nameOnRelationshipDict = relationshipDict['name']
      relationshipNameOnMainCapsule = getattr(result, relationshipNameAttributeName)
      if nameOnRelationshipDict != relationshipNameOnMainCapsule:
        # raise exception if the name of the relationship in dict is different from the 
        #   name in the relationship's id on the identified entity
        jsonSpec = json.dumps(capsuleDict, cls=uuid_encoder.UUIDEncoder, indent = 4)
        raise Exception(f"Badly specified relationship name on {capsuleType.__name__}:\n" + \
                        f"Name on capsule: {relationshipNameOnMainCapsule}\n" + \
                        f"Name on relationship dict: {nameOnRelationshipDict}\n" + \
                        f"json spec: \n" + \
                          jsonSpec)
        # FIXME: spec test for this error
    else:
      # set the id parameter of the relationship (if none) as provided in the dict
      #     equal to the one identified on DB
      relationshipDict['id'] = relationshipIdOnMainCapsule
    result = getattr(relationshipCapsuleClass, constants.NAME_OF_DICT_FNC)(
                    session = session, 
                    capsuleDict = relationshipDict)
    # no adding to session as this is done in fromDict
  else:
    # print('     relationship dict: ', relationshipDict)
    result = getattr(relationshipCapsuleClass, constants.NAME_OF_DICT_FNC)(
                    session = session,
                    capsuleDict = relationshipDict)
    # no adding to session as this is done in fromDict
  return result  
