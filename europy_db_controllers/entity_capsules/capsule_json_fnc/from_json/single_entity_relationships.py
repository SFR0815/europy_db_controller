import typing, json, uuid

import sqlalchemy.orm as sqlalchemy_orm

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base

from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import single_entity_json_excluded, single_entity_json_included

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

DEBUG_CAPSULE_TYPE = "TxTypeToParameterMapCapsule_x"

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Methods covering relationships on the 1-side of (1 to n) relationships
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def addSingleRelatedEntity(
              session: sqlalchemy_orm.Session,
              capsuleType: typing.Type[T],
              capsuleDict: dict[str, any],
              relationshipEntitiesCatalog: typing.Dict[str, dict],
              column_name: str,
              is_hybrid_property: bool,
              resultEntity: T,
              callingGlobals: typing.Dict[str, any]
              ) -> None :
  relationship_type_name, relationship_type, is_list = _capsule_utils.getRelationshipCapsuleTypeSpecOfIdColumnName(
                    idColumnName = column_name,
                    isHybridProperty = is_hybrid_property,
                    capsuleType = capsuleType,
                    callingGlobals = callingGlobals)
  dictAttributeNamingConventions = _capsule_utils.getDictOfAttributeNamingConventionsFromRelationshipName(
                  relationshipName = relationship_type_name)
  ## 
  ## add relationshipName to relationshipEntitiesCatalog:
  if not relationship_type_name in relationshipEntitiesCatalog:
    relationshipEntitiesCatalog[relationship_type_name] = {}
  ##
  
  relationshipNameAttributeName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_NAME]
  if relationship_type_name in capsuleType.sqlalchemyTableType._exclude_from_json:
    relationshipEntity = single_entity_json_excluded.getExcludedFromJsonSingleRelatedEntity(
                          session = session,
                          capsuleType = capsuleType,
                          capsuleDict = capsuleDict,
                          relationshipEntitiesCatalog = relationshipEntitiesCatalog,
                          relationshipName = relationship_type_name,
                          relationshipNameAttributeName = relationshipNameAttributeName,
                          relationshipCapsuleClass = relationship_type)
    if capsuleType.__name__ == DEBUG_CAPSULE_TYPE:
      print(f"\n[addSingleRelatedEntity] - relationshipEntity: {relationshipEntity.name} - id: {relationshipEntity.id}")
  else:
    relationshipEntity = single_entity_json_included.getIncludedInJsonSingleRelatedEntity(
                          session = session,
                          capsuleType = capsuleType,
                          capsuleDict = capsuleDict,
                          relationshipName = relationship_type_name,
                          relationshipNameAttributeName = relationshipNameAttributeName,
                          relationshipCapsuleClass = relationship_type,
                          resultEntity = resultEntity)
  if not relationshipEntity is None:
    setattr(resultEntity, relationship_type_name, relationshipEntity)


def addSingleRelatedEntities(resultEntity: T,
                            session: sqlalchemy_orm.Session,
                            capsuleType: typing.Type[T],
                            capsuleDict: dict[str, any],
                            columnsAndAlikeInfo: typing.Dict[str, typing.Tuple[str, bool, bool]],
                            relationshipEntitiesCatalog: typing.Dict[str, dict],
                            callingGlobals: typing.Dict[str, any]):
  sqlalchemyTableType = resultEntity.sqlalchemyTableType
  noJsonFields = sqlalchemyTableType._exclude_from_json
  for column_name, column_info in columnsAndAlikeInfo.items():
    is_hybrid_property = column_info[1]
    if not column_name in noJsonFields:
      isRelationshipColumn = _capsule_utils.isRelationshipIdColumnName(columnName = column_name)
      if isRelationshipColumn:
        addSingleRelatedEntity(
                      session = session,
                      capsuleType = capsuleType,
                      capsuleDict = capsuleDict,
                      relationshipEntitiesCatalog = relationshipEntitiesCatalog,
                      column_name = column_name,
                      is_hybrid_property = is_hybrid_property,
                      resultEntity = resultEntity,
                      callingGlobals = callingGlobals)
