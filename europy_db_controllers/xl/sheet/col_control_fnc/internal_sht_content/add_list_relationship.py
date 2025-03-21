from sqlalchemy import orm as sqlalchemy_orm

from europy_db_controllers.entity_capsules import _capsule_utils

def internalAddListRelationshipFnc(self,
                          relationship: sqlalchemy_orm.Relationship) -> None:
  def getRelationshipCapsuleTypeOfName(relationship_capsule_type_name: str):
    for capsuleType in self._capsuleList:
      if capsuleType.__name__ == relationship_capsule_type_name:
        return capsuleType
  relationship_sqlalchemy_type_name = relationship.mapper.class_.__name__
  relationship_capsule_type_name = _capsule_utils.getSqlaToCapsuleName(
                                                      sqlaTableName = relationship_sqlalchemy_type_name)
  relationship_capsule_type = getRelationshipCapsuleTypeOfName(
                                                      relationship_capsule_type_name = relationship_capsule_type_name)
  

  is_display_list, is_excluded_from_json = self.__getRelationshipFeatures(
                                relationship_name = relationship.key,
                                relationship_capsule_type=relationship_capsule_type)[:2]
  if not (is_display_list or is_excluded_from_json):
    relationshipName, _, _, relationshipCapsuleType = \
              self.__getRelationshipDefinitions(relationship = relationship)  
    self._addSubColControl(capsuleType = relationshipCapsuleType,
                            isList = True,
                            relationshipKey = relationshipName)