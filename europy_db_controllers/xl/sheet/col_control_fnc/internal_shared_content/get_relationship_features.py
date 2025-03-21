import typing

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base

CT = typing.TypeVar('CT', bound=_capsule_base.CapsuleBase)

def internalGetRelationshipFeaturesFnc(self,
                              relationship_name: str,
                              relationship_capsule_type: typing.Type[CT],
                              ) -> typing.Tuple[bool, 
                                                bool,
                                                bool, 
                                                bool]:
  def relationshipIsPartOfListOf() -> bool:
    if hasattr(self._sqlalchemyType, '_is_part_of_list_of'):
      if relationship_name in self._sqlalchemyType._is_part_of_list_of:
        return True
    return False
  is_display_list = _capsule_utils.isDisplayList(
              sqlalchemyTableType = self._sqlalchemyType,
              relationshipName = relationship_name)
  is_excluded_from_json = relationship_name in self._sqlalchemyType._exclude_from_json
  sqlalchemy_table_type = relationship_capsule_type.sqlalchemyTableType
  relationship_table_has_name= hasattr(sqlalchemy_table_type, 'name')
  is_list = relationshipIsPartOfListOf()
  return(is_display_list, is_excluded_from_json, is_list, relationship_table_has_name)