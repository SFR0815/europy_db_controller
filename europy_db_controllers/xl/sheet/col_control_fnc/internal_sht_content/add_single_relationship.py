import typing

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base 

from europy_db_controllers.xl.sheet.col_control_fnc.debug_cntr import debug_control

def internalAddSingleRelationshipFnc(self,
                            columnOrAlikeInfo: typing.Tuple[str, bool, bool]) -> None:
  column_name = columnOrAlikeInfo[0]

  debug_control.debugPrint(msg = f"column_name: {column_name}",
                           fncName = "__addSingleRelationship")
  
  is_hybrid_property = columnOrAlikeInfo[1]
  def getRelationshipCapsuleTypeOfName(relationshipCapsuleTypeName: str):
    for capsuleType in self._capsuleList:
      if capsuleType.__name__ == relationshipCapsuleTypeName:
        return capsuleType
  relationshipName, is_list, relationship_sqlalchemy_type_name = \
                  _capsule_utils.getRelationshipCapsuleBasicSpecOfIdColumnName(
                                            idColumnName = column_name,
                                            isHybridProperty = is_hybrid_property,
                                            capsuleType = self._capsuleType)
  relationship_capsule_type_name = _capsule_utils.getSqlaToCapsuleName(
                                                      sqlaTableName = relationship_sqlalchemy_type_name)
  relationship_capsule_type = getRelationshipCapsuleTypeOfName(
                                                      relationshipCapsuleTypeName= relationship_capsule_type_name)
  is_excluded_from_json, is_list, relationship_table_has_name = \
              self.__getRelationshipFeatures(
                relationship_name = relationshipName,
                relationship_capsule_type = relationship_capsule_type)[1:]
  # If the relationship is excluded from json but has a name, then the 
  #   relationship's 'name' field of the capsule is added as a Column 
  #   to the ColControl 
  #   The 'isPartOfListOf' avoids the inclusion of such 'name'-column in case of
  #   the current capsule is part of a list of the parent ColControl (is redundant
  #   information in such case).
  if is_excluded_from_json:
    if relationship_table_has_name and not is_list:
      relationshipNameAttrName = _capsule_utils.getColumnRelationshipNameField(columnName = column_name)
      validation = self.validations.getValidationOfCapsuleKey(capsuleKey = relationship_capsule_type._key())
  
      debug_control.debugPrint(msg = f"    adding column with label (excluded from json): {relationshipNameAttrName}",
                              fncName = "__addSingleRelationship")
  
      self._addColumn(label = relationshipNameAttrName,
                      validation = validation,
                      unique=False,
                      sqlalchemyDataType = "") # omit type validation as validated by source
  else:
  
    debug_control.debugPrint(msg = f"    adding sub control (included in json): {relationshipName} of type: {relationship_capsule_type.__name__}",
                            fncName = "__addSingleRelationship")
  
    self._addSubColControl(capsuleType = relationship_capsule_type,
                            isList = False,
                            relationshipKey = relationshipName)