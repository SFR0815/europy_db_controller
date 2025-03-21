import typing, json

from europy_db_controllers.entity_capsules import _capsule_utils
from europy_db_controllers.xl.sheet import data_block

def toDictFnc(self,
              dataEntry: data_block.DataBlock
              ) -> typing.Dict[str, any]:
  def getRelationshipCapsuleTypeOfName(relationship_capsule_type_name: str):
          for capsuleType in self._capsuleList:
            if capsuleType.__name__ == relationship_capsule_type_name:
              return capsuleType
  result: typing.Dict[str, any] = dict[str, any]()
  if not dataEntry.colControlData.hasDeleteMarker:
    columnsAndAlikeInfo = _capsule_utils.getCapsuleInitColumnsAndColumnLikeProperties(
                      capsuleType = self._capsuleType)
    for column_or_alike_name, column_info in columnsAndAlikeInfo.items():
      isHybridProperty = column_info[1]
      if column_or_alike_name in self._sqlalchemyType._changeTrackFields:
        pass # internal change control only
      elif _capsule_utils.isRelationshipIdColumnName(columnName = column_or_alike_name):
        relationship_name, _, relationship_sqlalchemy_type_name = \
                        _capsule_utils.getRelationshipCapsuleBasicSpecOfIdColumnName(idColumnName = column_or_alike_name,
                                                    isHybridProperty = isHybridProperty,
                                                    capsuleType = self._capsuleType)
        relationship_capsule_type_name = _capsule_utils.getSqlaToCapsuleName(
                                                            sqlaTableName = relationship_sqlalchemy_type_name)
        relationship_capsule_type = getRelationshipCapsuleTypeOfName(
                                                            relationship_capsule_type_name = relationship_capsule_type_name)
        is_excluded_from_json, relationship_decl_has_name = self.__getRelationshipFeatures(
                          relationship_name = relationship_name,
                          relationship_capsule_type = relationship_capsule_type)[1:4:2]
      
        if is_excluded_from_json:
          if relationship_decl_has_name:
            # no control for 'isPartOfListOf' here. If 'isPartOfListOf', the value must be added
            #     to the dataEntry.colControlData while processing the parent, see below [#add name of parent].
            relationshipNameAttrName = _capsule_utils.getColumnRelationshipNameField(columnName = column_or_alike_name)
            value = dataEntry.colControlData.getValue(relationshipNameAttrName)
            result[relationshipNameAttrName] = value   
            # print(f"[col_control.toDict] adding item to dict - column: {column_or_alike_name} is relationship column & excluded from json.\n" + \
            #       f"                      result[{relationshipNameAttrName}] = {value}")           
        else:
          subControlName = self.subControlNameOfRelationshipKey(relationshipKey = relationship_name)
          subColControl = self.subColControls[subControlName]
          relationshipDataEntry = dataEntry.getSubBlockOfName(colBlockName = subControlName)
          entryDict = subColControl.toDict(dataEntry = relationshipDataEntry)
          result[relationship_name] = entryDict
          # print(f"[col_control.toDict] adding item to dict - column: {column_or_alike_name} is relationship column & included from json.\n" + \
          #       f"                      result[{relationship_name}] = {entryDict}")           
      else:
        value = dataEntry.colControlData.getValue(column_or_alike_name)
        result[column_or_alike_name] = value
        # print(f"[col_control.toDict] adding item to dict - column: {column_or_alike_name} data column.\n" + \
        #       f"                      result[{column_or_alike_name}] = {value}")           
    for relationship in self._relationships:
      if relationship.uselist:
        
        relationship_sqlalchemy_type_name = relationship.mapper.class_.__name__
        relationship_capsule_type_name = _capsule_utils.getSqlaToCapsuleName(
                                                            sqlaTableName = relationship_sqlalchemy_type_name)
        relationship_capsule_type = getRelationshipCapsuleTypeOfName(
                                                            relationship_capsule_type_name = relationship_capsule_type_name)
        is_display_list, is_excluded_from_json = self.__getRelationshipFeatures(
                          relationship_name = relationship.key,
                          relationship_capsule_type = relationship_capsule_type)[:2]
        if not (is_display_list or is_excluded_from_json):

          # relationship_name, relationshipTable = \
          #               self.__getRelationshipDefinitions(relationship = relationship)[0:3:2]  
          relationship_name, relationshipSqlaTable, relationshipTable = \
                        self.__getRelationshipDefinitions(relationship = relationship)[0:3:1]  
          subControlName = self.subControlNameOfRelationshipKey(relationshipKey = relationship_name)
          subColControl = self.subColControls[subControlName]
          relationshipDataList = dataEntry.getSubBlockOfName(colBlockName = subControlName)
           # [#Evaluate if parent name to add to dict] identify whether the relationship entity has a link to this parent
          parentPartOfListOfChildDefs = self.parentPartOfListOfChildDefs(relationshipSqlaTable = relationshipSqlaTable,
                                                                          relationshipTable = relationshipTable)
          # relationshipDataEntry is a list DataBlock
          entryCount = 0
          relationshipDataDict: typing.Dict[int, any] = dict[int, any]()
          for relationshipDataEntry in relationshipDataList.listElements:
            # [#add name of parent] add the parent name attribute field of the current entity to the dictionary of child values
            #    depending on the evaluation of the relationship, see above [#Evaluate if parent name to add to dict]
            if not relationshipDataEntry.colControlData.isEmpty:
              if parentPartOfListOfChildDefs[0]:
                parentName = dataEntry.colControlData.getValue('name')
                relationshipDataEntry.colControlData.valueDict[parentPartOfListOfChildDefs[1]] = parentName
              relationshipDict = subColControl.toDict(dataEntry = relationshipDataEntry)
              relationshipDataDict[entryCount] = relationshipDict
              entryCount += 1
          result[relationship_name] = relationshipDataDict
          # print(f"[col_control.toDict] adding item to dict - list relationship: {relationship_name}.\n" + \
          #       f"                      result[{relationship_name}] = {relationshipDataDict}")  
  # Check if all values in result dict are None
  if all(value is None for value in result.values()):
    result = None
  # if self.tableName == 'asset_class':
  #   pretty_json = json.dumps(result, indent=4)
  #   print(f"[to_dict.toDict] - result: \n {pretty_json}")
  return result
