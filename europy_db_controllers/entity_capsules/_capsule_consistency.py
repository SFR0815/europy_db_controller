import typing

from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils

from europy_db_controllers.entity_capsules.capsule_consistency_fnc import a_ensure_consistent_relationship_name \
             as consistency_rel_name 
from europy_db_controllers.entity_capsules.capsule_consistency_fnc import c_ensure_consistent_relationship \
            as consistent_rel
from europy_db_controllers.entity_capsules.capsule_consistency_fnc import e_ensure_consistent_relationship_sqla_table \
             as consistency_rel_table

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)  

DEBUG_CAPSULE_TYPE = "CoreAccountCapsule" 
DEBUG_RELATIONSHIP_NAME = "asset"

def __defaultConsistencyCheck(self: T):
  pass
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Definition of class attributes:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Adding the consistency test of the relationship
def __addConsistencyCheck(capsuleType: type[T],
                        dictAttributeNamingConventions: dict[str, str],
                        isColumnName: bool):
  def fncConsistencyCheck(self: T):
    consistent_rel.ensureConsistentRelationship(capsule = self,
                                   dictAttributeNamingConventions = dictAttributeNamingConventions)
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]  
  nameOfFnc = _capsule_utils.getConsistencyCheckFncName(
                                   relationshipName = relationshipName)
  func = fncConsistencyCheck if isColumnName else __defaultConsistencyCheck
  fncConsistencyCheckDecorated = _capsule_base.cleanAndCloseSession(
                                    func = func)
  setattr(capsuleType, nameOfFnc, fncConsistencyCheckDecorated)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Adding the conditional sourcing with consistency test of the relationship
def __addConditionalSourcingWithConsistency(capsuleType: type[T],
                                          dictAttributeNamingConventions: dict[str, str],
                                          relationshipType: type[U],
                                          isColumnName: bool):
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]  
  nameOfFnc = _capsule_utils.getSourceAndConsistencyCheckFncName(relationshipName = relationshipName)
  def fncSourceAndTestForConsistency(self: T): 
    # if type(self).__name__ == DEBUG_CAPSULE_TYPE:
    #   print(f"[_capsule_consistency.__addConditionalSourcingWithConsistency] ({self.__class__.__name__}) method {nameOfFnc} executed")
    consistency_rel_table.ensureConsistentRelationshipSqlalchemyTable(capsule = self,
                                                  dictAttributeNamingConventions = dictAttributeNamingConventions,
                                                  relationshipType = relationshipType)
  func = fncSourceAndTestForConsistency if isColumnName else __defaultConsistencyCheck
  fncSourceAndTestForConsistencyDecorated = _capsule_base.cleanAndCloseSession(
                                    func = func)
  setattr(capsuleType, nameOfFnc, fncSourceAndTestForConsistencyDecorated)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Adding the consistency test over all relationships
def __addOverAllConsistencyCheck(capsuleType: type[T],
                               relationshipNames: typing.List[str]):
  def fncConsistencyCheck(self: T):
    nameOfFnc = _capsule_utils.getConsistencyCheckOverAllFncName()
    for relationshipName in relationshipNames:
      nameOfFnc = _capsule_utils.getConsistencyCheckFncName(
                                   relationshipName = relationshipName)
      getattr(self, nameOfFnc)()
  nameOfFnc = _capsule_utils.getConsistencyCheckOverAllFncName()
  fncConsistencyCheckDecorated = _capsule_base.cleanAndCloseSession(
                                    func = fncConsistencyCheck)
  setattr(capsuleType, nameOfFnc, fncConsistencyCheckDecorated)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 4. Adding the conditional sourcing with consistency test over all relationships
def __addOverAllConditionalSourcingWithConsistency(capsuleType: type[T],
                                                   relationshipNames: typing.List[str]):
  def fncSourceAndTestForConsistency(self: T):
    nameOfFnc = _capsule_utils.getSourceAndConsistencyCheckOverAllFncName()
    # if type(self).__name__ == DEBUG_CAPSULE_TYPE:
    #   print(f"[_capsule_consistency.__addOverAllConditionalSourcingWithConsistency] ({self.__class__.__name__}) method {nameOfFnc} executed")
    for relationshipName in relationshipNames:
      nameOfFnc = _capsule_utils.getSourceAndConsistencyCheckFncName(
                                   relationshipName = relationshipName)
      getattr(self, nameOfFnc)()
  nameOfFnc = _capsule_utils.getSourceAndConsistencyCheckOverAllFncName()
  fncSourceAndTestForConsistencyDecorated = _capsule_base.cleanAndCloseSession(
                                    func = fncSourceAndTestForConsistency)
  setattr(capsuleType, nameOfFnc, fncSourceAndTestForConsistencyDecorated)



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def addRelationshipConsistencyChecks(capsuleList: typing.List[T],
                                     callingGlobals):
  for capsuleType in capsuleList:
    sqlalchemyColumnsAndColumnLikeProperties = _capsule_utils.getSqlalchemyColumnsAndColumnLikeProperties(
                                              capsuleType = capsuleType)  
    sqlalchemyTableType = capsuleType.sqlalchemyTableType
    relationshipNames: typing.List[str] = []
    sqlalchemyTableColumnNames = [column.name for column in sqlalchemyTableType.__table__.columns]
    for attributeName, attributeInfo in sqlalchemyColumnsAndColumnLikeProperties.items():
      isColumnName = attributeName in sqlalchemyTableColumnNames
      if _capsule_utils.isBaseColumnName(capsuleType = capsuleType,
                                         columnName = attributeName): continue
      if not _capsule_utils.isRelationshipIdColumnName(columnName = attributeName): continue
      relationshipName, relationshipType, isList = _capsule_utils.getRelationshipCapsuleTypeSpecOfIdColumnName(
                                                    idColumnName = attributeName,
                                                    isHybridProperty = attributeInfo[1],
                                                    capsuleType = capsuleType,
                                                    callingGlobals = callingGlobals)
      if isColumnName:
        relationshipNames.append(relationshipName)
      dictAttributeNamingConventions = _capsule_utils.getDictOfAttributeNamingConventionsFromRelationshipName(
                      relationshipName = relationshipName)
      __addConsistencyCheck(capsuleType = capsuleType,
                          dictAttributeNamingConventions = dictAttributeNamingConventions,
                          isColumnName = isColumnName) 
      __addConditionalSourcingWithConsistency(capsuleType = capsuleType,
                                            dictAttributeNamingConventions = dictAttributeNamingConventions,
                                            relationshipType = relationshipType,
                                            isColumnName = isColumnName)
    __addOverAllConsistencyCheck(capsuleType = capsuleType,
                               relationshipNames = relationshipNames)      
    __addOverAllConditionalSourcingWithConsistency(capsuleType = capsuleType,
                                                 relationshipNames = relationshipNames)      
