import typing, datetime, sys, uuid, enum, sqlalchemy, json

from sqlalchemy.ext import declarative as sqlalchemy_decl


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils, _capsule_shared

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)  

DEBUG_CAPSULE_TYPE = "MarketTransactionCapsule" 
DEBUG_RELATIONSHIP_NAME = "asset"

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Internal utils:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# a. Function ensuring the name of the relationship's entity being consistent between
#       - the relationship entity's name stored as attribute on the capsule 
#         (<capsule>.<relationshipNameCapsuleInternalAttr>) AND
#       - the relationship entity's name available on the sqlalchemyTable of 
#         the relationship's entity 
#         (<capsule>.sqlalchemyTable.<relationshipName>.name)
def __ensureConsistentRelationshipName(capsule: T,
                                       dictAttributeNamingConventions: dict[str, str]):
  relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Do nothing if the capsule has no attribute for the relationships name
  #    (no <capsule>.<relationshipNameCapsuleInternalAttr>), 
  #    i.e. the sqlalchemyTable of the relationship does not have a 'name'
  #    attribute.
  if hasattr(capsule, relationshipNameCapsuleInternalAttr):

    if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
      print(f"__ensureConsistentRelationshipName on capsule {capsule.__class__.__name__} - relationshipName: {relationshipName}")
      print(f"    capsule.relationshipNameCapsuleInternalAttr: {relationshipNameCapsuleInternalAttr}")

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Identify the sqlalchemyTable of the relationship's entity
    with capsule.session.no_autoflush:
      relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Exit if the sqlalchemyTable of the relationship's entity is not defined yet
    if relationshipSqlaTable is None: 
      return 
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Identify 
    #    - the relationship entity's name as defined on the capsule 
    #      (as per <capsule>.<relationshipNameCapsuleInternalAttr>) and 
    #    - the relationship entity's name as defined on the relationship's sqlalchemyTable
    #      (as per <capsule>.sqlalchemyTable.<relationshipName>.name)
    relationshipNameOnCapsule = getattr(capsule, relationshipNameCapsuleInternalAttr)
    relationshipNameStoredSqla = getattr(relationshipSqlaTable, 'name')
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's name on the capsule 
    #    (<capsule>.<relationshipNameCapsuleInternalAttr>)
    #    does not yet have a value assigned, set it equal to the  
    #    relationship entity's name defined on the relationship's sqlalchemyTable
    #    (<capsule>.sqlalchemyTable.<relationshipName>.name)
    # Comment: relationship entity's name defined on the relationship's sqlalchemyTable
    #          might be 'None' in such case. 
    if relationshipNameOnCapsule is None:
      setattr(capsule, relationshipNameCapsuleInternalAttr, relationshipNameStoredSqla)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Raise Exception the relationship entity's name on the capsule is not the same
    #    as the relationship entity's name defined on the relationship's sqlalchemyTable
    else:
      errMsg = f"Inconsistent definition of '{relationshipNameCapsuleInternalAttr}' and " + \
                f"'name' of '{relationshipName}' on object of type '{type(capsule)}'.\n" + \
                f"Value of '{relationshipNameCapsuleInternalAttr}': {relationshipNameOnCapsule}\n" + \
                f"Value of 'name' of {relationshipName}: {relationshipNameStoredSqla}\n"
      if relationshipNameStoredSqla is None:
        capsule._raiseException(errMsg)
      if relationshipNameOnCapsule != relationshipNameStoredSqla:
        capsule._raiseException(errMsg)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# b. Function ensuring the id of the relationship's entity being consistent between
#       - the relationship entity's id available as and attribute on the capsule's 
#         sqlalchemyTable 
#         (<capsule>.sqlalchemyTable.<relationshipIdAttr>) AND
#       - the relationship entity's id available on the sqlalchemyTable of 
#         the relationship's entity 
#         (<capsule>.sqlalchemyTable.<relationshipName>.id)
def __ensureConsistentRelationshipId(capsule: T,
                                     dictAttributeNamingConventions: dict[str, str]):
  relationshipIdAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Raise Exception if the sqlalchemyTable of the capsule does not have
  #    an attribute providing the relationship's id (see above #b)
  _capsule_shared._raiseExceptionIfNoRelationshipIdOnCapsuleSqlaTable(capsule = capsule,
                                                       dictAttributeNamingConventions = dictAttributeNamingConventions) 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify the sqlalchemyTable of the relationship's entity
  with capsule.session.no_autoflush:
    relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Exit if the sqlalchemyTable of the relationship's entity is not defined yet
  if relationshipSqlaTable is None: 
    # do nothing if no property sqlalchemyTable
    return 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify 
  #    - the relationship entity's id as defined on the capsule's sqlalchemyTable 
  #      (as per <capsule>.sqlalchemyTable.<relationshipIdAttr>) and 
  #    - the relationship entity's id as defined on the relationship's sqlalchemy 
  #      table
  #      (as per <capsule>.sqlalchemyTable.<relationshipName>.id)
  with capsule.session.no_autoflush:
    relationshipIdOnCapsule = getattr(capsule.sqlalchemyTable, relationshipIdAttr)
    relationshipIdStoredSqla = getattr(relationshipSqlaTable, 'id')
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # If the relationship entity's id on the capsule's sqlalchemyTable 
  #    (<capsule>.sqlalchemyTable.<relationshipIdAttr>)
  #    does not yet have a value assigned, set it equal to the  
  #    relationship entity's id defined on the relationship's sqlalchemyTable
  #    (<capsule>.sqlalchemyTable.<relationshipName>.id)
  # Comment: relationship entity's id defined on the relationship's sqlalchemyTable
  #          might be 'None' in such case. 
  
  # if type(capsule).__name__ == "AssetClassCapsule":
  #   print(f"__ensureConsistentRelationshipId on capsule {type(capsule)}")
  #   print(f"    capsule.relationshipName: {relationshipName}                ")
  #   print(f"    capsule.relationshipIdOnCapsule: {relationshipIdOnCapsule}                ")
  #   print(f"    capsule.relationshipSqlaTable: \n{relationshipSqlaTable}                ")
  
  if relationshipIdOnCapsule is None:
    # If no relationshipIdAttr defined yet -> assign value
    capsule._setAttributeOnSqlalchemyTable(attributeName = relationshipIdAttr,
                                             value = relationshipIdStoredSqla)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Raise Exception the relationship entity's id on the capsule's sqlalchemyTable
  #    is not the same
  #    as the relationship entity's id defined on the relationship's sqlalchemy 
  #    table
  else:
      # if defied already and different -> raise error
    errMsg = f"Inconsistent definition of '{relationshipIdAttr}' and " + \
              f"'id' of '{relationshipName}' on object of type '{type(capsule)}'.\n" + \
              f"Value of '{relationshipIdAttr}': {relationshipIdOnCapsule}\n" + \
              f"Value of 'id' of {relationshipName}: {relationshipIdStoredSqla}\n"
    if relationshipIdStoredSqla is None:
      capsule._raiseException(errMsg)
    if relationshipIdOnCapsule != relationshipIdStoredSqla:
      # print("\n\ntype(relationshipIdOnCapsule): ", type(relationshipIdOnCapsule), " -  type(relationshipIdStoredSqla): ", type(relationshipIdStoredSqla))
      capsule._raiseException(errMsg)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# c. Function ensuring both 'name' and 'id' consistency:
def __ensureConsistentRelationship(capsule: T,
                                   dictAttributeNamingConventions: dict[str, str]):
  relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  __ensureConsistentRelationshipId(capsule = capsule,
                                    dictAttributeNamingConventions = dictAttributeNamingConventions)
  if hasattr(capsule, relationshipNameCapsuleInternalAttr):
    __ensureConsistentRelationshipName(capsule = capsule,
                                        dictAttributeNamingConventions = dictAttributeNamingConventions)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# d. Function sourcing the relationship entity's sqlalchemyTable from db based
#       upon the relationship entity's id
def __sourceRelationshipSqlalchemyTableBasedOnId(capsule: T,
                                                 dictAttributeNamingConventions: dict[str, str],
                                                 relationshipType: type[U]):
  relationshipIdAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  # NOT REQUIRED, SEE BELOW:
  # relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Raise Exception if the sqlalchemyTable of the capsule does not have
  #    an attribute providing the relationship's id (see above #b)
  _capsule_shared._raiseExceptionIfNoRelationshipIdOnCapsuleSqlaTable(capsule = capsule,
                                                       dictAttributeNamingConventions = dictAttributeNamingConventions)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify the relationship entity's id as defined on the capsule's sqlalchemyTable 
  #      (as per <capsule>.sqlalchemyTable.<relationshipIdAttr>)
  relationshipIdOnCapsule = getattr(capsule.sqlalchemyTable, relationshipIdAttr)
  if relationshipIdOnCapsule is not None:      
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's id as defined on the capsule's sqlalchemyTable 
    #      (as per <capsule>.sqlalchemyTable.<relationshipIdAttr>) has some
    #      value assigned, source it from the db
    # Comment: If no such relationship entity is found on the db, this raises an 
    #          Exception (see module _capsule_base)
    sqlalchemyTable = relationshipType._queryTableById(
                                session = capsule.session, 
                                id = relationshipIdOnCapsule)
    # Set the attribute <relationshipName> of the capsule's sqlalchemyTable
    #    equal to the relationship entity's sqlalchemyTable sourced
    capsule._setAttributeOnSqlalchemyTable(attributeName = relationshipName,
                                             value = sqlalchemyTable)
              # CAN'T BE THE CASE. WHEN UPDATING AND ID OR SQLALCHEMY TABLE
              # ON THE CAPSULE, ALL NON UPDATED ATTRIBUTES OF THE RELATIONSHIP
              # ARE SET TO 'NONE' AND ARE SUBSEQUENTLY UPDATED VIA THE CONSISTENCY
              # METHODS ABOVE. 
              # # Update the relationship entity's name on the capsule
              # # Comment: this effectively overwrites the capsule's internal relationship
              # #          name with the source relationship entity's name.
              # #          This ensures consistency in case of 'replacing' the relationship
              # #          entity by some other. 
              # if hasattr(sqlalchemyTable, 'name'): 
              #   relationshipNameStoredSqla = getattr(sqlalchemyTable, 'name')
    # Ensure name consistency between the 
    #       - the relationship entity's name stored as attribute on the capsule 
    #         (<capsule>.<relationshipNameCapsuleInternalAttr>) AND
    #       - the relationship entity's name available on the sqlalchemyTable of 
    #         the relationship's entity 
    #         (<capsule>.sqlalchemyTable.<relationshipName>.name)
    # see above #c
    __ensureConsistentRelationshipName(capsule = capsule,
                                       dictAttributeNamingConventions = dictAttributeNamingConventions)    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# e. Function sourcing the relationship entity's sqlalchemyTable from db based
#       upon the relationship entity's name
def __sourceRelationshipSqlalchemyTableBasedOnName(capsule: T,
                                                   dictAttributeNamingConventions: dict[str, str],
                                                   relationshipType: type[U]):
  relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Do nothing if the capsule has no attribute for the relationships name
  #    (no <capsule>.<relationshipNameCapsuleInternalAttr>), 
  #    i.e. the sqlalchemyTable of the relationship does not have a 'name'
  #    attribute.
  if hasattr(capsule, relationshipNameCapsuleInternalAttr):
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Identify the relationship entity's name as defined on the capsule 
    #      (as per <capsule>.<relationshipNameCapsuleInternalAttr>)
    relationshipNameOnCapsule = getattr(capsule, relationshipNameCapsuleInternalAttr)
    if relationshipNameOnCapsule is not None:      
      # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      # If the relationship entity's id as defined on the capsule 
      #      (as per <capsule>.<relationshipNameCapsuleInternalAttr>) has some
      #      value assigned, source it from the db
      # Comment: If no such relationship entity is found on the db, no Exception is  
      #          raised (see module _capsule_base)
      sqlalchemyTables = relationshipType._queryTableByName(
                                  session = capsule.session,
                                  name =  relationshipNameOnCapsule)
      # Raise an Exception if no such relationship entity has been identified or 
      #    the name of the relationship's entity provided is not unique
      if len(sqlalchemyTables) == 0:
        capsule._raiseException(f"No entities of '{relationshipName}' on object " + \
                        f"of type '{type(capsule)}' with name: {relationshipNameOnCapsule}")
      elif len(sqlalchemyTables) > 1:
        capsule._raiseException(f"Multiple entities of '{relationshipName}' on object " + \
                        f"of type '{type(capsule)}' with identical name: {relationshipNameOnCapsule}")
      # Set the attribute <relationshipName> of the capsule's sqlalchemyTable
      #    equal to the relationship entity's sqlalchemyTable sourced
      sqlalchemyTable = sqlalchemyTables[0]
      capsule._setAttributeOnSqlalchemyTable(attributeName = relationshipName,
                                             value = sqlalchemyTable)
      # Ensure name consistency between the 
      #       - the relationship entity's id available on the capsule's sqlalchemyTable 
      #         (<capsule>.sqlalchemyTable.<relationshipIdAttr>) AND
      #       - the relationship entity's id available on the sqlalchemyTable of 
      #         the relationship's entity 
      #         (<capsule>.sqlalchemyTable.<relationshipName>.id)
      # see above #d
      __ensureConsistentRelationshipId(capsule = capsule,
                                       dictAttributeNamingConventions = dictAttributeNamingConventions)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# f. Function ensuring the relationship entity's sqlalchemy is consistent with
#       the capsule's relationship entity's id and name definitions AND
#       present if such relationship entity is defined on db
def __ensureConsistentRelationshipSqlalchemyTable(capsule: T,
                                                dictAttributeNamingConventions: dict[str, str],
                                                relationshipType: type[U]):
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify the sqlalchemyTable of the relationship's entity

  if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
    print(f"\n[_capsule_consistency.__ensureConsistentRelationshipSqlalchemyTable] on capsule {capsule.__class__.__name__} - relationshipName: {relationshipName}")
      
  relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName)
  if relationshipSqlaTable is None:

    if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
      print(f"[_capsule_consistency.__ensureConsistentRelationshipSqlalchemyTable]    relationshipSqlaTable is None - trying to source it based on id")
      
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's sqlalchemyTable is not defined yet, 
    #    try to identify via the capsule's relationship entity id (see above #e)
    # Comment: This does nothing if the capsule sqlalchemyTable's relationship id  
    #          (<capsule>.sqlalchemyTable.<relationshipIdAttr>) is 'None'
    __sourceRelationshipSqlalchemyTableBasedOnId(capsule = capsule,
                                                 dictAttributeNamingConventions = dictAttributeNamingConventions,
                                                 relationshipType = relationshipType)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's sqlalchemyTable has not been identified
    #    based on id (see above), 
    #    try to identify via the capsule's relationship entity name (see above #f)
    # Comment: This does nothing if the capsule's relationship name  
    #          (<capsule>.<relationshipNameCapsuleInternalAttr>) is 'None'
    if getattr(capsule.sqlalchemyTable, relationshipName) is None:

      if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
        print(f"[_capsule_consistency.__ensureConsistentRelationshipSqlalchemyTable]    relationshipSqlaTable is None - trying to source it based on name")
      
      __sourceRelationshipSqlalchemyTableBasedOnName(capsule = capsule,
                                                     dictAttributeNamingConventions = dictAttributeNamingConventions,
                                                     relationshipType = relationshipType)

    if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
      print(f"[_capsule_consistency.__ensureConsistentRelationshipSqlalchemyTable]    relationshipSqlaTable is None? {getattr(capsule.sqlalchemyTable, relationshipName) is None} - after sourcing based on name")
      
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Ensure name and id consistency between definitions on the capsule and the 
    #    relationship entity's sqlalchemyTable (if the latter is defined),
    #    see above #c & #b
    # Comment: This does nothing if the sqlalchemyTable of the relationship's entity
    #          (<capsule>.sqlalchemyTable.<relationshipName>) is 'None'
    nameOfConsistencyFnc = _capsule_utils.getConsistencyCheckFncName(
                                    relationshipName = relationshipName)
    getattr(capsule, nameOfConsistencyFnc)()
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Definition of class attributes:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Adding the consistency test of the relationship
def __addConsistencyCheck(capsuleType: type[T],
                        dictAttributeNamingConventions: dict[str, str]):
  def fncConsistencyCheck(self: T):
    
    # if type(self).__name__ == "AssetClassCapsule":
    #   print(f"__addConsistencyCheck on capsule {type(self)}")
    #   print(f"    capsule.sqlalchemyTable: \n{self.sqlalchemyTable}                ")
    
    __ensureConsistentRelationship(capsule = self,
                                   dictAttributeNamingConventions = dictAttributeNamingConventions)
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]  
  nameOfFnc = _capsule_utils.getConsistencyCheckFncName(
                                   relationshipName = relationshipName)
  fncConsistencyCheckDecorated = _capsule_base.cleanAndCloseSession(
                                    func = fncConsistencyCheck)
  setattr(capsuleType, nameOfFnc, fncConsistencyCheckDecorated)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Adding the conditional sourcing with consistency test of the relationship
def __addConditionalSourcingWithConsistency(capsuleType: type[T],
                                          dictAttributeNamingConventions: dict[str, str],
                                          relationshipType: type[U]):
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]  
  nameOfFnc = _capsule_utils.getSourceAndConsistencyCheckFncName(relationshipName = relationshipName)
  def fncSourceAndTestForConsistency(self: T): 
    if type(self).__name__ == DEBUG_CAPSULE_TYPE:
      print(f"[_capsule_consistency.__addConditionalSourcingWithConsistency] ({self.__class__.__name__}) method {nameOfFnc} executed")
    __ensureConsistentRelationshipSqlalchemyTable(capsule = self,
                                                  dictAttributeNamingConventions = dictAttributeNamingConventions,
                                                  relationshipType = relationshipType)
  fncSourceAndTestForConsistencyDecorated = _capsule_base.cleanAndCloseSession(
                                    func = fncSourceAndTestForConsistency)
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
    if type(self).__name__ == DEBUG_CAPSULE_TYPE:
      print(f"[_capsule_consistency.__addOverAllConditionalSourcingWithConsistency] ({self.__class__.__name__}) method {nameOfFnc} executed")
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
    for attributeName, attributeInfo in sqlalchemyColumnsAndColumnLikeProperties.items():
      if _capsule_utils.isBaseColumnName(capsuleType = capsuleType,
                                         columnName = attributeName): continue
      if not _capsule_utils.isRelationshipIdColumnName(columnName = attributeName): continue
      relationshipName, relationshipType, isList = _capsule_utils.getRelationshipCapsuleTypeSpecOfIdColumnName(
                                                    idColumnName = attributeName,
                                                    isHybridProperty = attributeInfo[1],
                                                    capsuleType = capsuleType,
                                                    callingGlobals = callingGlobals)
      relationshipNames.append(relationshipName)
      dictAttributeNamingConventions = _capsule_utils.getDictOfAttributeNamingConventionsFromRelationshipName(
                      relationshipName = relationshipName)
      __addConsistencyCheck(capsuleType = capsuleType,
                          dictAttributeNamingConventions = dictAttributeNamingConventions) 
      __addConditionalSourcingWithConsistency(capsuleType = capsuleType,
                                            dictAttributeNamingConventions = dictAttributeNamingConventions,
                                            relationshipType = relationshipType)
    __addOverAllConsistencyCheck(capsuleType = capsuleType,
                               relationshipNames = relationshipNames)      
    __addOverAllConditionalSourcingWithConsistency(capsuleType = capsuleType,
                                                 relationshipNames = relationshipNames)      
