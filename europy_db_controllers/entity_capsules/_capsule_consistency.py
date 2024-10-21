import typing, datetime, sys, uuid, enum, sqlalchemy, json

from sqlalchemy.ext import declarative as sqlalchemy_decl


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils, _capsule_shared

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Internal utils:
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# a. Function ensuring the name of the relationship's entity being consistent between
#       - the relationship entity's name stored as attribute on the capsule 
#         (<capsule>.<relationshipNameCapsuleInternalAttr>) AND
#       - the relationship entity's name available on the sqlalchemy table of 
#         the relationship's entity 
#         (<capsule>.sqlalchemyTable.<relationshipName>.name)
def __ensureConsistentRelationshipName(capsule: T,
                                       attributeNameDict: dict[str, str]):
  relationshipNameCapsuleInternalAttr = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipName = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Do nothing if the capsule has no attribute for the relationships name
  #    (no <capsule>.<relationshipNameCapsuleInternalAttr>), 
  #    i.e. the sqlalchemy table of the relationship does not have a 'name'
  #    attribute.
  if hasattr(capsule, relationshipNameCapsuleInternalAttr):
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Identify the sqlalchemy table of the relationship's entity
    with capsule.session.no_autoflush:
      relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Exit if the sqlalchemy table of the relationship's entity is not defined yet
    if relationshipSqlaTable is None: 
      return 
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Identify 
    #    - the relationship entity's name as defined on the capsule 
    #      (as per <capsule>.<relationshipNameCapsuleInternalAttr>) and 
    #    - the relationship entity's name as defined on the relationship's sqlalchemy 
    #      table
    #      (as per <capsule>.sqlalchemyTable.<relationshipName>.name)
    relationshipNameOnCapsule = getattr(capsule, relationshipNameCapsuleInternalAttr)
    relationshipNameStoredSqla = getattr(relationshipSqlaTable, 'name')
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's name on the capsule 
    #    (<capsule>.<relationshipNameCapsuleInternalAttr>)
    #    does not yet have a value assigned, set it equal to the  
    #    relationship entity's name defined on the relationship's sqlalchemy table
    #    (<capsule>.sqlalchemyTable.<relationshipName>.name)
    # Comment: relationship entity's name defined on the relationship's sqlalchemy table
    #          might be 'None' in such case. 
    if relationshipNameOnCapsule is None:
      setattr(capsule, relationshipNameCapsuleInternalAttr, relationshipNameStoredSqla)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Raise Exception the relationship entity's name on the capsule is not the same
    #    as the relationship entity's name defined on the relationship's sqlalchemy 
    #    table
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
#         sqlalchemy table 
#         (<capsule>.sqlalchemyTable.<relationshipIdAttr>) AND
#       - the relationship entity's id available on the sqlalchemy table of 
#         the relationship's entity 
#         (<capsule>.sqlalchemyTable.<relationshipName>.id)
def __ensureConsistentRelationshipId(capsule: T,
                                     attributeNameDict: dict[str, str]):
  relationshipIdAttr = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  relationshipName = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Raise Exception if the sqlalchemy table of the capsule does not have
  #    an attribute providing the relationship's id (see above #b)
  _capsule_shared._raiseExceptionIfNoRelationshipIdOnCapsuleSqlaTable(capsule = capsule,
                                                       attributeNameDict = attributeNameDict) 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify the sqlalchemy table of the relationship's entity
  with capsule.session.no_autoflush:
    relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Exit if the sqlalchemy table of the relationship's entity is not defined yet
  if relationshipSqlaTable is None: 
    # do nothing if no property sqlalchemy table
    return 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify 
  #    - the relationship entity's id as defined on the capsule's sqlalchemy table 
  #      (as per <capsule>.sqlalchemyTable.<relationshipIdAttr>) and 
  #    - the relationship entity's id as defined on the relationship's sqlalchemy 
  #      table
  #      (as per <capsule>.sqlalchemyTable.<relationshipName>.id)
  with capsule.session.no_autoflush:
    relationshipIdOnCapsule = getattr(capsule.sqlalchemyTable, relationshipIdAttr)
    relationshipIdStoredSqla = getattr(relationshipSqlaTable, 'id')
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # If the relationship entity's id on the capsule's sqlalchemy table 
  #    (<capsule>.sqlalchemyTable.<relationshipIdAttr>)
  #    does not yet have a value assigned, set it equal to the  
  #    relationship entity's id defined on the relationship's sqlalchemy table
  #    (<capsule>.sqlalchemyTable.<relationshipName>.id)
  # Comment: relationship entity's id defined on the relationship's sqlalchemy table
  #          might be 'None' in such case. 
  
  # if type(capsule).__name__ == "AssetClassCapsule":
  #   print(f"__ensureConsistentRelationshipId on capsule {type(capsule)}")
  #   print(f"    capsule.relationshipName: {relationshipName}                ")
  #   print(f"    capsule.relationshipIdOnCapsule: {relationshipIdOnCapsule}                ")
  #   print(f"    capsule.relationshipSqlaTable: \n{relationshipSqlaTable}                ")
  
  if relationshipIdOnCapsule is None:
    # If no relationshipIdAttr defined yet -> assign value
    setattr(capsule.sqlalchemyTable, relationshipIdAttr, relationshipIdStoredSqla)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Raise Exception the relationship entity's id on the capsule's sqlalchemy table
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
                                   attributeNameDict: dict[str, str]):
  relationshipNameCapsuleInternalAttr = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  __ensureConsistentRelationshipId(capsule = capsule,
                                    attributeNameDict = attributeNameDict)
  if hasattr(capsule, relationshipNameCapsuleInternalAttr):
    __ensureConsistentRelationshipName(capsule = capsule,
                                        attributeNameDict = attributeNameDict)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# d. Function sourcing the relationship entity's sqlalchemy table from db based
#       upon the relationship entity's id
def __sourceRelationshipSqlalchemyTableBasedOnId(capsule: T,
                                                 attributeNameDict: dict[str, str],
                                                 relationshipType: type[U]):
  relationshipIdAttr = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  # NOT REQUIRED, SEE BELOW:
  # relationshipNameCapsuleInternalAttr = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipName = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Raise Exception if the sqlalchemy table of the capsule does not have
  #    an attribute providing the relationship's id (see above #b)
  _capsule_shared._raiseExceptionIfNoRelationshipIdOnCapsuleSqlaTable(capsule = capsule,
                                                       attributeNameDict = attributeNameDict)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify the relationship entity's id as defined on the capsule's sqlalchemy table 
  #      (as per <capsule>.sqlalchemyTable.<relationshipIdAttr>)
  relationshipIdOnCapsule = getattr(capsule.sqlalchemyTable, relationshipIdAttr)
  if relationshipIdOnCapsule is not None:      
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's id as defined on the capsule's sqlalchemy table 
    #      (as per <capsule>.sqlalchemyTable.<relationshipIdAttr>) has some
    #      value assigned, source it from the db
    # Comment: If no such relationship entity is found on the db, this raises an 
    #          Exception (see module _capsule_base)
    sqlalchemyTable = relationshipType._queryTableById(
                                session = capsule.session, 
                                id = relationshipIdOnCapsule)
    # Set the attribute <relationshipName> of the capsule's sqlalchemy table
    #    equal to the relationship entity's sqlalchemy table sourced
    setattr(capsule.sqlalchemyTable, relationshipName, sqlalchemyTable)
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
              #   setattr(capsule, relationshipNameCapsuleInternalAttr, relationshipNameStoredSqla)
    # Ensure name consistency between the 
    #       - the relationship entity's name stored as attribute on the capsule 
    #         (<capsule>.<relationshipNameCapsuleInternalAttr>) AND
    #       - the relationship entity's name available on the sqlalchemy table of 
    #         the relationship's entity 
    #         (<capsule>.sqlalchemyTable.<relationshipName>.name)
    # see above #c
    __ensureConsistentRelationshipName(capsule = capsule,
                                       attributeNameDict = attributeNameDict)    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# e. Function sourcing the relationship entity's sqlalchemy table from db based
#       upon the relationship entity's name
def __sourceRelationshipSqlalchemyTableBasedOnName(capsule: T,
                                                   attributeNameDict: dict[str, str],
                                                   relationshipType: type[U]):
  relationshipNameCapsuleInternalAttr = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipName = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Do nothing if the capsule has no attribute for the relationships name
  #    (no <capsule>.<relationshipNameCapsuleInternalAttr>), 
  #    i.e. the sqlalchemy table of the relationship does not have a 'name'
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
      # Set the attribute <relationshipName> of the capsule's sqlalchemy table
      #    equal to the relationship entity's sqlalchemy table sourced
      sqlalchemyTable = sqlalchemyTables[0]
      setattr(capsule.sqlalchemyTable, relationshipName, sqlalchemyTable)
      # Ensure name consistency between the 
      #       - the relationship entity's id available on the capsule's sqlalchemy table 
      #         (<capsule>.sqlalchemyTable.<relationshipIdAttr>) AND
      #       - the relationship entity's id available on the sqlalchemy table of 
      #         the relationship's entity 
      #         (<capsule>.sqlalchemyTable.<relationshipName>.id)
      # see above #d
      __ensureConsistentRelationshipId(capsule = capsule,
                                       attributeNameDict = attributeNameDict)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# f. Function ensuring the relationship entity's sqlalchemy is consistent with
#       the capsule's relationship entity's id and name definitions AND
#       present if such relationship entity is defined on db
def __ensureConsistentRelationshipSqlalchemyTable(capsule: T,
                                                attributeNameDict: dict[str, str],
                                                relationshipType: type[U]):
  relationshipName = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify the sqlalchemy table of the relationship's entity
  relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName)
  if relationshipSqlaTable is None:
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's sqlalchemy table is not defined yet, 
    #    try to identify via the capsule's relationship entity id (see above #e)
    # Comment: This does nothing if the capsule sqlalchemy table's relationship id  
    #          (<capsule>.sqlalchemyTable.<relationshipIdAttr>) is 'None'
    __sourceRelationshipSqlalchemyTableBasedOnId(capsule = capsule,
                                                 attributeNameDict = attributeNameDict,
                                                 relationshipType = relationshipType)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's sqlalchemy table has not been identified
    #    based on id (see above), 
    #    try to identify via the capsule's relationship entity name (see above #f)
    # Comment: This does nothing if the capsule's relationship name  
    #          (<capsule>.<relationshipNameCapsuleInternalAttr>) is 'None'
    if getattr(capsule.sqlalchemyTable, relationshipName) is None:
      __sourceRelationshipSqlalchemyTableBasedOnName(capsule = capsule,
                                                     attributeNameDict = attributeNameDict,
                                                     relationshipType = relationshipType)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Ensure name and id consistency between definitions on the capsule and the 
    #    relationship entity's sqlalchemy table (if the latter is defined),
    #    see above #c & #b
    # Comment: This does nothing if the sqlalchemy table of the relationship's entity
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
                        attributeNameDict: dict[str, str]):
  def fncConsistencyCheck(self: T):
    
    # if type(self).__name__ == "AssetClassCapsule":
    #   print(f"__addConsistencyCheck on capsule {type(self)}")
    #   print(f"    capsule.sqlalchemyTable: \n{self.sqlalchemyTable}                ")
    
    __ensureConsistentRelationship(capsule = self,
                                   attributeNameDict = attributeNameDict)
  relationshipName = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]  
  nameOfFnc = _capsule_utils.getConsistencyCheckFncName(
                                   relationshipName = relationshipName)
  fncConsistencyCheckDecorated = _capsule_base.cleanAndCloseSession(
                                    func = fncConsistencyCheck)
  setattr(capsuleType, nameOfFnc, fncConsistencyCheckDecorated)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Adding the conditional sourcing with consistency test of the relationship
def __addConditionalSourcingWithConsistency(capsuleType: type[T],
                                          attributeNameDict: dict[str, str],
                                          relationshipType: type[U]):
  def fncSourceAndTestForConsistency(self: T):
    __ensureConsistentRelationshipSqlalchemyTable(capsule = self,
                                                  attributeNameDict = attributeNameDict,
                                                  relationshipType = relationshipType)
  relationshipName = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]  
  nameOfFnc = _capsule_utils.getSourceAndConsistencyCheckFncName(
                                  relationshipName = relationshipName)
  fncSourceAndTestForConsistencyDecorated = _capsule_base.cleanAndCloseSession(
                                    func = fncSourceAndTestForConsistency)
  setattr(capsuleType, nameOfFnc, fncSourceAndTestForConsistencyDecorated)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 3. Adding the consistency test over all relationships
def __addOverAllConsistencyCheck(capsuleType: type[T],
                               relationshipNames: typing.List[str]):
  def fncConsistencyCheck(self: T):
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
    sqlalchemyTableType = capsuleType.sqlalchemyTableType
    table = sqlalchemyTableType.__table__
    relationshipNames: typing.List[str] = []
    for column in table.columns:
      if _capsule_utils.isRelationshipIdColumn(column=column):
        attributeNameDict = _capsule_utils.getDictOfRelationshipAttrName(
                        column = column)
        relationshipName = attributeNameDict[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
        relationshipType = _capsule_utils.getRelationshipCapsuleTypeOfName(
                        relationshipName = relationshipName,
                        table = table,
                        callingGlobals = callingGlobals)
        relationshipNames.append(relationshipName)
        __addConsistencyCheck(capsuleType = capsuleType,
                            attributeNameDict = attributeNameDict) 
        __addConditionalSourcingWithConsistency(capsuleType = capsuleType,
                                              attributeNameDict = attributeNameDict,
                                              relationshipType = relationshipType)
    __addOverAllConsistencyCheck(capsuleType = capsuleType,
                               relationshipNames = relationshipNames)      
    __addOverAllConditionalSourcingWithConsistency(capsuleType = capsuleType,
                                                 relationshipNames = relationshipNames)      
