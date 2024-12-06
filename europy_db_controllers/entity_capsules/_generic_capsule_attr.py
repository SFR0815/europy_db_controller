import typing, datetime, sys, uuid, enum, sqlalchemy

from sqlalchemy.ext import declarative as sqlalchemy_decl
from sqlalchemy.ext import hybrid as sqlalchemy_hyb


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils, _capsule_shared

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)
V = typing.TypeVar("V", bound=_capsule_base.CapsuleBase)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Utils
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The enumeration of relationship attributes
class __RelationshipPropertyEnum(enum.Enum):
  ID = 0
  NAME_INTERNAL = 1
  SQLALCHEMY_TABLE = 2
# Setting values to relationship attributes on a capsule (& resp. sqlalchemy bbles) general 
#   function
def __setRelationshipAttributeValuesMain(self: T,
                                         dictAttributeNamingConventions: dict[str, str],
                                         sqlaId: uuid.UUID,
                                         sqlalchemyTable: sqlalchemy_decl.DeclarativeMeta,
                                         name: str):
  relationshipIdAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  if sqlalchemyTable is not None:
    if hasattr(sqlalchemyTable, 'name'):
      name = getattr(sqlalchemyTable, 'name')
    sqlaId = getattr(sqlalchemyTable, 'id')
  if hasattr(type(self), relationshipNameCapsuleInternalAttr):
    setattr(self, relationshipNameCapsuleInternalAttr, name) 
  setattr(self.sqlalchemyTable, relationshipName, sqlalchemyTable)
  setattr(self.sqlalchemyTable, relationshipIdAttr, sqlaId)
  fncNameSourceAndConsistency = _capsule_utils.getSourceAndConsistencyCheckFncName(
                                relationshipName = relationshipName)
  getattr(self, fncNameSourceAndConsistency)()
  self._hasValueInput = True    
  pass
# Define the list of values according to enumeration of relationship attributes
def __getRelationshipAttributeValues(value: typing.Union[uuid.UUID, sqlalchemy_decl.DeclarativeMeta, str],
                                     valueType: __RelationshipPropertyEnum):
  result = []
  for valuePos in range(0, len(__RelationshipPropertyEnum)):
    if valuePos == valueType.value:
      result.append(value)
    else:
      result.append(None)
  return result
# Setting values to relationship attributes on a capsule (& resp. sqlalchemy tables) based on 
#    enumeration of relationship attributes
def setRelationshipAttributeValues(self: T,
                                   dictAttributeNamingConventions: dict[str, str],
                                   value: typing.Union[uuid.UUID, sqlalchemy_decl.DeclarativeMeta, str],
                                   valueType: __RelationshipPropertyEnum):
  valueList = __getRelationshipAttributeValues(value = value,
                                               valueType = valueType)
  __setRelationshipAttributeValuesMain(self = self,
                                       dictAttributeNamingConventions = dictAttributeNamingConventions,
                                       sqlaId = valueList[__RelationshipPropertyEnum.ID.value],
                                       sqlalchemyTable = valueList[__RelationshipPropertyEnum.SQLALCHEMY_TABLE.value],
                                       name = valueList[__RelationshipPropertyEnum.NAME_INTERNAL.value])
# Check whether a relationship's sqlalchemyTable modified an object
def isModifyingObject(parentObj: T, 
                      childObj: U, 
                      relationshipName: str) -> bool:
  relationshipIdAttr = f"{relationshipName}_id"
  relationshipName_capsule_attr = f"{relationshipName}_name"
  def getParentPropertyNameAttr(parentObj: T):
    try: return getattr(parentObj, relationshipName_capsule_attr)
    except: return None
  def getChildPropertyNameAttr(childObj: U):
    try: return getattr(childObj, 'name')
    except: return None
  relationshipIdAttr = f"{relationshipName}_id"
  parentObjPropId = getattr(parentObj, relationshipIdAttr) 
  parentObjPropName = getParentPropertyNameAttr(parentObj=parentObj)
  parentPropsNone = (parentObjPropId is None) and (parentObjPropName is None)
  if (childObj is None) != parentPropsNone: return True
  childObjPropId = getattr(childObj.sqlalchemyTable, 'id')
  childObjPropName = getChildPropertyNameAttr(childObj=childObj)
  if childObjPropId != parentObjPropId or \
      childObjPropName != parentObjPropName: 
    return True
  return False
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 1. Defining getter, setter properties and omit-if-none methods all data columns not 
#    named 'name' or 'id' (latter with special treatment)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def addDataColumnAttributes(capsuleList: typing.List[T],
                            callingGlobals):
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # a. the function setting getter, setter and omit if None
  def addSetterAndGetter(capsuleType: type[T], 
                         attributeInfo: typing.List[typing.Union[str, bool]]):

    doPrint = (capsuleType.__name__ == "MarketAndForwardTransactionCapsule")
    if doPrint:
      print(f"addDataColumnAttributes {capsuleType.__name__}")

  
    attributeName = attributeInfo[0]
    hasSetter = attributeInfo[2]
    def getterFnc(self):
      return getattr(self.sqlalchemyTable, attributeName)
    def setterFnc(self, value):
      # Check if the attribute has a validator
      if hasattr(self.sqlalchemyTable, f'validate_{attributeName}'):
        # If the sqlalchemyTable is not in the session, add it
        if self.sqlalchemyTable not in self.session:
          self.session.add(self.sqlalchemyTable)
        # Call the validator
        getattr(self.sqlalchemyTable, f'validate_{attributeName}')(attributeName, value)
      if self.sqlalchemyTable.id is not None:
        if getattr(self.sqlalchemyTable, attributeName) != value:
          self.sqlalchemyTable.modified_at = datetime.datetime.now()
      setattr(self.sqlalchemyTable, attributeName, value)
      self._hasValueInput = True
    def omitNonFnc(self, value):
      if value is None: return # do nothing if value is 'None'
      setattr(self, attributeName, value)
    # add getter

    if doPrint:
      print(f"    implementing getterFnc: {attributeName}")

    propertyGetter = property(_capsule_base.cleanAndCloseSession(getterFnc))
    setattr(capsuleType, attributeName, propertyGetter)
    # add setter and omitNone if and only if <hasSetter> is True

    if doPrint:
      print(f"    implementing setterFnc: {attributeName}")

    if hasSetter:
      propertySetter = propertyGetter.setter(_capsule_base.cleanAndCloseSession(setterFnc))
      setattr(capsuleType, attributeName, propertySetter)
      omitNoneFncName = _capsule_utils.getOmitIfNoneFncName(
              attributeName = attributeName)
      setattr(capsuleType, omitNoneFncName, _capsule_base.cleanAndCloseSession(omitNonFnc)) 
  def addHybridGetter(capsuleType: type[T], 
                      attributeName: str):
    def getterFnc(self):
      return getattr(self.sqlalchemyTable, attributeName)
    propertyGetter = property(_capsule_base.cleanAndCloseSession(getterFnc))
    setattr(capsuleType, attributeName, propertyGetter)
  for capsuleType in capsuleList:

    doPrint = (capsuleType.__name__ == "MarketAndForwardTransactionCapsule")
    if doPrint:
      print(f"addDataColumnAttributes {capsuleType.__name__}")

    # getSqlalchemyColumnsAndColumnLikeProperties(capsuleType = capsuleType) 
    #       -> dict{<nameOfItem>: [<nameOfItem>, <isHybridProperty>, <hasSetter]}
    sqlalchemyColumnsAndColumnLikeProperties = _capsule_utils.getSqlalchemyColumnsAndColumnLikeProperties(
                                                capsuleType = capsuleType)  
    for attributeName, attributeInfo in sqlalchemyColumnsAndColumnLikeProperties.items():
      noHybridProperty = not attributeInfo[1]
      isRelationshipIdColumn = _capsule_utils.isRelationshipIdColumnName(columnName = attributeName)

      if doPrint:
        print(f"\n    attributeInfo: {attributeName} {attributeInfo} isRelationshipIdColumn: {isRelationshipIdColumn} noHybridProperty: {noHybridProperty}")
        print(f"      continue? {isRelationshipIdColumn or noHybridProperty}")

      if isRelationshipIdColumn and noHybridProperty: continue

      if doPrint:
        print(f"    adding setter and getter for {attributeName}")

      addSetterAndGetter(capsuleType = capsuleType, attributeInfo = attributeInfo)


    #   # Only columns not hidden for internal processing (change tracking)
    #   columns = _capsule_utils.getNonChangeTrackColumns(sqlalchemyTableType = capsuleType.sqlalchemyTableType)
    
    # # iterate over sqlalchemyColumnsAndColumnLikeProperties

    # for column in columns:
    #   columnName = column.name
    #   if columnName not in ['id', 'name']:
    #     # exclude in addition:
    #     #    - columns on which properties are set by CapsuleBase 
    #     #      (see module _capsule_base)
    #     #    - columns whose name complies with relationship naming conventions
    #     #      (see module _capsule_utils)
    #     isBaseColumn = _capsule_utils.isBaseColumn(capsuleType = capsuleType,
    #                                               column = column)
    #     isRelationshipIdColumn = _capsule_utils.isRelationshipIdColumn(column)
    #     if not (isBaseColumn or isRelationshipIdColumn):
    #       addSetterAndGetter(capsuleType=capsuleType, attributeName=column.name)
    #   else:
    #     pass
    # for key, objType in vars(capsuleType.sqlalchemyTableType).items():
    #   if isinstance(objType, sqlalchemy_hyb.hybrid_property):
    #     addHybridGetter(capsuleType=capsuleType, attributeName=key)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Defining getter, setter properties and omit-if-none methods for relationships (for id, name,
#    and the capsule object) defined by foreign key columns 
#    (the 'one' part of 'one'-to-'many' relationships - 1-to-1 out of scope!)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setter & getter properties related to the relationshipIdAttr
# additional features if relationship_type has a (unique) id:
def __setIdProperties(capsuleType: type[T],
                      dictAttributeNamingConventions: dict[str, str]) -> str:
  relationshipIdAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  def getterFnc(self: capsuleType) -> uuid.UUID:
    fncNameSourceAndConsistency = _capsule_utils.getConsistencyCheckFncName(
                                  relationshipName = relationshipName)
    getattr(self, fncNameSourceAndConsistency)()
    return getattr(self.sqlalchemyTable, relationshipIdAttr)
  def setterFnc(self: capsuleType, 
                id: uuid.UUID):
    if getattr(self.sqlalchemyTable, relationshipIdAttr) != id:
      # no nothing if input is same as stored
      if self.sqlalchemyTable.id is not None:
        self.sqlalchemyTable.modified_at = datetime.datetime.now()
      setRelationshipAttributeValues(self = self,
                                    dictAttributeNamingConventions = dictAttributeNamingConventions,
                                    value = id,
                                    valueType = __RelationshipPropertyEnum.ID)
  def omitNonFnc(self: capsuleType, id: uuid.UUID):
    if id is None: return
    setattr(self, relationshipIdAttr, id)
  propertyGetter = property(_capsule_base.cleanAndCloseSession(getterFnc))
  propertySetter = propertyGetter.setter(_capsule_base.cleanAndCloseSession(setterFnc))
  setattr(capsuleType, relationshipIdAttr, propertyGetter)
  setattr(capsuleType, relationshipIdAttr, propertySetter)
  omitNoneFncName = _capsule_utils.getOmitIfNoneFncName(
          attributeName = relationshipIdAttr)
  setattr(capsuleType, omitNoneFncName, _capsule_base.cleanAndCloseSession(omitNonFnc)) # No prop!!
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setter & getter properties related to the relationshipName_capsule_attr
# additional features if relationship_type has a (unique) name:
def __setNameProperties(capsuleType: type[T],
                        dictAttributeNamingConventions: dict[str, str],
                        relationshipType: type[U]) -> str:
  if not hasattr(relationshipType.sqlalchemyTableType, 'name'):
    return
  relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipNameAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_NAME]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  def getterFnc(self: T) -> str:
    fncNameSourceAndConsistency = _capsule_utils.getConsistencyCheckFncName(
                                  relationshipName = relationshipName)
    getattr(self, fncNameSourceAndConsistency)()
    return getattr(self, relationshipNameCapsuleInternalAttr)
  def setterFnc(self: capsuleType, name: str):
    if getattr(self, relationshipNameCapsuleInternalAttr) != name:
      # no nothing if input is same as stored
      if self.sqlalchemyTable.id is not None:
        self.sqlalchemyTable.modified_at = datetime.datetime.now()
      setRelationshipAttributeValues(self = self,
                                    dictAttributeNamingConventions = dictAttributeNamingConventions,
                                    value = name,
                                    valueType = __RelationshipPropertyEnum.NAME_INTERNAL)
  def omitNonFnc(self: capsuleType, name: str):
    if name is None: return # do nothing if name is 'None'      
    setattr(self, relationshipNameCapsuleInternalAttr, name)
  # set the internal relationship name attribute first
  setattr(capsuleType, relationshipNameCapsuleInternalAttr, None)
  propertyGetter = property(_capsule_base.cleanAndCloseSession(getterFnc))
  propertySetter = propertyGetter.setter(_capsule_base.cleanAndCloseSession(setterFnc))
  setattr(capsuleType, relationshipNameAttr, propertyGetter)
  setattr(capsuleType, relationshipNameAttr, propertySetter)
  omitNoneFncName = _capsule_utils.getOmitIfNoneFncName(
          attributeName = relationshipNameAttr)
  setattr(capsuleType, omitNoneFncName, _capsule_base.cleanAndCloseSession(omitNonFnc)) # No prop!!
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Setter & getter properties related to the object
def __setRelationshipObjectProperties(capsuleType: type[T],
                                      dictAttributeNamingConventions: dict[str, str],
                                      relationshipType: type[U]) -> str:

  doPrint = (capsuleType.__name__ == "MarketAndForwardTransactionCapsule")
  if doPrint:
    print(f"    __setRelationshipObjectProperties {capsuleType.__name__}")

  relationshipIdAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  def getterFnc(self) -> relationshipType:
    if not hasattr(self.sqlalchemyTable, relationshipIdAttr):
      self._raiseException(f"Illegal use of setter of property '{relationshipName}'" + \
                      f"on object {type(self)}.\n" +
                      f"Usage not permissible if '{relationshipIdAttr}' not defined on object.")
    fncNameSourceAndConsistency = _capsule_utils.getConsistencyCheckFncName(
                                  relationshipName = relationshipName)
    getattr(self, fncNameSourceAndConsistency)()
    relationshipSqlaTable = getattr(self.sqlalchemyTable, relationshipName) 
    if relationshipSqlaTable is None: 
      return None
    else: 
      
      if doPrint:
        print(f"       calling getter function for '{relationshipName}' on {capsuleType.__name__}")
      return relationshipType.defineBySqlalchemyTable(
                  session = self.session,
                  sqlalchemyTableEntity = relationshipSqlaTable)  
  def setterFnc(self, obj: relationshipType):
    if not hasattr(self.sqlalchemyTable, relationshipIdAttr):
      self._raiseException(f"Illegal use of setter of property '{relationshipName}'" + \
                      f"on object {type(self)}.\n" +
                      f"Usage not permissible if '{relationshipIdAttr}' not defined on object.")
    sqlalchemyTable = None if obj is None else obj.sqlalchemyTable
    if isModifyingObject(self, obj, relationshipName):
      if self.sqlalchemyTable.id is not None:
        self.sqlalchemyTable.modified_at = datetime.datetime.now()
      setRelationshipAttributeValues(self = self,
                                    dictAttributeNamingConventions = dictAttributeNamingConventions,
                                    value = sqlalchemyTable,
                                    valueType = __RelationshipPropertyEnum.SQLALCHEMY_TABLE)
  def omitNonFnc(self: capsuleType, obj: relationshipType):
    if obj is None: return # do nothing if value is 'None'      
    setattr(self, relationshipName, obj)
  propertyGetter = property(_capsule_base.cleanAndCloseSession(getterFnc))
  propertySetter = propertyGetter.setter(_capsule_base.cleanAndCloseSession(setterFnc))
  if doPrint:
    print(f"       relationshipName: {relationshipName}")
  setattr(capsuleType, relationshipName, propertyGetter)
  setattr(capsuleType, relationshipName, propertySetter)
  omitNoneFncName = _capsule_utils.getOmitIfNoneFncName(
          attributeName = relationshipName)
  setattr(capsuleType, omitNoneFncName, _capsule_base.cleanAndCloseSession(omitNonFnc)) # No prop!!

def __addGetValidationItemsAttribute(capsuleType: type[T],
                                     callingGlobals):
  sqlalchemyTableType = capsuleType.sqlalchemyTableType
  relationships = capsuleType.sqlalchemyTableType.__mapper__.relationships
  validationItemsFncName = _capsule_utils.getValidationItemsFncName()
  def getterFnc(self, callingTableType: type[V] = None) -> typing.List[type[U]]:
    result = capsuleType._referred_by_name_capsules
    # if the calling sqlalchemyTableType is the same as the called sqlalchemyTableType, 
    #    then no need to go through the relationships  
    #    this avoids infinite recursion on self-referencing tables
    if not callingTableType is None:
      if callingTableType.__name__ == sqlalchemyTableType.__name__:
        return result
    for relationship in relationships:
      if not relationship.key in capsuleType.sqlalchemyTableType._exclude_from_json:
        relationshipType = _capsule_utils.getRelationshipCapsuleTypeOfName(
                        relationshipName = relationship.key,
                        sqlalchemyTableType = sqlalchemyTableType,
                        callingGlobals = callingGlobals)
        relationshipValidationItems = getattr(relationshipType, validationItemsFncName)(callingTableType = sqlalchemyTableType)
        for relationshipValidationItem in relationshipValidationItems:
          if not relationshipValidationItem in result:
            result.append(relationshipValidationItem)
    return result
  propertyGetter = classmethod(getterFnc)
  setattr(capsuleType, validationItemsFncName, propertyGetter)

# Adding single relationship attributes based on the above
def addRelationshipAttributes(capsuleList: typing.List[T],
                              callingGlobals):
  for capsuleType in capsuleList:
    # reset the _referred_by_name_capsules to ensure independent list
    #   for each capsuleType

    doPrint = (capsuleType.__name__ == "MarketAndForwardTransactionCapsule")
    if doPrint:
      print(f"addRelationshipAttributes {capsuleType.__name__}")

    capsuleType._referred_by_name_capsules = []  
    sqlalchemyTableType = capsuleType.sqlalchemyTableType
    __addGetValidationItemsAttribute(capsuleType=capsuleType,
                                      callingGlobals=callingGlobals)
    sqlalchemyColumnsAndColumnLikeProperties = _capsule_utils.getSqlalchemyColumnsAndColumnLikeProperties(
                                              capsuleType = capsuleType)  
    for attributeName, attributeInfo in sqlalchemyColumnsAndColumnLikeProperties.items():
      if not _capsule_utils.isRelationshipIdColumnName(columnName = attributeName): continue
      relationshipName = _capsule_utils.getColumnToRelationshipName(columnName = attributeName)
      isHybridProperty = attributeInfo[1]
      if doPrint:
        print(f"  relationship id column: {attributeName} - isHybridProperty: {isHybridProperty}")
        print(f"  relationship name     : {relationshipName}")
      dictAttributeNamingConventions = _capsule_utils.getDictOfAttributeNamingConventionsFromRelationshipName(
                      relationshipName = relationshipName)
      if isHybridProperty:
        relationshipType = getattr(sqlalchemyTableType, relationshipName).fget.__annotations__['return']
        # Ensure that the return type is properly defined. As an example refer to:
        #    module: EStG36a.src.model.transaction.py
        #    class: MarketAndForwardTransaction 
        #    (code below has been changed - example only)
        # @sqlalchemy_hyb.hybrid_property 
        # def asset(self) -> "asset.AssetTable":
        #     RETURN_TYPE_PROPERTY_NAME = "_hyb_prop_asset_return_type"
        #     def specify_return_type(class_type_definition):
        #       print(f"[specify_return_type running]")
        #       if not hasattr(class_type_definition, RETURN_TYPE_PROPERTY_NAME):
        #           print(f"  setting {RETURN_TYPE_PROPERTY_NAME}")
        #           mkt_tx_table_class = class_type_definition.market_transaction.property.mapper.class_
        #           setattr(class_type_definition, RETURN_TYPE_PROPERTY_NAME, mkt_tx_table_class.asset.property.mapper.class_)
        #           class_type_definition.asset.fget.__annotations__['return'] = getattr(class_type_definition, RETURN_TYPE_PROPERTY_NAME)
        #     if isinstance(self, MarketAndForwardTransactionTable):
        #         specify_return_type(self.__class__)
        #         if self.market_transaction is not None:
        #             return self.market_transaction.asset
        #         else:
        #             return None
        #     else:
        #         specify_return_type(self)
        #         pass
        # @asset.setter
        # def asset(self, value):
        #     self.market_transaction.asset = value
        # Define list prefixes and check if any match
        relationShipTypeClassName = relationshipType.__name__
        list_prefixes = ['list', 'List', 'typing.List']
        prefix_index = next((i for i, prefix in enumerate(list_prefixes) 
                           if relationShipTypeClassName.startswith(prefix)), -1)
        isList = prefix_index >= 0
        # If it's a list type, get the actual table class name without the list prefix
        if isList:
            # Remove the list prefix
            if prefix_index >= 0:
                prefix = list_prefixes[prefix_index]
                relationShipTypeClassName = relationShipTypeClassName[len(prefix):]
            # Remove brackets if present
            starts_with_bracket = relationShipTypeClassName.startswith('[')
            ends_with_bracket = relationShipTypeClassName.endswith(']')
            if starts_with_bracket and ends_with_bracket:
                relationShipTypeClassName = relationShipTypeClassName[1:-1]
        hybridPropertyCapsuleClassName = _capsule_utils.getSqlaToCapsuleName(
                                            sqlaTableName = relationShipTypeClassName)
        relationshipType = callingGlobals[hybridPropertyCapsuleClassName]
      else:
        relationshipType = _capsule_utils.getRelationshipCapsuleTypeOfName(
                      relationshipName = relationshipName,
                      sqlalchemyTableType = sqlalchemyTableType,
                      callingGlobals = callingGlobals)
        relationship = capsuleType.sqlalchemyTableType.__mapper__.relationships[relationshipName]
        isList = relationship.uselist

      if doPrint:
        print(f"  relationship type     : {relationshipType.__name__} isList: {isList}")
 
      __setIdProperties(capsuleType = capsuleType,
                        dictAttributeNamingConventions = dictAttributeNamingConventions)
      __setNameProperties(capsuleType = capsuleType,
                          dictAttributeNamingConventions = dictAttributeNamingConventions,
                          relationshipType = relationshipType)
      __setRelationshipObjectProperties(capsuleType = capsuleType,
                          dictAttributeNamingConventions = dictAttributeNamingConventions,
                          relationshipType = relationshipType)
      if hasattr(relationshipType, 'name'):
        # append the relationship's capsule type to _referred_by_name_capsules
        #   used to identify permissible values for selecting by user 
        if not relationshipType in capsuleType._referred_by_name_capsules:
          # List have the reverse selection - so they are not included
          #     for both display and manipulation lists
          if not isList:
            #print(f"capsule type: {capsuleType.__name__}")
            capsuleType._referred_by_name_capsules.append(relationshipType)
      pass
    # for column in sqlalchemyTableType.__table__.columns:
    #   if _capsule_utils.isRelationshipIdColumn(column=column):
    #     relationshipName = _capsule_utils.getRelationshipNameOfColumn(column = column)
    #     dictAttributeNamingConventions = _capsule_utils.getDictOfAttributeNamingConventionsFromRelationshipName(
    #                     relationshipName = relationshipName)
    #     relationshipType = _capsule_utils.getRelationshipCapsuleTypeOfName(
    #                     relationshipName = relationshipName,
    #                     sqlalchemyTableType = sqlalchemyTableType,
    #                     callingGlobals = callingGlobals)
    #     relationship = capsuleType.sqlalchemyTableType.__mapper__.relationships[relationshipName]

    #     if doPrint:
    #       print(f"  relationship id column: {column.name}")
    #       print(f"  relationship name     : {relationshipName}")
    #       print(f"  relationship type     : {relationshipType.__name__}")
    #       print(f"  relationship          : {relationship.key}")

    #     __setIdProperties(capsuleType = capsuleType,
    #                       dictAttributeNamingConventions = dictAttributeNamingConventions)
    #     __setNameProperties(capsuleType = capsuleType,
    #                         dictAttributeNamingConventions = dictAttributeNamingConventions,
    #                         relationshipType = relationshipType)
    #     __setRelationshipObjectProperties(capsuleType = capsuleType,
    #                         dictAttributeNamingConventions = dictAttributeNamingConventions,
    #                         relationshipType = relationshipType)
    #     if hasattr(relationshipType, 'name'):
    #       # append the relationship's capsule type to _referred_by_name_capsules
    #       #   used to identify permissible values for selecting by user 
    #       if not relationshipType in capsuleType._referred_by_name_capsules:
    #         # List have the reverse selection - so they are not included
    #         #     for both display and manipulation lists
    #         if not relationship.uselist:
    #           #print(f"capsule type: {capsuleType.__name__}")
    #           capsuleType._referred_by_name_capsules.append(relationshipType)


        

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 2. Define the tables of relationships (the 'many' side of 'one'-to-'many' 
#       relationships) that are restricted to review and not allowed for manipulation
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# shared utils
def __getRelationshipSqlalchemyTables(self: T,
                                      relationshipName: str):
  relationshipSqlalchemyTables = getattr(self.sqlalchemyTable, relationshipName)
  relationshipSqlalchemyTables = [] if relationshipSqlalchemyTables is None else \
                                  relationshipSqlalchemyTables
  return relationshipSqlalchemyTables
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# shared attribute definitions
def __addCountOfListOfProperty(capsuleType: type[T],
                               relationshipName: str):
  countFncName = _capsule_utils.getCountOfListOfPropertyFncName(relationshipName)
  def getterFnc_NumberOf(self: T):
    if self.sqlalchemyTable is None: return 0
    relationshipSqlalchemyTables = __getRelationshipSqlalchemyTables(self,
                                                                    relationshipName)
    return len(relationshipSqlalchemyTables)
  getter_NumberOf = property(_capsule_base.cleanAndCloseSession(getterFnc_NumberOf))
  setattr(capsuleType, countFncName, getter_NumberOf)

def __addGetItemFunction(capsuleType: type[T],
                         relationshipName: str,
                         relationshipCapsuleClass: type[U]):
  countFncName = _capsule_utils.getCountOfListOfPropertyFncName(relationshipName)
  getFncName = _capsule_utils.getItemFromListOfPropertyFncName(relationshipName)
  def getListItem(self: T, 
                  position: int) -> relationshipCapsuleClass:
    if self.sqlalchemyTable is None: return None
    noOfItems = getattr(self, countFncName)
    if position < 0:
      self._raiseException(f"Trying to get a {relationshipCapsuleClass} from a {capsuleType}'s {relationshipName}-list. \n" + \
                      f"Only positive postion values are permissible. Position requested is {str(position)}.")
    elif position >= noOfItems:
      self._raiseException(f"Trying to get a {relationshipCapsuleClass} from a {capsuleType}'s {relationshipName}-list. \n" + \
                      f"Position value must be strictly less than len of list (={str(noOfItems)}). Position requested is {str(position)}.")
    attrList = getattr(self.sqlalchemyTable, relationshipName)

    return relationshipCapsuleClass.defineBySqlalchemyTable(
              session = self.session,
              sqlalchemyTableEntity = attrList[position])    
  setattr(capsuleType, getFncName, _capsule_base.cleanAndCloseSession(getListItem))
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Define the display list
def __addDisplayList(capsuleType: type[T],
                     relationshipName: str,
                     relationshipCapsuleClass: type[U]):
  def getterFnc(self):
    relationshipSqlalchemyTables = __getRelationshipSqlalchemyTables(self,
                                                                    relationshipName)
    for relationshipSqlalchemyTable in relationshipSqlalchemyTables:
      yield relationshipCapsuleClass.defineBySqlalchemyTable(
                session = self.session,
                sqlalchemyTableEntity = relationshipSqlalchemyTable)
  __addCountOfListOfProperty(capsuleType = capsuleType,
                             relationshipName = relationshipName)
  __addGetItemFunction(capsuleType = capsuleType,
                       relationshipName = relationshipName,
                       relationshipCapsuleClass = relationshipCapsuleClass)
  getter = property(_capsule_base.cleanAndCloseSession(getterFnc))
  setattr(capsuleType, relationshipName, getter)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Define the manipulation list
def __addManipulationList(capsuleType: type[T],
                          relationshipName: str,
                          relationshipCapsuleClass: type[U]):
  if not relationshipName.endswith('s'):
    raise Exception("Can't define a list for manipulation if rel name doesn't end with 's'.\n" + \
                    f"rel name: {relationshipName}\n" + \
                    f"On class: {capsuleType.__name__}")
  # names of function added to the capsuleType:
  countFncName = _capsule_utils.getCountOfListOfPropertyFncName(relationshipName)
  appendFncName = _capsule_utils.getAppendToListOfPropertyFncName(relationshipName)
  removeFncName = _capsule_utils.getRemoveFromListOfPropertyFncName(relationshipName)
  def getterFncList(self: T):
    if self.sqlalchemyTable is None: return 0
    relationshipSqlalchemyTables = __getRelationshipSqlalchemyTables(self,
                                                                    relationshipName)
    for relationshipSqlalchemyTable in relationshipSqlalchemyTables:
      yield relationshipCapsuleClass.defineBySqlalchemyTable(
                session = self.session,
                sqlalchemyTableEntity = relationshipSqlalchemyTable)
  def appendItem(self: T, 
                 item: relationshipCapsuleClass):
    if self.sqlalchemyTable.id is not None:
      self.sqlalchemyTable.modified_at = datetime.datetime.now()
    if not item.sqlalchemyTable in getattr(self.sqlalchemyTable, relationshipName):
      getattr(self.sqlalchemyTable, relationshipName).append(item.sqlalchemyTable)
    item.addToSession()
  def removeItem(self: T, 
                 position: int):
    if self.sqlalchemyTable.id is not None:
      self.sqlalchemyTable.modified_at = datetime.datetime.now()
    noOfItems = getattr(self, countFncName)
    if position < 0:
      self._raiseException(f"Trying to remove a {relationshipCapsuleClass} from a {capsuleType}'s {relationshipName}-list. \n" + \
                      f"Only positive postion values are permissible. Position requested is {str(position)}.")
    elif position >= noOfItems:
      self._raiseException(f"Trying to remove a {relationshipCapsuleClass} from a {capsuleType}'s {relationshipName}-list. \n" + \
                      f"Position value must be strictly less than len of list (={str(noOfItems)}). Position requested is {str(position)}.")
    del getattr(self.sqlalchemyTable, relationshipName)[position]

  __addCountOfListOfProperty(capsuleType=capsuleType,
                             relationshipName=relationshipName)
  __addGetItemFunction(capsuleType = capsuleType,
                       relationshipName = relationshipName,
                       relationshipCapsuleClass = relationshipCapsuleClass)
  getterList = property(_capsule_base.cleanAndCloseSession(getterFncList))
  setattr(capsuleType, relationshipName, getterList)
  setattr(capsuleType, appendFncName, _capsule_base.cleanAndCloseSession(appendItem))
  setattr(capsuleType, removeFncName, _capsule_base.cleanAndCloseSession(removeItem))


def addListAttributes(capsuleList: typing.List[T],
                             callingGlobals):
  for capsuleType in capsuleList:
    sqlalchemyTableType = capsuleType.sqlalchemyTableType
    table = sqlalchemyTableType.__table__
    tableTypeName = _capsule_utils.getSqlalchemyTableTypeName(table=table)
    tableType = callingGlobals[tableTypeName]
    relationshipNames: typing.List[str] = []
    for relationship in tableType.__mapper__.relationships:
      # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      # Identify relationships that are lists (others treated above - if conventions met)
      if relationship.uselist:
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Identify capsule class of the relationship                                                   relationship = relationship)
        relationshipSqlaTableClassName = relationship.mapper.class_.__name__
        relationshipCapsuleClassName = _capsule_utils.getSqlaToCapsuleName(relationshipSqlaTableClassName)
        relationshipCapsuleClass = callingGlobals[relationshipCapsuleClassName]
        isDisplayList = _capsule_utils.isDisplayList(sqlalchemyTableType = sqlalchemyTableType,
                                                     relationship = relationship)
        relationshipName = relationship.key
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Identify relationships that are lists (others treated above - if conventions met)                                                   relationship = relationship)
        if isDisplayList:
          __addDisplayList(capsuleType = capsuleType,
                           relationshipName = relationshipName, 
                           relationshipCapsuleClass = relationshipCapsuleClass)
        else: 
          relationshipPropertyName = relationshipName.removesuffix("s")
          __addManipulationList(capsuleType = capsuleType,
                           relationshipName = relationshipName, 
                           relationshipCapsuleClass = relationshipCapsuleClass)
      else:
        pass
