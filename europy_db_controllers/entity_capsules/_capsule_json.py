import json, typing, uuid, datetime
from sqlalchemy import orm as sqlalchemy_orm

from . import _capsule_base
from . import _capsule_utils

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S%z'


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        elif isinstance(obj, (datetime.datetime, datetime.date)):
          return str(obj)
        return json.JSONEncoder.default(self, obj)

def __addToJsonFunction(capsuleType: type[T],
                        callingGlobals):
  def fncToDict(self: T,
                omitIds: bool = False) -> dict[str, any]:
    result = {}
    sqlalchemyTableType = self.sqlalchemyTableType
    table = sqlalchemyTableType.__table__
    noJsonFields = sqlalchemyTableType._exclude_from_json
    noChangeTrackColumns = _capsule_utils.getNonChangeTrackColumns(table = table,
                                                                  callingGlobals = callingGlobals)
    for column in noChangeTrackColumns:
      columnName = column.name 
      if columnName in noJsonFields:
        pass # Exclude unwanted fields from JSON
      else:
        isRelationshipColumn = _capsule_utils.isRelationshipIdColumnName(columnName = columnName)
        isIdColumn = columnName == 'id'
        if isRelationshipColumn:
          relationship = _capsule_utils.getRelationship(table = table,
                                                        column = column,
                                                        callingGlobals = callingGlobals)
          relationshipCapsuleType = _capsule_utils.getRelationshipCapsuleTypeOfName(
                                                        relationshipName = relationship.key, 
                                                        table = table, 
                                                        callingGlobals = callingGlobals)
          if hasattr(relationshipCapsuleType, 'name'):
            relationshipEntityNameAttributeName = _capsule_utils.convertRelationshipNameToNameField(
                                    relationshipName=relationship.key)
            result[relationshipEntityNameAttributeName] = getattr(self, relationshipEntityNameAttributeName)
        columnVal = getattr(self.sqlalchemyTable, columnName)
        if columnVal is None: columnVal = None
        if type(columnVal) is uuid.UUID: columnVal = columnVal
        if type(columnVal) is datetime.datetime: columnVal = columnVal #f'{columnVal.strftime(DATETIME_FORMAT)}'
        if (isRelationshipColumn or isIdColumn) and omitIds:
          result[columnName] = None
        else:
          result[columnName] = columnVal
    # ********** relationship **********
    # **
    rel_dict = {}
    nameOfDictFnc = _capsule_utils.getToDictFncName()
    for relationship in sqlalchemyTableType.__mapper__.relationships:
      relationshipName = relationship.key
      if not relationshipName in noJsonFields:
        if relationship.uselist:
          sqlalchemyTableType = capsuleType.sqlalchemyTableType
          isDisplayList = _capsule_utils.isDisplayList(sqlalchemyTableType = sqlalchemyTableType,
                                                       relationship = relationship)
          if not isDisplayList:
            relationshipDict: dict[int, any] = {}
            countOfRelationshipEntities: int = 0
            for relationshipEntity in getattr(self, relationshipName):
              relationshipDict[countOfRelationshipEntities] = \
                      getattr(relationshipEntity, nameOfDictFnc)(omitIds = omitIds)
              countOfRelationshipEntities += 1
            result[relationshipName] = relationshipDict
        else:
          relationshipEntity = getattr(self, relationshipName)
          if not relationshipEntity is None: 
            rel_dict = relationshipEntity.toDict(omitIds = omitIds)
            result[relationshipName] = rel_dict
          else: 
            result[relationshipName] = None
    # **
    # ****************************************
    return result
  def fncToJson(self: T) -> json.decoder: # ????
    objDict: dict[str, any] = {}
    objDict = fncToDict(self)
    return json.dumps(objDict, cls=UUIDEncoder, indent=2) # json derived form dict
  

  nameOfDictFnc = _capsule_utils.getToDictFncName() # define in _capsule_utils
  nameOfJsonFnc = _capsule_utils.getToJsonFncName() # define in _capsule_utils
  fncToDictDecorated =  _capsule_base.cleanAndCloseSession(
                      func = fncToDict)
  fncToJsonDecorated =  _capsule_base.cleanAndCloseSession(
                      func = fncToJson)
  setattr(capsuleType, nameOfDictFnc, fncToDictDecorated)
  setattr(capsuleType, nameOfJsonFnc, fncToJsonDecorated)


  
  
def __addFromJsonFunction(capsuleType: type[T],
                          callingGlobals):
  nameOfDictFnc = _capsule_utils.getFromDictFncName()
  nameOfJsonFnc = _capsule_utils.getFromJsonFncName()
  def fncFromDict(self, 
                  session: sqlalchemy_orm.Session,
                  capsuleDict: dict[str, any],
                  persistentMustHaveId: bool = False,
                  relationshipEntitiesCatalog: typing.Dict[str, dict] = None) -> T:
    def ensureKeyInDict(key: str):
      if not key in capsuleDict:
        raise Exception(f"Missing key in dictionary provided to {nameOfDictFnc} of " + \
                        f"class {self.__name__}. \n" + \
                        f"Key missing: {key}.\n" + \
                        "Dictionary:\n" + str(capsuleDict))
    ##
    ## Identification whether relationshipEntitiesCatalog is provided, or not
    hasRelationshipEntitiesCatalog = not relationshipEntitiesCatalog is None
    ## Identify the object class of the table
    sqlalchemyTableType = self.sqlalchemyTableType
    ## Identify the sqlalchemy table
    table = sqlalchemyTableType.__table__
    ## Identify the fields that are excluded from json
    noJsonFields = sqlalchemyTableType._exclude_from_json
    ## Identify the columns that are not merely used for change tracking
    noChangeTrackColumns = _capsule_utils.getNonChangeTrackColumns(table = table,
                                                                  callingGlobals = callingGlobals)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Define inputs to __init__
    #     - this does NOT include any relationships 
    #     Attention: 
    #       - in case of validations that are made upon relationships, this will cause
    #         the validation to fail, as the relationships are not yet initialized
    # [2027-10-04] - decision taken on CoreAccountTable: 
    #       - include another column in the table (level_depth)
    #       - condition the is_bookable validation on the level_depth value
    #       
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    initParameters = {"session": session}
    for column in noChangeTrackColumns:
      isRelationshipColumn = _capsule_utils.isRelationshipIdColumnName(columnName = column.name)
      if isRelationshipColumn:
        # # check if an entity with the relationship's name exists on db
        # #     if yes: include the the name in the parameters
        # #     if  no: do not include as it will cause and error
        # dictOfAttributeNames = _capsule_utils.getDictOfRelationshipAttrName(
        #                 column = column)
        # relationshipName = dictOfAttributeNames[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
        # relationshipNameAttributeName = dictOfAttributeNames[_capsule_utils.REL_ATTR_DICT_KEY_NAME]
        # relationshipCapsuleClass = _capsule_utils.getRelationshipCapsuleTypeOfName(
        #                 relationshipName = relationshipName,
        #                 table = table,
        #                 callingGlobals = callingGlobals)
        # if hasattr(relationshipCapsuleClass, 'name'):
        #   ensureKeyInDict(key = relationshipNameAttributeName)
        #   nameOfRelationshipEntity = capsuleDict[relationshipNameAttributeName]
        #   if nameOfRelationshipEntity is None: continue # no relationship name on capsuleDict

        #   # if not relationshipCapsuleClass.nameExists(
        #   #                   session=session, 
        #   #                   name=nameOfRelationshipEntity):
        #   #   jsonSpec = json.dumps(capsuleDict, cls=UUIDEncoder, indent = 4)
        #   #   raise Exception(f"Badly specified relationship name on {capsuleType.__name__}:\n" + \
        #   #                   f"No relationship with such name identified on db or session.\n" + \
        #   #                   f"Relationship           : {relationshipName}\n" + \
        #   #                   f"Name provided          : {nameOfRelationshipEntity}\n" + \
        #   #                   f"Provided on attribute  : {relationshipNameAttributeName}\n" + \
        #   #                   f"json spec: \n" + \
        #   #                     jsonSpec)
        #   #   # FIXME: spec test for this error
        #   # else:
        #   initParameters[relationshipNameAttributeName] = nameOfRelationshipEntity
        pass
      else:
        columnName = column.name
        initParameters[columnName] = capsuleDict[columnName]
    # initialize the capsule here (before handling the [possibly] provided 
    #     dict definitions of the capsule's relationships
    #     Justification: In case of reading a dict without Ids -
    #                    If the name of a relationship is provided in the
    #                    main capsule's dict, the relationship's capsule must 
    #                    be identified. 
    #                    Otherwise, the relationship's capsule will go without
    #                    id and violate consistency constraints. 
    result = capsuleType(**initParameters)
    initParameters = None # not used anymore
    result.addToSession()
    # Test if entity has an id that is not provided by the input dict
    if persistentMustHaveId:
      resultId = result.id
      capsuleDictId = capsuleDict['id']
      if not resultId is None and capsuleDictId is None:
        jsonSpec = json.dumps(capsuleDict, cls=UUIDEncoder, indent = 4)
        raise Exception(f"Trying to upload an existing entity to the database: {result.name}\n" + \
                        f"Id found in the database:  {resultId.hex}\n" + \
                        f"json spec: \n" + \
                          jsonSpec)
      # Comment: if capsuleDictId is not none, the id of the result is necessarily equal
      #          to resultId - no test or error needed. 

    # if the relationships' json has been provided, 


    for column in noChangeTrackColumns:
      columnName = column.name

      if not columnName in noJsonFields:
        # print(f"\ncolumnName: {columnName}")
        isRelationshipColumn = _capsule_utils.isRelationshipIdColumnName(columnName = columnName)
        if isRelationshipColumn:
          # print(f"   RelationshipColumn: {columnName}")
          dictOfAttributeNames = _capsule_utils.getDictOfRelationshipAttrName(
                          column = column)
          relationshipName = dictOfAttributeNames[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
          ## 
          ## add relationshipName to relationshipEntitiesCatalog:
          if hasRelationshipEntitiesCatalog:
            if not relationshipName in relationshipEntitiesCatalog:
              relationshipEntitiesCatalog[relationshipName] = {}
          ##
          relationshipCapsuleClass = _capsule_utils.getRelationshipCapsuleTypeOfName(
                          relationshipName = relationshipName,
                          table = table,
                          callingGlobals = callingGlobals)
          relationshipNameAttributeName = dictOfAttributeNames[_capsule_utils.REL_ATTR_DICT_KEY_NAME]
          if relationshipName in capsuleType.sqlalchemyTableType._exclude_from_json:
            # 
            # Identify the dictionary of previously identified relationship entities:
            if hasRelationshipEntitiesCatalog:
              thisRelationshipEntitiesCatalog = relationshipEntitiesCatalog[relationshipName]
            else:
              thisRelationshipEntitiesCatalog = {}
            # Relationship is not available on the dict as as sub-dict
            #   but is identified by it's name
            #   Such name must be identifiable on the db (or as new/dirty)
            if hasattr(relationshipCapsuleClass, 'name'):
              ensureKeyInDict(key = relationshipNameAttributeName)
              relationshipEntityName = capsuleDict[relationshipNameAttributeName]
              # relationshipEntityName might not be defined -> do nothing
              if relationshipEntityName is None: continue
              if not relationshipEntityName in thisRelationshipEntitiesCatalog:
                # check if an entity with the relationship's name exists on db
                #     if yes: include the the relationshipEntity in the catalog of relationship entities
                #     if  no: do not include as it will cause and error
                if not relationshipCapsuleClass.nameExists(
                                  session=session, 
                                  name=relationshipEntityName):
                  jsonSpec = json.dumps(capsuleDict, cls=UUIDEncoder, indent = 4)
                  raise Exception(f"Badly specified relationship name on {capsuleType.__name__}:\n" + \
                                  f"No relationship with such name identified on db or session.\n" + \
                                  f"Relationship           : {relationshipName}\n" + \
                                  f"Name provided          : {relationshipEntityName}\n" + \
                                  f"Provided on attribute  : {relationshipNameAttributeName}\n" + \
                                  f"json spec: \n" + \
                                    jsonSpec)
                else:
                  relationshipEntity = relationshipCapsuleClass(
                                  session = session, 
                                  name = relationshipEntityName)
                  ## 
                  ## add relationshipEntity to catalog
                  # if hasRelationshipEntitiesCatalog:
                  #   print(f"    adding relationshipEntity of name '{relationshipEntityName}' to catalog")
                  thisRelationshipEntitiesCatalog[relationshipEntityName] = relationshipEntity
              ##
              ## Identify the relationshipEntity from the catalog of relationship entities
              relationshipEntity = thisRelationshipEntitiesCatalog[relationshipEntityName]
              ##
              setattr(result, relationshipName, relationshipEntity) 
            continue # no relationship defined in dict
          else:
            # Relationship is available as a sub-dict within the capsuleDict provided
            ensureKeyInDict(key = relationshipName)
            relationshipDict = capsuleDict[relationshipName]
            # print(f"   relationshipDict: {relationshipDict}")      
            # if no relationship defined on capsuleDict -> do nothing
            if relationshipDict is None or len(relationshipDict) == 0: continue 
            # identify if relationship has been identified in previous initialization of 
            #   the capsule, see above [result = capsuleType(**initParameters)]
            isIdentifiedRelationship = not getattr(result, relationshipName) is None
            if isIdentifiedRelationship:
              idOnRelationshipDict = relationshipDict['id']
              relationshipIdOnMainCapsule = getattr(result, columnName)
              # if relationship is not identified by name but by id check id consistency
              if not idOnRelationshipDict is None:
                if type(idOnRelationshipDict) is uuid.UUID:
                  idOnRelationshipDict = str(idOnRelationshipDict)
                if idOnRelationshipDict != str(relationshipIdOnMainCapsule):
                  # raise exception if the id of the relationship in dict is different from the 
                  #   id in the relationship's id on the identified entity
                  jsonSpec = json.dumps(capsuleDict, cls=UUIDEncoder, indent = 4)
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
                  jsonSpec = json.dumps(capsuleDict, cls=UUIDEncoder, indent = 4)
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
              relationshipEntity = getattr(relationshipCapsuleClass, nameOfDictFnc)(
                              session = session, 
                              capsuleDict = relationshipDict)
              relationshipEntity.addToSession()
              setattr(result, relationshipName, relationshipEntity)
            else:
              # print('     relationship dict: ', relationshipDict)
              relationshipEntity = getattr(relationshipCapsuleClass, nameOfDictFnc)(
                              session = session,
                              capsuleDict = relationshipDict)
              relationshipEntity.addToSession()
              setattr(result, relationshipName, relationshipEntity)

    # append values to manipulation lists
    for relationship in sqlalchemyTableType.__mapper__.relationships:
      relationshipName = relationship.key
      # if not relationshipName in noJsonFields:
      relationshipCapsuleClass = _capsule_utils.getRelationshipCapsuleTypeOfName(
                      relationshipName = relationshipName,
                      table = table,
                      callingGlobals = callingGlobals)
      # if not relationshipName in noJsonFields:
      if relationship.uselist:
        sqlalchemyTableType = capsuleType.sqlalchemyTableType
        isDisplayList = _capsule_utils.isDisplayList(sqlalchemyTableType = sqlalchemyTableType,
                                                    relationship = relationship)
        if not isDisplayList:
          ensureKeyInDict(key = relationshipName)
          appendToListFncName = _capsule_utils.getAppendToListOfPropertyFncName(
                    relationshipName = relationshipName)
          listDictionary = capsuleDict[relationshipName]
          if len(listDictionary) == 0:
            pass
          else:
            for pos in range(0, len(capsuleDict[relationshipName])):
              capsuleDictEntity = capsuleDict[relationshipName][pos]
              relationshipEntity = getattr(relationshipCapsuleClass, nameOfDictFnc)(
                        session = session,
                        capsuleDict = capsuleDict[relationshipName][pos])
              getattr(result, appendToListFncName)(relationshipEntity)     
              
    result.addToSession()
    return result
    
  def fncFromJson(self: type[T], 
                  session: sqlalchemy_orm.Session,
                  capsuleJson: json.decoder) -> T:
    dictFromJson = json.loads(capsuleJson)
    obj = getattr(self, fncFromDict)(
                        session = session, 
                        capsuleDict = dictFromJson)
    return obj 
  fncFromDictDecorated =  _capsule_base.cleanAndCloseSession(
                      func = fncFromDict)
  fncFromJsonDecorated =  _capsule_base.cleanAndCloseSession(
                      func = fncFromJson)
  fncFromDictClassFnc = classmethod(fncFromDictDecorated)
  fncFromJsonClassFnc = classmethod(fncFromJsonDecorated)
  setattr(capsuleType, nameOfDictFnc, fncFromDictClassFnc)
  setattr(capsuleType, nameOfJsonFnc, fncFromJsonClassFnc)



def addJsonFunctions(capsuleList: typing.List[T],
                       callingGlobals):
  for capsuleType in capsuleList:
    __addToJsonFunction(capsuleType = capsuleType,
                        callingGlobals = callingGlobals)
    __addFromJsonFunction(capsuleType = capsuleType,
                          callingGlobals = callingGlobals)