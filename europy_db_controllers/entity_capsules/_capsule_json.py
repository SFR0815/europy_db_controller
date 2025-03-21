import json, typing, uuid, datetime

import sqlalchemy as sqlalchemy
from sqlalchemy import orm as sqlalchemy_orm
from sqlalchemy.ext import declarative as sqlalchemy_decl

from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import consistency_checks, basic_capsule_initialization
from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import basic_capsule_initialization
from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import single_entity_relationships
from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import multiple_entity_relationships

from . import _capsule_base
from . import _capsule_utils

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

DEBUG_CAPSULE_TYPE = 'TxTypeToParameterMapCapsule'
DEBUG_CAPSULE_TYPE_SINGLE_RELATIONSHIP = 'ProjectAssetCapsule'

DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'


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
    noJsonFields = sqlalchemyTableType._exclude_from_json
    noChangeTrackColumns = _capsule_utils.getNonChangeTrackColumns(sqlalchemyTableType = sqlalchemyTableType)
    for column in noChangeTrackColumns:
      columnName = column.name 
      if columnName in noJsonFields:
        pass # Exclude unwanted fields from JSON
      else:
        isRelationshipColumn = _capsule_utils.isRelationshipIdColumnName(columnName = columnName)
        isIdColumn = columnName == 'id'
        if isRelationshipColumn:
          relationship = _capsule_utils.getRelationship(sqlalchemyTableType = sqlalchemyTableType,
                                                        column = column)
          relationshipCapsuleType = _capsule_utils.getRelationshipCapsuleTypeOfName( 
                                                        relationshipName = relationship.key, 
                                                        sqlalchemyTableType = sqlalchemyTableType, 
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
      # print(f"\n\n[_capsule_json.fncToDict] - relationshipName: {relationshipName}")
      if not relationshipName in noJsonFields:
        if relationship.uselist:
          sqlalchemyTableType = capsuleType.sqlalchemyTableType
          isDisplayList = _capsule_utils.isDisplayList(sqlalchemyTableType = sqlalchemyTableType,
                                                       relationshipName = relationship.key)
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
    # Initiate if relationshipEntitiesCatalog is provided:
    if relationshipEntitiesCatalog is None:
      relationshipEntitiesCatalog: typing.Dict[str, dict] = {}
    columnsAndAlikeInfo = _capsule_utils.getCapsuleInitColumnsAndColumnLikeProperties(capsuleType = capsuleType)
    result, is_not_in_session = basic_capsule_initialization.getInitializedBasicCapsule(session = session,
                                                                     capsuleType = capsuleType,
                                                                     columnsAndAlikeInfo = columnsAndAlikeInfo,
                                                                     capsuleDict = capsuleDict,
                                                                     persistentMustHaveId = persistentMustHaveId)
    single_entity_relationships.addSingleRelatedEntities(resultEntity = result,
                                                        session = session,
                                                        capsuleType = capsuleType,
                                                        capsuleDict = capsuleDict,
                                                        columnsAndAlikeInfo = columnsAndAlikeInfo,
                                                        relationshipEntitiesCatalog = relationshipEntitiesCatalog,
                                                        callingGlobals = callingGlobals)

    multiple_entity_relationships.addIncludedInJsonMultipleRelatedEntities(resultEntity = result,
                                                                          session = session,
                                                                          capsuleType = capsuleType,
                                                                          capsuleDict = capsuleDict,
                                                                          callingGlobals = callingGlobals)
    # DO NOT add anything to session. THe adding took place in getInitializedBasicCapsule, see above.
    
    if is_not_in_session:
      #Invoke runAtAddToSession() here as entity definition is now complete 
      #   Do this IF AND ONLY IF the entity was NOT in the session before capsule initialization
      result.runAtAddToSession()
    if result.sqlalchemyTable._flush_after_add_to_session:
      # print(f"     [fncFromDict ({capsuleType.__name__})] - flushing session")
      # if result.__class__.__name__ == 'AssetClassCapsule':
      #   print(f"     capsuleDict: {json.dumps(capsuleDict, indent=4)}")
      session.flush()
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
