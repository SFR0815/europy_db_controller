import typing, sys, json, uuid, datetime

from . import controller, _controller_utils, _controller_base
from sqlalchemy import orm as sqlalchemy_orm

from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils, _capsule_json

T = typing.TypeVar('T', bound=_controller_base.ControllerBase)
CT = typing.TypeVar('CT', bound=_capsule_base.CapsuleBase)


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        elif isinstance(obj, (datetime.datetime, datetime.date)):
          return str(obj)
        return json.JSONEncoder.default(self, obj)


def __addToDictFunctions(controllerType : type[T],
                        controllerKeyEnumType: typing.Type,
                        callingGlobals):
  nameOfDictFnc = _controller_utils.getControllerDataToDictFncName()
  nameOfJsonFnc = _controller_utils.getControllerDataToJsonFncName()
  def fncToDict(self: T,
                scope: _controller_base.ControllerDataScopes = 
                          _controller_base.ControllerDataScopes.NEW_AND_DIRTY,
                omitIds: bool = False,
                subControllerSelected: controllerKeyEnumType = None,
                fullListsFromDb: typing.List[typing.List[str]] = [] # specify lists that should come from db
                ) -> dict[str, any]:
    if not (isinstance(subControllerSelected, controllerKeyEnumType) or subControllerSelected is None):
      raise Exception("Got non enum subControllerSelected: ", subControllerSelected)
    result = {}
    validationItems: typing.List[type[CT]] = list[type[CT]]()
    if len(self._key) == 0:
      validationItemLocators = typing.List[tuple[str, str]]
      for subController in vars(self).values():
        if isinstance(subController, _controller_base.ControllerBase):
            subControllerKey = subController._key
            # reduce fullListFromDb according to the current search level
            #   (one step deeper) -> nextFullListsFromDb
            nextFullListsFromDb: typing.List[typing.List[str]] = []
            for fullListFromDb in fullListsFromDb:
              if fullListFromDb[0] == subControllerKey:
                if len(fullListFromDb) > 1:
                  nextFullListsFromDb.append(fullListFromDb[1:])
            #   [nextFullListsFromDb completed]
            # if len(subControllerSelected) == 0 or subControllerKey == subControllerSelected:
            subControllerSelectedNone = subControllerSelected is None
            subControllerSelectedVal: str = "" 
            if not subControllerSelectedNone:
              subControllerSelectedVal = subControllerSelected.value
            if subControllerSelectedNone or subControllerKey == subControllerSelectedVal:
              result[subControllerKey] = getattr(subController, nameOfDictFnc)(scope = scope,
                                                                               omitIds = omitIds,
                                                                               subControllerSelected = None,
                                                                               fullListsFromDb = nextFullListsFromDb)
              # add the validation items of the subcontroller (these are unique)
              validationItems = getattr(subController, _capsule_utils.getValidationItemsFncName())(
                                validationItems = validationItems)
      # Add validation information if and only if a subController is selected 
      if not subControllerSelected is None:
        validationItemLocators = self.__class__.getValidationItemLocators(validationItems = validationItems)
        # iterate unique validationItemLocators
        for validationItemLocator in validationItemLocators:
          subControllerKey, capsuleKey = validationItemLocator
          # ensure the set of all required json fields for the subController
          if not subControllerKey in result: result[subControllerKey] = {}
          validationSubController = self.getSubControllerOfKey(subControllerKey = subControllerKey)
          requiredFields = validationSubController._keys
          for requiredField in requiredFields:
            if not requiredField in result[subControllerKey]:
              result[subControllerKey][requiredField] = {}
              result[subControllerKey][requiredField + "_delete"] = {}
          # always add at least an empty dict for the subController
          if not subControllerKey in result:
            result[subControllerKey] = {}  
          # do not include data from the same subController 
          #   Theses must be validated based on values possible manipulated by the user on 
          #   user interfaces (e.g. Excel)
          if subControllerKey != subControllerSelected.value:
            # Define the dict of capsules stored on the db
            subController = self.getSubControllerOfKey(subControllerKey = subControllerKey)
            capsulesByKeyFncName = _controller_utils.getControllerIterByKeyFncName()
            countOfCapsule = 0
            numberedCapsulesDict = {}
            capsules = getattr(subController, capsulesByKeyFncName)(
                          capsuleKey = capsuleKey, 
                          scope = _controller_base.ControllerDataScopes.STORED_ON_DB)
            for capsule in capsules:
              if capsule == None:
                numberedCapsulesDict = None
              else:
                capsuleDict = {}
                capsuleDict['name'] = capsule.name
                numberedCapsulesDict[countOfCapsule] = capsuleDict
                countOfCapsule += 1
            result[subControllerKey][capsuleKey] = numberedCapsulesDict
            result[subControllerKey][capsuleKey + "_delete"] = {}
    else:
      capsuleKeys = self._keys
      capsulesDict = {}
      for capsuleKeyCount in range(0, len(capsuleKeys)):
        capsuleKey = self._keys[capsuleKeyCount]
        #   [validation items gathered]
        # update the scope of the search - if included in fullListFromDb
        thisScope: _controller_base.ControllerDataScopes = scope
        for fullListFromDb in fullListsFromDb:
          if len(fullListFromDb) == 1 and fullListFromDb[0] == capsuleKey:
            thisScope = _controller_base.ControllerDataScopes.STORED_ON_DB
        #   [scope updated]
        countOfCapsule = 0
        numberedCapsulesDict = {}
        capsulesByKeyFncName = _controller_utils.getControllerIterByKeyFncName()
        capsules = getattr(self, capsulesByKeyFncName)(capsuleKey = capsuleKey, scope = thisScope)
        for capsule in capsules:
          if capsule == None:
            # set dictionary of capsuleKey to None if no value is present 
            numberedCapsulesDict = None
          else:
            capsuleDict = capsule.toDict(omitIds = omitIds)
            numberedCapsulesDict[countOfCapsule] = capsuleDict
            countOfCapsule += 1
        if capsuleKey in capsulesDict: continue
        capsulesDict[capsuleKey] = numberedCapsulesDict
        # if 'transaction_type' in capsulesDict:
        #   print('capsulesDict[transaction_type]: ', capsulesDict['transaction_type'])
        capsulesDict[capsuleKey + "_delete"] = {}
      result = capsulesDict
    return result
  

  def fncToJson(self: T,
                scope: _controller_base.ControllerDataScopes = 
                      _controller_base.ControllerDataScopes.NEW_AND_DIRTY) -> json.decoder:
    objDict = fncToDict(self, scope = scope)
    return json.dumps(objDict, cls=UUIDEncoder)
  fncToDictDecorated = _controller_base.cleanAndCloseSession(
                    func = fncToDict)
  fncToJsonDecorated = _controller_base.cleanAndCloseSession(
                    func = fncToJson)
  setattr(controllerType, nameOfDictFnc, fncToDictDecorated)
  setattr(controllerType, nameOfJsonFnc, fncToJsonDecorated)


def __addFromDictFunctions(controllerType : type[T],
                           callingGlobals):
  nameOfFromDictFnc = _controller_utils.getControllerDataFromDictFncName()
  def fncFromDict(self: T, 
                  session: sqlalchemy_orm.Session,
                  controllerDict: dict[str, any],
                  persistentMustHaveId: bool = False) -> T:
    def ensureSomeControllerKeyInDict() -> None:
      for subControllerType in self._subControllerTypes:
        subControllerKey = subControllerType._key
        if subControllerKey in controllerDict:
          return 
      controllerDictKeys = list(controllerDict.keys())
      raise Exception("No controller key found in controllerDict.\n" + \
                      "Keys in controllerDict:\n" + \
                      "    " + "    \n".join(controllerDictKeys) + '\n'
                      "Controller keys:\n" + \
                      "    " + "    \n".join([cntr._key for cntr in self._subControllerTypes]))
    def ensureOnlyControllerKeyInDict() -> None:
      controllerKeys = [cntr._key for cntr in self._subControllerTypes]
      controllerDictKeys = list(controllerDict.keys())
      for key in controllerDict.keys():
        if not key in controllerDictKeys:
          raise Exception("Some key found in controllerDict not corresponding to a subController.\n" + \
                          f"Bad controllerDict key: {key}\n" + \
                          "Controller keys:\n" + \
                          "    " + "    \n".join(controllerKeys))
    def ensureAllKeysInSubControllerDict() -> None: # checks if all capsule & capsule_delete keys are 
                                                    # are present in the subController dict
      subControllerKeys = self._keys
      subControllerDeleteKeys = [key + '_delete' for key in subControllerKeys]
      for pos in range(0, len(subControllerKeys)):
        subControllerKey = subControllerKeys[pos]
        subControllerDeleteKey = subControllerDeleteKeys[pos]
        if not (subControllerKey in controllerDict and subControllerDeleteKey in controllerDict):
          raise Exception("Some required SubController key no found in subControllerDict.\n" + \
                          "" if subControllerKey in controllerDict else f"Missing subControllerKey: {subControllerKey}\n" + \
                          "" if subControllerDeleteKey in controllerDict else f"Missing subControllerDeleteKey: {subControllerDeleteKey}\n" + \
                          "controllerDict keys:\n" + \
                          "    " + "    \n".join(list(controllerDict.keys())))
    def ensureKeyInDict(key: str):
      if not key in controllerDict:
        self._raiseException(f"Missing key in dictionary provided to {nameOfFromDictFnc} of " + \
                            f"class {self.__name__}. \n" + \
                            f"Key missing: {key}. \n" + \
                            "Dictionary:\n" + str(controllerDict))
    output = controllerType(session = session)   
    #print('ControllerDict: \n', controllerDict) 
    if len(self._key) == 0:
      ensureOnlyControllerKeyInDict()
      ensureSomeControllerKeyInDict()
      for subControllerType in self._subControllerTypes:
        subControllerKey = subControllerType._key
        subControllerTypeName = subControllerType.__name__
        subControllerAttributeName = _controller_utils.getStartsWithLowerCase(subControllerTypeName)
        #FIXME: trows error here cause fifoData has no key
        if not subControllerKey in controllerDict:
          continue
        subControllerDict = controllerDict[subControllerKey]
        _ = getattr(subControllerType, nameOfFromDictFnc)(
                                  session = session,
                                  controllerDict = subControllerDict,
                                  persistentMustHaveId = persistentMustHaveId)
    else:
      if self._key in controllerDict:
        controllerDict = controllerDict[self._key]
      ensureAllKeysInSubControllerDict()
      relationshipEntitiesCatalog: typing.Dict[str, dict] = dict[str, dict]()
      for contentPos in range(0, len(self._content)):
        # keys of sub controller and content of sub controller come in same order
        contentType = self._content[contentPos]
        contentKey = self._keys[contentPos]
        if not contentKey in controllerDict: continue # do nothing if key not present
        contentDict = controllerDict[contentKey]
        for capsuleDict in contentDict.values():
          # Do not try to convert validation list entries into capsules
          if len(capsuleDict) == 1 and 'name' in capsuleDict: continue
          _ = contentType.fromDict(session = output.session,
                                   capsuleDict = capsuleDict,
                                   persistentMustHaveId = persistentMustHaveId,
                                   relationshipEntitiesCatalog = relationshipEntitiesCatalog)
          # if contentKey == 'transaction_type':
          #   ctDict = contentType.toDict()
          #   print('ctDict: \n', ctDict)
    return output              
                
              


  fncFromDictDecorated = _controller_base.cleanAndCloseSession(
                func = fncFromDict)
  fncFromDictClassFnc = classmethod(fncFromDictDecorated)
  setattr(controllerType, nameOfFromDictFnc, fncFromDictClassFnc)




def addDictFunctions(controllerTypeNames : typing.List[type[T]],
                     controllerKeyEnumType: typing.Type,
                     callingGlobals):
  for controllerTypeName in controllerTypeNames:
    controllerType = callingGlobals[controllerTypeName]
    __addToDictFunctions(controllerType = controllerType,
                         controllerKeyEnumType = controllerKeyEnumType,
                         callingGlobals= callingGlobals)
    __addFromDictFunctions(controllerType = controllerType,
                        callingGlobals= callingGlobals)