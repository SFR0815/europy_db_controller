from __future__ import annotations

import sys, uuid, typing, datetime
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from db_controllers.entity_capsules import _capsule_base, _capsule_utils 
from db_controllers.xl import io_wkb 
from db_controllers import _controller_base, _controller_attr, controller_head, \
                            _controller_obj_setup, _controller_json, _controller_utils

######################################################################################
######################################################################################
# ATTENTION: do not forget to update _controller_base.ControllerKeyEnum
#            when adding sub-controllers to the main controller 
######################################################################################
######################################################################################


C = typing.TypeVar("C", bound=_controller_base.ControllerBase)
CT = typing.TypeVar("CT", bound=_capsule_base.CapsuleBase)
CONTR_ENUM_STR = typing.TypeVar("CONTR_ENUM_STR", str, _controller_base.ControllerKeyEnum)

_controllerTypeNames = ['BasicSpecification', 
                       'AssetClassification', 
                       'AssetPricing', 
                       'Controller',
                       'ClientAdmin',
                       'ProjectInput',
                       'FifoData']

# class BasicSpecification(_controller_base.ControllerBase):
#   _content = [capsules.TransactionTypeCapsule,
#               capsules.CurrencyCapsule,
#               capsules.CountryCapsule, 
#               capsules.AssetClassCapsule,
#               capsules.GermanStockExchangeCapsule,
#               capsules.CoreAccountCapsule]
#   _key = _controller_base.BASIC_SPECIFICATION_KEY
#   # FIXME add currency here
#   def __init__(self,
#                session: sqlalchemy_orm.Session) -> None:
#     super(BasicSpecification, self).__init__(session=session)

# class AssetClassification(_controller_base.ControllerBase):
#   _content = [capsules.AssetCapsule]
#   _key = _controller_base.ASSET_CLASSIFICATION_KEY
#   def __init__(self,
#                session: sqlalchemy_orm.Session) -> None:
#     super(AssetClassification, self).__init__(session=session)

# class AssetPricing(_controller_base.ControllerBase):
#   _content = [capsules.AssetPriceCapsule,
#               capsules.CurrencyConversionRateCapsule]
#   _key = _controller_base.ASSET_PRICING_KEY
#   def __init__(self,
#                session: sqlalchemy_orm.Session) -> None:
#     super(AssetPricing, self).__init__(session=session)

# class ClientAdmin(_controller_base.ControllerBase):
#   _content = [capsules.ClientCapsule,
#               capsules.ProjectCapsule]
#   _key = _controller_base.CLIENT_ADMIN_KEY
#   def __init__(self,
#                session: sqlalchemy_orm.Session) -> None:
#     super(ClientAdmin, self).__init__(session=session)

# class ProjectInput(_controller_base.ControllerBase):
#   _content = [capsules.MarketTransactionCapsule, 
#               capsules.MicroHedgeCapsule,
#               capsules.DividendCapsule]
#   _key = _controller_base.PROJECT_INPUT_KEY
#   def __init__(self,
#                session: sqlalchemy_orm.Session) -> None:
#     super(ProjectInput, self).__init__(session=session)

# class FifoData(_controller_base.ControllerBase):
#   _content = [capsules.FifoLotCapsule,
#               capsules.FifoTransactionCapsule,
#               capsules.MarketAndForwardTransactionCapsule]
#   _key = _controller_base.FIFO_DATA_KEY
#   def __init__(self,
#                session: sqlalchemy_orm.Session) -> None:
#     super(FifoData, self).__init__(session=session)

class Controller(_controller_base.ControllerBase):
  # no content allowed on main controller
  #   all information provided by sub-controllers
  _subControllerTypes = []
  _headSubControllerType =  controller_head.ControllerHead
  _content = []
  _key = ""

  def __init__(self,
               session: sqlalchemy_orm.Session) -> None:
    super(Controller, self).__init__(session=session)
    self.head = controller_head.ControllerHead()
    self.dict = {}
    # for _subControllerType in self._subControllerTypes:
    #   setattr(self, _controller_utils.getStartsWithLowerCase(_subControllerType.__name__),
    #           _subControllerType(session = session))
  
  def __getSubControllerDefsOfKey(self,  subControllerKey: CONTR_ENUM_STR) -> typing.Tuple[str, C]:    
    if isinstance(subControllerKey, _controller_base.ControllerKeyEnum):
      subControllerKey = subControllerKey.value
    for subControllerDefs in vars(self).items():
      if isinstance(subControllerDefs[1], _controller_base.ControllerBase):
        if subControllerDefs[1]._key == subControllerKey:
          return subControllerDefs    
  def getSubControllerOfKey(self,  subControllerKey: CONTR_ENUM_STR) -> C:
    return self.__getSubControllerDefsOfKey(subControllerKey=subControllerKey)[1]
  def getSubControllerAttrAndClassOfKey(self,  subControllerKey: CONTR_ENUM_STR) -> typing.Type[C]:
    subControllerDefs = self.__getSubControllerDefsOfKey(subControllerKey = subControllerKey)   
    return (subControllerDefs[0], subControllerDefs[1].__class__)

  @classmethod
  def getControllerRegistry(self) -> typing.List[tuple[str, type[CT]]]:
    result: typing.List[tuple[str, type[CT]]] = list[(str, type[CT])]()
    for subControllerType in self._subControllerTypes:
      for contentType in subControllerType._content:
        result.append((subControllerType._key, contentType))
    return result
  @classmethod
  def getValidationItemLocators(self,
                                validationItems: typing.List[type[CT]],
                                ) -> typing.List[tuple[str, str]]:
    result: typing.List[tuple[str, str]] = list[tuple[str, str]]()
    controllerRegistry = self.getControllerRegistry()
    for validationItem in validationItems:
      for controllerRegistryItem in controllerRegistry:
        controllerKey, capsuleType = controllerRegistryItem
        if capsuleType == validationItem:
          capsuleKey = capsuleType._key()
          locatorTuple = (controllerKey, capsuleKey)
          result.append(locatorTuple)
    return result
  @classmethod
  def getValidationItemLocatorsOfSubController(self,
                                               subControllerKey: _controller_base.ControllerKeyEnum
                                               ) -> typing.List[tuple[str, str]]:
    for subControllerType in self._subControllerTypes:
      thisSubControllerKey = subControllerType._key
      if subControllerKey.value == thisSubControllerKey:
        validationItemsFncName = _capsule_utils.getValidationItemsFncName()
        validationItems = getattr(subControllerType, validationItemsFncName)()
        return self.getValidationItemLocators(validationItems = validationItems)

 
  @property
  def isLoaded(self):
    return self.head.isLoaded
  @property
  def isAmbiguous(self):
    return self.head.isAmbiguous 
  
  def _executeXl(self):
    xl = self.head.excel
    xl.ensureConsistency()
    validationLocators = self.__class__.getValidationItemLocatorsOfSubController(
                    subControllerKey=xl.subControllerKey)
    subController = self.getSubControllerOfKey(
                    subControllerKey = xl.subControllerKey)
    xl.ioWkb = io_wkb.IoWorkbook(subControllerKey = xl.subControllerKey,
                                 capsuleTypes = subController._content,
                                 validationLocators = validationLocators)    
    if xl.action == _controller_base.XlControllerAction.DOWNLOAD:
      controllerDict = self._controllerDataToDict(scope = xl.scope,
                                                  subControllerSelected = xl.subControllerKey)
      # print(f"controller - _executeXl - controllerDict:\n{controllerDict}")
      xl.ioWkb.setupDownload(controllerDict = controllerDict,
                             path = xl.downloadFilePath)
    elif xl.action == _controller_base.XlControllerAction.UPLOAD:
      xl.ioWkb.loadAndIdentifyUploadWkb(path = xl.uploadFilePath)
      nameOfFromDictFnc = _controller_utils.getControllerDataFromDictFncName()
      subControllerAttributeName, subControllerClass = self.getSubControllerAttrAndClassOfKey(
                subControllerKey = self.head.excel.subControllerKey)
      subControllerDict = xl.ioWkb.toDict()[subControllerClass._key]
      newSubController = getattr(subControllerClass, nameOfFromDictFnc)(session = self.session,
                                                                        controllerDict = subControllerDict,
                                                                        persistentMustHaveId = True)
      setattr(self, subControllerAttributeName, newSubController) 
  def execute(self):
    if self.isAmbiguous:
      raise Exception("Can't execute controller.\n" + \
                      "Both GUI and EXCEL heads require execution.")
    self.dict = {}
    self.dict['head'] = self.head.toDict()  
    if self.head.excel.isLoaded:
      self._executeXl()
 

  def setSubControllerTypes(
              self,
              subControllerTypes: list[_controller_base.ControllerBase],
              session: sqlalchemy_orm.Session) -> None:
    for _subControllerType in subControllerTypes:
      setattr(self, _controller_utils.getStartsWithLowerCase(_subControllerType.__name__),
              _subControllerType(session = session))

def get_basic_controller(session: sqlalchemy_orm.Session) -> typing.Type[C]:
  return Controller(session = session)

def getControllerClass(
            subControllerTypes: list[_controller_base.ControllerBase],
            session: sqlalchemy_orm.Session,
            callingGlobals: typing.Dict[str, any]
            ) ->typing.Type[C]:
  controller = Controller(session = session)
  controller.setSubControllerTypes(subControllerTypes = subControllerTypes)
  _controller_attr.addAttributes(controllerTypeNames = _controllerTypeNames,
                                  callingGlobals = callingGlobals)

  _controller_obj_setup.addSetupMethods(controllerTypeNames = _controllerTypeNames,
                                        callingGlobals = callingGlobals)

  _controller_json.addDictFunctions(controllerTypeNames = _controllerTypeNames,
                                    callingGlobals = callingGlobals)
  return controller



# project_input.addAttributes(projectInputSubcontroller = ProjectInput)
# fifo_data.addAttributes(fifoDataController = FifoData)
# asset_classification.addAttributes(assetClassificationController = AssetClassification)