from __future__ import annotations

import sys, uuid, typing, datetime, textwrap, \
       decimal
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from db_controllers.entity_capsules import capsules, _capsule_utils
from db_controllers.xl import io_wkb 
from db_controllers import _controller_base, _controller_attr, controller_head, \
                            _controller_obj_setup, _controller_json, _controller_utils
from db_controllers.entity_capsules.specific_capsule_attr.transactions import fifo_lot as spec_fifo_lot
from db_controllers.entity_capsules.specific_capsule_attr.asset import asset as spec_asset

from src.process_data.utils import base_asset_group_lots
from src.process_data.utils.specific_time_series import asset_time_series, lot_time_series

GET_LOTS_FNC_NAME = 'getLots'
GET_LOTS_HOLDINGS = 'getLotsHoldingsDict'
GET_OPEN_LOTS_FNC_NAME = 'getOpenLots'
GET_OPEN_LOTS_IN_PERIOD_FNC_NAME = 'getOpenLotsInPeriod'
GET_OPEN_LOT_HOLDINGS_IN_PERIOD_FNC_NAME = 'getHoldingsOfOpenLotsInPeriod'
GET_OPEN_LOTS_VOLUME_FNC_NAME = 'getOpenLotsVolume'
GET_LAST_LOT_NUMBER_FNC_NAME = 'getLastLotNumber'
GET_DAILY_TRADED_QUANTITY_ACTIVITY_OF_ASSET_ON_PROJECT = 'getDailyTradedQuantityActivityOfAssetOnProjectFnc'
GET_DAILY_HOLDINGS_OF_ASSET_ON_PROJECT_FNC_NAME = 'getDailyHoldingsOfAssetOnProject'
GET_DAILY_HOLDINGS_OF_ASSET_ON_PROJECT_AT_DATE_FNC_NAME = 'getDailyHoldingsOfAssetOnProjectAtDate'
GET_PROJECT_ASSETS_FNC_NAME = 'getAssetsOfProject'
GET_PROJECT_BASE_ASSETS_FNC_NAME = 'getBaseAssetsOfProject'

HAS_LONG_LOTS_FNC_NAME = 'hasLongLots'
HAS_SHORT_LOTS_FNC_NAME = 'hasShortLots'
GET_MINIMUM_HOLDING_PERIOD_COMPLIANT_LOTS_FNC_NAME = 'getMinimumHoldingPeriodCompliantLots'

HAS_HOLDINGS_OF_ASSET_ON_PROJECT = 'hasHoldingsOfAssetOnProject'
GET_HOLDINGS_PERIOD_START_OF_ASSET_ON_PROJECT = 'hasHoldingPeriodStartOfAssetOnProject'
GET_HOLDINGS_PERIOD_END_OF_ASSET_ON_PROJECT = 'hasHoldingPeriodEndOfAssetOnProject'

GET_HOLDINGS_DATA_OF_ASSET_ON_PROJECT_FNC_NAME = 'getHoldingsDataOfAssetOnProject'

GET_EXTENDED_ASSET_GROUP_OF_BASE_ASSET_ON_PROJECT = 'getExtendedAssetGroupOfBaseAssetOnProject'
GET_ASSET_GROUP_OF_BASE_ASSET_ON_PROJECT = 'getAssetGroupOfBaseAssetOnProject'

GET_EXTENDED_ASSET_GROUP_HOLDINGS_DATA_ON_PROJECT = 'getExtendedAssetGroupHoldingsDataOnProject'


GET_LOT_DATA_OF_BASE_ASSET_ON_PROJECT = 'getLotDataOfBaseAssetGroupOnProject'
GET_EXTENDED_PRICING_DATA_OF_BASE_ASSET_ON_PROJECT = 'getExtendedPricingDataOfBaseAssetOnProject'





C = typing.TypeVar("C", bound = _controller_base.ControllerBase)
T = typing.TypeVar("T", bound = capsules._capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound = capsules._capsule_base.CapsuleBase)
V = typing.TypeVar("V", bound = capsules._capsule_base.CapsuleBase)


def addAttributes(fifoDataController: typing.Type[C]) -> None:
  def getLotsFnc(self: C,
                 project: capsules.ProjectCapsule,
                 asset: capsules.AssetCapsule) -> typing.List[capsules.FifoLotCapsule]:
    result: typing.List[capsules.FifoLotCapsule] = list[capsules.FifoLotCapsule]()
    filterConditions = {'project_id': project.id, 'asset_id': asset.id}
    for fifoLot in self.fifoLots(scope = _controller_base.ControllerDataScopes.ALL,
                                 filterConditions = filterConditions,
                                 sortedBy = 'number'):
      result.append(fifoLot)
    return result
  setattr(fifoDataController, GET_LOTS_FNC_NAME, getLotsFnc)

  def getOpenLotsFnc(self: C,
                     project: capsules.ProjectCapsule,
                     asset: capsules.AssetCapsule,
                     timestamp: datetime.datetime = None) -> typing.List[capsules.FifoLotCapsule]:
    result: typing.List[capsules.FifoLotCapsule] = list[capsules.FifoLotCapsule]()
    for fifoLot in getattr(self, GET_LOTS_FNC_NAME)(project = project,
                                                    asset = asset):
      if timestamp is None:
        if fifoLot.is_open:
          result.append(fifoLot)
      else:
        openingDate = fifoLot.opening_timestamp
        closingDate = fifoLot.closing_timestamp
        if openingDate > timestamp: continue 
        if closingDate is None: 
          result.append(fifoLot)
        else:
          if closingDate > timestamp:
            result.append(fifoLot)
    return result
  setattr(fifoDataController, GET_OPEN_LOTS_FNC_NAME, getOpenLotsFnc)

  def getOpenLotsInPeriodFnc(self: C,
                             project: capsules.ProjectCapsule,
                             asset: capsules.AssetCapsule,
                             startDate: datetime.date,
                             endDate: datetime.date) -> typing.List[capsules.FifoLotCapsule]:
    result: typing.List[capsules.FifoLotCapsule] = list[capsules.FifoLotCapsule]()
    listOfLots = getattr(self, GET_LOTS_FNC_NAME)(project = project,
                                                    asset = asset)
    for fifoLot in listOfLots:
      # Do not use 'IS_OPEN_IN_PERIOD' of fifoLot here!
      #   -> we focus on daily trading here and not on the tax 'holding period'
      #   -> tax 'holding period' does not include day of acquisition and sale
      if not fifoLot.closing_timestamp is None:
        if fifoLot.closing_timestamp.date() < startDate: continue
      if fifoLot.opening_timestamp.date() > endDate: continue
      result.append(fifoLot)
    return result
  setattr(fifoDataController, GET_OPEN_LOTS_IN_PERIOD_FNC_NAME, getOpenLotsInPeriodFnc)

  def getHoldingsInPerLotInPeriodFnc(self: C,
                          project: capsules.ProjectCapsule,
                          asset: capsules.AssetCapsule,
                          startDate: datetime.date,
                          endDate: datetime.date) -> typing.Dict[int, typing.Dict[str, float]]:
    result = dict[int, dict]()
    openLotsInPeriod = getattr(self, GET_OPEN_LOTS_IN_PERIOD_FNC_NAME)(
                              project = project,
                              asset = asset,
                              startDate = startDate,
                              endDate = endDate)
    for lot in openLotsInPeriod:
      holdings = getattr(lot, spec_fifo_lot.GET_HOLDINGS)(
                                startDate = startDate,
                                endDate = endDate)
      result[lot.number] = holdings
    return result
  setattr(fifoDataController, GET_OPEN_LOT_HOLDINGS_IN_PERIOD_FNC_NAME, getHoldingsInPerLotInPeriodFnc)

  def getOpenLotsVolumeFnc(self: C,
                           project: capsules.ProjectCapsule,
                           asset: capsules.AssetCapsule,
                           timestamp: datetime.datetime = None) -> float:
    lots = getattr(self, GET_OPEN_LOTS_FNC_NAME)(project = project,
                                                 asset = asset,
                                                 timestamp = timestamp)
    result: float = 0.0
    for lot in lots:
      result += lot.remaining_quantity
    return result
  setattr(fifoDataController, GET_OPEN_LOTS_VOLUME_FNC_NAME, getOpenLotsVolumeFnc)

  def getLastLotNumberFnc(self: C,
                          project: capsules.ProjectCapsule,
                          asset: capsules.AssetCapsule) -> float:
    lots = getattr(self, GET_LOTS_FNC_NAME)(project = project,
                                            asset = asset)
    lotsLen = len(lots)
    if lotsLen == 0: return -1
    return lots[lotsLen - 1].number
  setattr(fifoDataController, GET_LAST_LOT_NUMBER_FNC_NAME, getLastLotNumberFnc)

  def getDailyTradedQuantityActivityOfAssetOnProjectFnc(self: C,
                              asset: capsules.AssetCapsule,
                              project: capsules.ProjectCapsule,
                              scope: _controller_base.ControllerDataScopes = \
                                _controller_base.ControllerDataScopes.ALL
                              ) -> typing.Dict[datetime.date, typing.Tuple[decimal.Decimal, bool]]:
    result = dict[datetime.date, tuple[decimal.Decimal, bool]]()
    filterConditions = {'project_id': project.id, 'asset_id': asset.id}
    for tx in self.marketAndForwardTransactions(scope = scope,
                                                filterConditions = filterConditions):
      txDate = tx.trade_timestamp.date()
      txTradedQuantity = decimal.Decimal(tx.traded_quantity)
      activityEntry = result[txDate] if txDate in result \
                                     else (decimal.Decimal("0.0"), True)
      activityEntry = (activityEntry[0] + txTradedQuantity, activityEntry[1])
      result[txDate] = activityEntry
    return result  
  setattr(fifoDataController, GET_DAILY_TRADED_QUANTITY_ACTIVITY_OF_ASSET_ON_PROJECT, getDailyTradedQuantityActivityOfAssetOnProjectFnc)

  def getDailyHoldingsOfAssetOnProjectFnc(self: C,
                              asset: capsules.AssetCapsule,
                              project: capsules.ProjectCapsule,
                              scope: _controller_base.ControllerDataScopes = \
                                _controller_base.ControllerDataScopes.ALL
                              ) -> typing.Dict[datetime.date, typing.Tuple[decimal.Decimal, bool]]:
    result = dict[datetime.date, tuple[decimal.Decimal, bool]]()
    dailyTradedQuantity = getattr(self, GET_DAILY_TRADED_QUANTITY_ACTIVITY_OF_ASSET_ON_PROJECT)(
                                    asset = asset, 
                                    project = project,
                                    scope = scope)
    activityDates = list(dailyTradedQuantity.keys())
    if len(activityDates) == 0: return result
    firstActivityDate = min(activityDates)
    lastActivityDate = max(activityDates)
    currDate = firstActivityDate
    previousHoldings = decimal.Decimal("0.0")
    while currDate <= lastActivityDate:
      entry = dailyTradedQuantity[currDate] if currDate in dailyTradedQuantity \
                                            else (decimal.Decimal("0.0"), False)
      previousHoldings = entry[0] + previousHoldings
      entry = (previousHoldings, entry[1]) 
      result[currDate] = entry      
      currDate += datetime.timedelta(days = 1)
    return result  
  setattr(fifoDataController, GET_DAILY_HOLDINGS_OF_ASSET_ON_PROJECT_FNC_NAME, getDailyHoldingsOfAssetOnProjectFnc)

  def getDailyHoldingsOfAssetOnProjectAtDateFnc(self: C,
                                                project: capsules.ProjectCapsule,
                                                asset: capsules.AssetCapsule,
                                                date: datetime.date,
                                                scope: _controller_base.ControllerDataScopes = \
                                                  _controller_base.ControllerDataScopes.ALL) -> decimal.Decimal:
    dailyHoldings = getattr(self, GET_DAILY_HOLDINGS_OF_ASSET_ON_PROJECT_FNC_NAME)(
                                    asset = asset, 
                                    project = project,
                                    scope = scope)
    holdingDays = list(dailyHoldings.keys())
    if len(holdingDays) == 0: return decimal.Decimal("0.0")
    firstHoldingDay = min(holdingDays)
    lastHoldingDay = max(holdingDays)
    if date < firstHoldingDay: 
      return decimal.Decimal("0.0")
    elif date > lastHoldingDay:
      lastHolding = dailyHoldings[lastHoldingDay]
      return lastHolding[0]
    else:
      return dailyHoldings[date][0] 
  setattr(fifoDataController, GET_DAILY_HOLDINGS_OF_ASSET_ON_PROJECT_AT_DATE_FNC_NAME, getDailyHoldingsOfAssetOnProjectAtDateFnc)

  def hasLongLotsFnc(self: C,
                     project: capsules.ProjectCapsule,
                     asset: capsules.AssetCapsule,
                     timestamp: datetime.datetime = None) -> bool:
    lots = getattr(self, GET_OPEN_LOTS_FNC_NAME)(project = project,
                                                 asset = asset,
                                                 timestamp = timestamp)
    result: bool = None
    if len(lots) == 0: return False
    for lot in lots:
      if result is None: 
        result = lot.is_long
      else: 
        if result != lot.is_long:
          errMsgSuffix = ""
          for lot in lots:
                errMsgSuffix = errMsgSuffix + textwrap.indent(lot.sqlalchemyTable.__repr__(), prefix="    ")
          raise Exception("Irregularly specified FiFo lots: simultaneous long & short open lots.\nLots:\n" + errMsgSuffix)
    return result
  setattr(fifoDataController, HAS_LONG_LOTS_FNC_NAME, hasLongLotsFnc)

  def hasShortLotsFnc(self: C,
                      project: capsules.ProjectCapsule,
                      asset: capsules.AssetCapsule,
                      timestamp: datetime.datetime = None) -> bool:
    lots = getattr(self, GET_OPEN_LOTS_FNC_NAME)(project = project,
                                                 asset = asset,
                                                 timestamp = timestamp)
    if len(lots) == 0: return False
    return not getattr(self, HAS_LONG_LOTS_FNC_NAME)(project = project,
                                                     asset = asset,
                                                     timestamp = timestamp)
  setattr(fifoDataController, HAS_SHORT_LOTS_FNC_NAME, hasShortLotsFnc)
  
  def getMinimumHoldingPeriodCompliantLotsFnc(
                      self: C,
                      project: capsules.ProjectCapsule,
                      asset: capsules.AssetCapsule,
                      date: datetime.date,
                      numberOfDaysBeforeAndAfter: int = 45,
                      scope: _controller_base.ControllerDataScopes = \
                          _controller_base.ControllerDataScopes.ALL
                      ) -> typing.List[capsules.FifoLotCapsule]:
    result: typing.List[capsules.FifoLotCapsule] = list[capsules.FifoLotCapsule]()
    filterConditions = {'project_id': project.id, 'asset_id': asset.id}
    for fifoLot in self.fifoLots(scope = scope,
                                 filterConditions = filterConditions,
                                 sortedBy = 'number'):
      if getattr(fifoLot, spec_fifo_lot.HAS_COMPLIANT_MINIMUM_HOLDING_PERIOD_FNC_NAME)(
                                 date = date,
                                 numberOfDaysBeforeAndAfter = numberOfDaysBeforeAndAfter):
        result.append(fifoLot)
    return result
  setattr(fifoDataController, GET_MINIMUM_HOLDING_PERIOD_COMPLIANT_LOTS_FNC_NAME, getMinimumHoldingPeriodCompliantLotsFnc)   

  def getAssetsOfProjectFnc(self: C,
                   project: capsules.ProjectCapsule,
                   scope: _controller_base.ControllerDataScopes = \
                      _controller_base.ControllerDataScopes.ALL) -> typing.Dict[str, capsules.AssetCapsule]:
    result: typing.Dict[str, capsules.AssetCapsule] = dict[str, capsules.AssetCapsule]()
    for marketAndForwardTransaction in self.marketAndForwardTransactions(scope = scope,
                                                                         filterConditions = {'project_id': project.id}):
      asset = marketAndForwardTransaction.asset
      if not asset.name in result: result[asset.name] = asset
    return result
  setattr(fifoDataController, GET_PROJECT_ASSETS_FNC_NAME, getAssetsOfProjectFnc)

  def has_holdingsOfAssetOnProjectFnc(self: C,
                                     asset: capsules.AssetCapsule,
                                     project: capsules.ProjectCapsule,
                                     scope: _controller_base.ControllerDataScopes = \
                                        _controller_base.ControllerDataScopes.ALL) -> bool:
    filterConditions = {'project_id': project.id, 'asset_id': asset.id}
    for tx in self.marketAndForwardTransactions(scope = scope,
                                                filterConditions = filterConditions):
      return True
    return False
  setattr(fifoDataController, HAS_HOLDINGS_OF_ASSET_ON_PROJECT, has_holdingsOfAssetOnProjectFnc)

  def getHoldingsStartDateOfAssetOnProjectFnc(self: C,
                                       asset: capsules.AssetCapsule,
                                       project: capsules.ProjectCapsule,
                                       scope: _controller_base.ControllerDataScopes = \
                                          _controller_base.ControllerDataScopes.ALL) -> datetime.datetime:
    if not getattr(self, HAS_HOLDINGS_OF_ASSET_ON_PROJECT)(project = project, 
                                                         asset = asset,
                                                         scope = scope): return None
    filterConditions = {'project_id': project.id, 'asset_id': asset.id}
    for tx in self.marketAndForwardTransactions(scope = scope,
                                                filterConditions = filterConditions):
      return tx.trade_timestamp.date()
  setattr(fifoDataController, GET_HOLDINGS_PERIOD_START_OF_ASSET_ON_PROJECT, getHoldingsStartDateOfAssetOnProjectFnc)

  def getHoldingsEndDateOfAssetOnProjectFnc(self: C,
                                       asset: capsules.AssetCapsule,
                                       project: capsules.ProjectCapsule,
                                       scope: _controller_base.ControllerDataScopes = \
                                          _controller_base.ControllerDataScopes.ALL) -> datetime.datetime:
    if not getattr(self, HAS_HOLDINGS_OF_ASSET_ON_PROJECT)(project = project, 
                                                         asset = asset,
                                                         scope = scope): return None
    filterConditions = {'project_id': project.id, 'asset_id': asset.id}
    result: datetime.date = None
    for tx in self.marketAndForwardTransactions(scope = scope,
                                                filterConditions = filterConditions):
      settlementDate = tx.settlement_timestamp.date()
      if result is None: result = settlementDate
      else: 
        if result < settlementDate: result = settlementDate
    return result
  setattr(fifoDataController, GET_HOLDINGS_PERIOD_END_OF_ASSET_ON_PROJECT, getHoldingsEndDateOfAssetOnProjectFnc)

  def getHoldingsDataOfAssetOnProjectFnc(self: C,
                                         asset: capsules.AssetCapsule,
                                         project: capsules.ProjectCapsule,
                                         startDate: datetime.date,
                                         endDate: datetime.date,
                                         baseAsset: capsules.AssetCapsule = None,
                                         dividendCapsule: V = None,
                                         limitToHoldingPeriodAnalysis: bool = False,
                                         extendedBaseAssetGroupPricingData: dict[str, asset_time_series.AssetTimeSeries] = {},
                                         time: datetime.time = datetime.datetime.max.time(),
                                         scope: _controller_base.ControllerDataScopes = \
                                            _controller_base.ControllerDataScopes.ALL) -> asset_time_series.AssetTimeSeries:
    def addHoldingsData() -> None:
      holdings: decimal.Decimal = decimal.Decimal("0.0")
      filterConditions = {'project_id': project.id, 'asset_id': asset.id}
      initFalseSeries = dict.fromkeys(mainResult.dates, False)
      mainResult._holdingsTimeSeries.fillSeries(label = asset_time_series.LABEL_HAS_HOLDINGS, dataDict = initFalseSeries)
      mainResult._holdingsTimeSeries.fillSeries(label = asset_time_series.LABEL_HAS_TRADE, dataDict = initFalseSeries)
      dailyHoldings = getattr(self, GET_DAILY_HOLDINGS_OF_ASSET_ON_PROJECT_FNC_NAME)(
                                    asset = asset, 
                                    project = project,
                                    scope = scope)
      if len(dailyHoldings) == 0:
        dailyHoldings = {key: (decimal.Decimal("0.0"), False) for key in (startDate + datetime.timedelta(days=n) \
                                                              for n in range((endDate - startDate).days + 1))}
      
      holdingDays = list(dailyHoldings.keys())
      firstHoldingDay = min(holdingDays)
      lastHoldingDay = max(holdingDays)

      currDate = startDate
      while currDate <= endDate:
        if currDate < firstHoldingDay: 
          holdingsOfDay = decimal.Decimal("0.0")
          hasTrade = False
        elif currDate > lastHoldingDay:
          lastHolding = dailyHoldings[lastHoldingDay]
          holdingsOfDay = lastHolding[0]
          hasTrade = False
        else:
          holdingInfoOfDay = dailyHoldings[currDate]
          holdingsOfDay = holdingInfoOfDay[0] 
          hasTrade = holdingInfoOfDay[1] 

        mainResult.total_holdings_set_on_date(date = currDate,
                                              value = holdingsOfDay)
        mainResult.has_trade_set_on_date(date = currDate,
                                         value = hasTrade)
        mainResult.has_holdings_set_on_date(date = currDate,
                                            value = round(holdingsOfDay, 15) != decimal.Decimal("0.0"))

        currDate += datetime.timedelta(days = 1)
    mainResult = getattr(asset, spec_asset.GET_PRICING_DATA_IN_PERIOD_FNC_NAME)(
                                                         startDate = startDate,
                                                         endDate = endDate,
                                                         baseAsset = baseAsset,
                                                         dividendCapsule = dividendCapsule,
                                                         limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
                                                         assetGroupAndPathPricingData = extendedBaseAssetGroupPricingData,
                                                         time = time)
    if mainResult is None: return None
    addHoldingsData()    
    return mainResult
  setattr(fifoDataController, GET_HOLDINGS_DATA_OF_ASSET_ON_PROJECT_FNC_NAME, getHoldingsDataOfAssetOnProjectFnc)

  def getBaseAssetsOnProjectFnc(self: C,
                                baseAssets: typing.Dict[str, capsules.AssetCapsule],
                                project: capsules.ProjectCapsule,
                                startDate: datetime.date,
                                endDate: datetime.date,
                                scope: _controller_base.ControllerDataScopes = \
                                  _controller_base.ControllerDataScopes.ALL) -> typing.Dict[str, capsules.AssetCapsule]:
    def someAssetInBaseAssetGroup(baseAsset: capsules.AssetCapsule,
                                  assetsOfProject: typing.Dict[str, capsules.AssetCapsule]) -> bool:
      for assetOfProject in assetsOfProject.values():
        if getattr(assetOfProject, spec_asset.IS_PART_OF_ISOMORPHIC_GROUP_IN_PERIOD_FNC_NAME)(
                                    baseAsset = baseAsset,
                                    startDate = startDate,
                                    endDate = endDate): return True
      return False
    assetsOfProject = getattr(self, GET_PROJECT_ASSETS_FNC_NAME)(project = project,
                                                                 scope = scope)
    result = dict[str, capsules.AssetCapsule]()
    for baseAsset in baseAssets.values():
      if someAssetInBaseAssetGroup(baseAsset = baseAsset,
                                   assetsOfProject = assetsOfProject):
        result[baseAsset.name] = baseAsset
    return result
  setattr(fifoDataController, GET_PROJECT_BASE_ASSETS_FNC_NAME, getBaseAssetsOnProjectFnc)

  def getExtendedAssetGroupOfBaseAssetOnProjectFnc(self: C,
                                project: capsules.ProjectCapsule,
                                baseAsset: capsules.AssetCapsule,
                                startDate: datetime.date,
                                endDate: datetime.date,
                                scope: _controller_base.ControllerDataScopes = \
                                  _controller_base.ControllerDataScopes.ALL) -> typing.Dict[str, 
                                                                                            typing.Tuple[capsules.AssetCapsule, bool]]:
    #FIXME: 
    result: typing.Dict[str, capsules.AssetCapsule] = dict[str, capsules.AssetCapsule]()
    projectAssets = getattr(self, GET_PROJECT_ASSETS_FNC_NAME)(project = project, 
                                                               scope = scope)
    # print(f"  projectAssetsNames: {list(projectAssets.keys())}")
    for projectAssetName, projectAsset in projectAssets.items():
      assetsOnPathToBaseAsset = getattr(projectAsset, spec_asset.GET_ASSETS_ON_PATH_TO_BASE_ASSET)(baseAsset = baseAsset,
                                                                                          startDate = startDate,
                                                                                          endDate = endDate)
      for assetOnPathName, assetOnPath in assetsOnPathToBaseAsset.items():
        if assetOnPathName in result: continue
        assetInProject = assetOnPathName in projectAssets
        entry = (assetOnPath, assetInProject) 
        result[assetOnPathName] = entry
    return result
  setattr(fifoDataController, GET_EXTENDED_ASSET_GROUP_OF_BASE_ASSET_ON_PROJECT, getExtendedAssetGroupOfBaseAssetOnProjectFnc)

  def getAssetGroupOfBaseAssetOnProjectFnc(self: C,
                                project: capsules.ProjectCapsule,
                                baseAsset: capsules.AssetCapsule,
                                startDate: datetime.date,
                                endDate: datetime.date,
                                scope: _controller_base.ControllerDataScopes = \
                                  _controller_base.ControllerDataScopes.ALL) -> typing.Dict[str, capsules.AssetCapsule]:
    result: typing.Dict[str, capsules.AssetCapsule] = dict[str, capsules.AssetCapsule]()
    extendedAssetGroup = getattr(self, GET_EXTENDED_ASSET_GROUP_OF_BASE_ASSET_ON_PROJECT)(baseAsset = baseAsset,
                                                                                          project = project,
                                                                                          startDate = startDate,
                                                                                          endDate = endDate)
    for assetName, entry in extendedAssetGroup.items():
      if entry[1]: result[assetName] = entry[0]
    return result
  setattr(fifoDataController, GET_ASSET_GROUP_OF_BASE_ASSET_ON_PROJECT, getAssetGroupOfBaseAssetOnProjectFnc)

  def getLotDataOfBaseAssetOnProjectFnc(self: C,
                                project: capsules.ProjectCapsule,
                                baseAsset: capsules.AssetCapsule,
                                startDate: datetime.date,
                                endDate: datetime.date,
                                extendedBaseAssetGroupPricingData: typing.Dict[str, asset_time_series.AssetTimeSeries] = None,
                                dividendCapsule: V = None,
                                limitToHoldingPeriodAnalysis: bool = False,
                                time: datetime.time = datetime.datetime.max.time(),
                                scope: _controller_base.ControllerDataScopes = \
                                  _controller_base.ControllerDataScopes.ALL
                                ) -> base_asset_group_lots.BaseAssetLots:
    assetsOfBaseAsset =  getattr(self, GET_ASSET_GROUP_OF_BASE_ASSET_ON_PROJECT)(project = project,
                                                                                baseAsset = baseAsset,
                                                                                startDate = startDate,
                                                                                endDate = endDate,
                                                                                scope = scope)
    if extendedBaseAssetGroupPricingData is None:
      extendedBaseAssetGroupPricingData = getattr(self, GET_EXTENDED_PRICING_DATA_OF_BASE_ASSET_ON_PROJECT)(project = project,
                                                                                  baseAsset = baseAsset,
                                                                                  startDate = startDate,
                                                                                  endDate = endDate,
                                                                                  dividendCapsule = dividendCapsule,
                                                                                  limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
                                                                                  time = time,
                                                                                  scope = scope)
    dividendExDate = dividendCapsule.ex_date if dividendCapsule is not None else None
    resultFeatures = base_asset_group_lots.BaseAssetLotsFeatures(startDate = startDate,
                                                                 endDate = endDate,
                                                                 dividendExDate = dividendExDate,
                                                                 limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis)
    result = base_asset_group_lots.BaseAssetLots(baseAsset = baseAsset,
                                                 extendedBaseAssetGroupPricingData = extendedBaseAssetGroupPricingData,
                                                 baseAssetLotsFeatures = resultFeatures)
    if len(assetsOfBaseAsset) == 0: return result

    for assetOfBaseAsset in assetsOfBaseAsset.values():
      assetHoldingData = getattr(self, GET_HOLDINGS_DATA_OF_ASSET_ON_PROJECT_FNC_NAME)(project = project,
                                                                              asset = assetOfBaseAsset,
                                                                              startDate = startDate,
                                                                              endDate = endDate,
                                                                              baseAsset = baseAsset,
                                                                              dividendCapsule = dividendCapsule,
                                                                              limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
                                                                              extendedBaseAssetGroupPricingData = extendedBaseAssetGroupPricingData,
                                                                              scope = scope,
                                                                              time = time)
      assetOpenLots = getattr(self, GET_OPEN_LOTS_IN_PERIOD_FNC_NAME)(project = project,
                                                                  asset = assetOfBaseAsset,
                                                                  startDate = startDate,
                                                                  endDate = endDate)
      for assetLot in assetOpenLots:
        lotHoldingsData = getattr(assetLot, spec_fifo_lot.GET_LOT_HOLDINGS_DATA)(
                       assetHoldingData = assetHoldingData)
        result.addLotData(lot = assetLot, 
                          lotData = lotHoldingsData)
    return result
  setattr(fifoDataController, GET_LOT_DATA_OF_BASE_ASSET_ON_PROJECT, getLotDataOfBaseAssetOnProjectFnc)

  def getExtendedAssetPricingDataOfBaseAssetOnProjectFnc(self: C,
                                project: capsules.ProjectCapsule,
                                baseAsset: capsules.AssetCapsule,
                                startDate: datetime.date,
                                endDate: datetime.date,
                                dividendCapsule: V = None,
                                limitToHoldingPeriodAnalysis: bool = False,
                                time: datetime.time = datetime.datetime.max.time(),
                                scope: _controller_base.ControllerDataScopes = \
                                  _controller_base.ControllerDataScopes.ALL
                                ) -> typing.Dict[str, asset_time_series.AssetTimeSeries]:
    extendedAssetsOfBaseAsset =  getattr(self, GET_EXTENDED_ASSET_GROUP_OF_BASE_ASSET_ON_PROJECT)(project = project,
                                                                                baseAsset = baseAsset,
                                                                                startDate = startDate,
                                                                                endDate = endDate,
                                                                                scope = scope)
    result = dict[str, asset_time_series.AssetTimeSeries]()
    if len(extendedAssetsOfBaseAsset) == 0: return result
    for assetOfBaseAssetInfo in extendedAssetsOfBaseAsset.values():
      assetOfBaseAsset = assetOfBaseAssetInfo[0]
      assetPricingData = getattr(self, GET_HOLDINGS_DATA_OF_ASSET_ON_PROJECT_FNC_NAME)(project = project,
                                                                              asset = assetOfBaseAsset,
                                                                              baseAsset = baseAsset,
                                                                              dividendCapsule = dividendCapsule,
                                                                              extendedBaseAssetGroupPricingData = result,
                                                                              limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
                                                                              startDate = startDate,
                                                                              endDate = endDate,
                                                                              scope = scope,
                                                                              time = time)
      result[assetOfBaseAsset.name] = assetPricingData
    return result
  setattr(fifoDataController, GET_EXTENDED_PRICING_DATA_OF_BASE_ASSET_ON_PROJECT, getExtendedAssetPricingDataOfBaseAssetOnProjectFnc)





