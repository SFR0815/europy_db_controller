from __future__ import annotations

import sys, uuid, typing, datetime, \
       decimal, os

from dateutil import relativedelta as dateutil_rd

from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from src.utils import date_utils
from europy_db_controllers import _controller_base 
from europy_db_controllers.entity_capsules import _capsule_base 
from europy_db_controllers.entity_capsules.specific_capsule_attr.utils import state_control
from europy_db_controllers.entity_capsules.specific_capsule_attr.basic_specification import currency as spec_currency
from europy_db_controllers.entity_capsules.specific_capsule_attr.utils import dict_init as dt_init

from src.process_data.utils import time_series
from src.process_data.utils.specific_time_series import asset_iso_content_data_time_series, asset_time_series, \
                                                          time_series_to_excel

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)
V = typing.TypeVar("V", bound=_capsule_base.CapsuleBase)

IS_ISOMORPHIC_BASE_ASSET_PROP_NAME = 'is_isomorphic_base_asset'
GET_UNDERLYING_STATE_VALIDITY_PROP_NAME = 'underlying_states_validity'

GET_UNDERLYING_STATE_OF_DATE_FNC_NAME = 'getUnderlyingStateOfDate'
IS_PART_OF_ISOMORPHIC_GROUP_ON_DATE_FNC_NAME = 'isPartOfIsomorphicGroupOnDate'
GET_ASSETS_ON_PATH_TO_BASE_ASSET = 'getAssetsOnPathToBaseAsset'
GET_ASSETS_OF_BASE_ASSET_GROUP = 'getAssetsOFBaseAssetGroup'
IS_PART_OF_ISOMORPHIC_GROUP_SERIES_IN_PERIOD_FNC_NAME = 'isPartOfIsomorphicGroupSeriesInPeriod'
IS_PART_OF_ISOMORPHIC_GROUP_IN_PERIOD_FNC_NAME = 'isPartOfIsomorphicGroupInPeriod'

GET_ISOMORPHIC_ASSET_CONTENT_ON_DATE_FNC_NAME = 'getIsomorphicAssetContentOnDate'
GET_CONTAINED_SHARES_OF_ISOMORPHIC_ASSET_IN_PERIOD_FNC_NAME = 'getContainedSharesOfIsomorphicAssetInPeriod'
GET_ISOMORPHIC_ASSET_CONTENT_IN_PERIOD_FNC_NAME = 'getIsomorphicAssetContentInPeriod'

GET_CONSECUTIVE_PRODUCT_OF_DENOMINATION_IN_PERIOD_FNC_NAME = 'getConsecutiveProductOfDenominationInPeriod'

GET_PRICE_CAPSULE_ON_DATE_FNC_NAME = 'getPriceCapsuleOnDate'
GET_PRICE_CAPSULE_IN_PERIOD_FNC_NAME = 'getPriceCapsuleInPeriod'

GET_PRICING_DATA_IN_PERIOD_BASIC_FNC_NAME = 'getPricingDataInPeriodBasic'
GET_PRICING_DATA_IN_PERIOD_FNC_NAME = 'getPricingDataInPeriod'

GET_BASIC_PRICING_DATA_IN_PERIOD_OF_ASSETS_ON_PATH_OF_BASE_ASSET_FNC_NAME = 'getBasicPricingDataOfAssetsOnPathOfBaseAsset'

GET_STATIC_ASSET_DATA_FNC_NAME = "getStaticAssetData"
GET_LAST_AVAILABLE_PRICE_DATA_FNC_NAME = "getLastAvailablePriceData"
GET_AVAILABLE_PRICE_DATA_FNC_NAME = "getAvailablePriceData"
GET_DIVIDEND_DATA_FNC_NAME = "getDividendData"
GET_BASIC_PRICING_DATA_IN_PERIOD_OF_BASE_ASSET_GROUP_FNC_NAME = 'getBasicPricingDataOfBaseAssetGroup'
GET_PRICING_DATA_OF_BASE_ASSETS_GROUP_FNC_NAME = 'getPricingDataOfBaseAssetGroup'
IS_ACTIVE_IN_PERIOD_FNC_NAME = 'isActiveInPeriod'
IS_ACTIVE_ON_DATE_FNC_NAME = 'isActiveOnDate'



def addAttributes(assetCapsule: typing.Type[T]) -> None:
  def __ensureIsBaseAssetOfIsomorphicGroup(baseAsset: U,
                              fncName: str) -> None:
    if not getattr(baseAsset, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME):
      raise Exception("Base asset must be the base asset of an isomorphic asset group.\n" +
                      f"Base asset provided to {fncName}: {baseAsset.name}")
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def isIsomorphicBaseAssetFnc(self: T) -> bool:
    return len(self.sqlalchemyTable.underlying_states) == 0
  setattr(assetCapsule, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME, 
              property(_capsule_base.cleanAndCloseSession(isIsomorphicBaseAssetFnc)))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getUnderlyingStateValiditiesFnc(self) -> typing.List[typing.Tuple[datetime.date,
                                                                  datetime.date,
                                                                  any]]:
      beginningOfTime = date_utils.beginningOfTime()
      endOfTime = date_utils.endOfTime()
      underlyingStatesCount = 0
      underlyingStates = list(self.underlying_states)
      result = []
      if len(underlyingStates) == 0:
        result.append((beginningOfTime, endOfTime, None))
      for underlyingStatesCount in range(0, len(underlyingStates)):
        underlyingState = underlyingStates[underlyingStatesCount]
        validFrom = underlyingState.timestamp.date()
        # ASSUMPTION: underlying timestamp is always START of day
        if underlyingStatesCount < len(underlyingStates) - 1: 
          validTo = underlyingStates[underlyingStatesCount + 1].timestamp.date()
          # ASSUMPTION: underlying timestamp is always START of day
          validTo = validTo - datetime.timedelta(days = 1)
        else: 
          if self.expiration_date is None:
            validTo = endOfTime
          else:
            validTo = self.expiration_date.date()
            # ASSUMPTION: expiration_date is always END of day
        resultItem = (validFrom, validTo, underlyingStates[underlyingStatesCount])
        result.append(resultItem)
      return result
  setattr(assetCapsule, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME, 
            property(_capsule_base.cleanAndCloseSession(getUnderlyingStateValiditiesFnc)))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getUnderlyingStateOfDateFnc(self,
                                  date: datetime.date
                                  ) -> any:
      underlyingStates = list(self.underlying_states)
      if len(underlyingStates) == 0: return None
      expirationDate = self.expiration_date
      if not expirationDate is None:
        if date > expirationDate.date(): return None
      for underlyingStatesCount in range(0, len(underlyingStates)):
        underlyingState = underlyingStates[underlyingStatesCount]
        if underlyingStatesCount == 0:
          setupDate = underlyingState.timestamp.date()
          if date < setupDate: return None
        if underlyingStatesCount == (len(underlyingStates) - 1):
          return underlyingState
        elif underlyingStatesCount < (len(underlyingStates) - 1):
          nextUnderlying = underlyingStates[underlyingStatesCount + 1]
          nextUnderlyingDate = nextUnderlying.timestamp.date()
          if date < nextUnderlyingDate: return underlyingState
  setattr(assetCapsule, GET_UNDERLYING_STATE_OF_DATE_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getUnderlyingStateOfDateFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def isPartOfIsomorphicGroupOnDateFnc(self: T,
                              baseAsset: U,
                              date: datetime.date) -> bool:
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset = baseAsset,
                            fncName = IS_PART_OF_ISOMORPHIC_GROUP_ON_DATE_FNC_NAME)
    assetsOnPathToBaseAsset = getattr(self, GET_ASSETS_ON_PATH_TO_BASE_ASSET)(
                                                baseAsset = baseAsset,
                                                startDate = date,
                                                endDate = date)
    return len(assetsOnPathToBaseAsset) > 0
  setattr(assetCapsule, IS_PART_OF_ISOMORPHIC_GROUP_ON_DATE_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(isPartOfIsomorphicGroupOnDateFnc))    
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def isPartOfIsomorphicGroupSeriesInPeriodFnc(self: T,
                              baseAsset: U,
                              startDate: datetime.date,
                              endDate: datetime.date) -> typing.Dict[datetime.date, bool]:
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset = baseAsset,
                            fncName = IS_PART_OF_ISOMORPHIC_GROUP_SERIES_IN_PERIOD_FNC_NAME)
    # an base asset is part of it's isomorphic asset group
    if self.name == baseAsset.name:
      return dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = bool,
                                  value = True)
    # a base asset of another isomorphic asset group is not part of 
    #    another's base asset's isomorphic asset group
    if getattr(self, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME):
      return dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = bool,
                                  value = False)
    result = dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = bool,
                                  value = False)
    # -> there is at least one underlying state
    underlyingStateValidities = getattr(self, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME)
    for underlyingStateValidity in underlyingStateValidities:
      validFrom, validTo, underlyingState = underlyingStateValidity
      validFrom = max(validFrom, startDate)
      validTo = min(validTo, endDate)
      for underlying in underlyingState.underlyings:
        underlyingAsset = underlying.asset
        isPartOfIsomorphicGroupSeries = getattr(underlyingAsset, IS_PART_OF_ISOMORPHIC_GROUP_SERIES_IN_PERIOD_FNC_NAME)(
                                                baseAsset = baseAsset,
                                                startDate = validFrom,
                                                endDate = validTo)
        currDate = validFrom
        while currDate <= validTo:
          isPartOfIsomorphicGroupOnDate = isPartOfIsomorphicGroupSeries[currDate]
          if isPartOfIsomorphicGroupOnDate: result[currDate] = True
          currDate += datetime.timedelta(days=1) 
    return result
  setattr(assetCapsule, IS_PART_OF_ISOMORPHIC_GROUP_SERIES_IN_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(isPartOfIsomorphicGroupSeriesInPeriodFnc))   
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getAssetsOnPathToBaseAssetFnc(self: T,
                                    baseAsset: U,
                                    startDate: datetime.date,
                                    endDate: datetime.date) -> typing.Dict[str, V]:
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset = baseAsset,
                            fncName = GET_ASSETS_ON_PATH_TO_BASE_ASSET)
    result = {}
    if self.name == baseAsset.name: 
      result[baseAsset.name] = baseAsset
      return result
    if getattr(self, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME): 
      return result
    underlyingStateValidities = getattr(self, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME)
    for underlyingStateValidity in underlyingStateValidities:
      validFrom, validTo, underlyingState = underlyingStateValidity
      # continue if validity of underlyingState does not intersect with period between from and to date:
      if endDate < validFrom or startDate > validTo: continue
      validTo = min(validTo, endDate)
      validFrom = max(validFrom, startDate)
      for underlying in underlyingState.underlyings:
        underlyingAsset = underlying.asset
        assetsOnSubPathToBaseAsset = getattr(underlyingAsset, GET_ASSETS_ON_PATH_TO_BASE_ASSET)(
                                                baseAsset = baseAsset,
                                                startDate = validFrom,
                                                endDate = validTo)
        if baseAsset.name in assetsOnSubPathToBaseAsset:
          result[self.name] = self
          for subPathAssetName, assetOnSubPathToBaseAsset in assetsOnSubPathToBaseAsset.items():
            if subPathAssetName in result: continue
            result[subPathAssetName] = assetOnSubPathToBaseAsset
    return result      
  setattr(assetCapsule, GET_ASSETS_ON_PATH_TO_BASE_ASSET, 
              _capsule_base.cleanAndCloseSession(getAssetsOnPathToBaseAssetFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getAssetsOfBaseAssetGroupFnc(self: T,
                                  startDate: datetime.date,
                                  endDate: datetime.date) -> typing.Dict[str, V]:
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset = self,
                                      fncName = GET_ASSETS_ON_PATH_TO_BASE_ASSET)
    result = {}
    if not getattr(self, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME): return result
    allAssets = self.__class__.queryAll(session=self.session)
    for asset in allAssets:
      if asset.name == self.name:
        result.append(asset)
        continue
      if getattr(asset, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME):
        continue
      if getattr(asset, IS_PART_OF_ISOMORPHIC_GROUP_IN_PERIOD_FNC_NAME)(
                      baseAsset=self,
                      startDate=startDate,
                      endDate=endDate):
        result.append(asset)
        continue
    return result
  setattr(assetCapsule, GET_ASSETS_OF_BASE_ASSET_GROUP, 
              _capsule_base.cleanAndCloseSession(getAssetsOfBaseAssetGroupFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def isPartOfIsomorphicGroupInPeriodFnc(self: T,
                              baseAsset: U,
                              startDate: datetime.date,
                              endDate: datetime.date) -> bool:
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset = baseAsset,
                            fncName = IS_PART_OF_ISOMORPHIC_GROUP_IN_PERIOD_FNC_NAME)
    assetsOnPathToBaseAsset = getattr(self, GET_ASSETS_ON_PATH_TO_BASE_ASSET)(
                                                baseAsset = baseAsset,
                                                startDate = startDate,
                                                endDate = endDate)
    return len(assetsOnPathToBaseAsset) > 0
  setattr(assetCapsule, IS_PART_OF_ISOMORPHIC_GROUP_IN_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(isPartOfIsomorphicGroupInPeriodFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~       relative isomorphic section       ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getIsomorphicAssetContentOnDateFnc(self: T,
                                        baseAsset: U,
                                        date: datetime.date,
                                        dividendCapsule: V = None,
                                        limitToHoldingPeriodAnalysis: bool = False,
                                        assetGroupAndPathPricingData: dict[str, asset_time_series.AssetTimeSeries] = {},
                                        time: datetime.time = datetime.datetime.max.time()
                                        ) -> decimal.Decimal:
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset = baseAsset,
                            fncName=GET_ISOMORPHIC_ASSET_CONTENT_ON_DATE_FNC_NAME)
    containedSharesOfIsomorphicAssetInPeriod = getattr(self, GET_ISOMORPHIC_ASSET_CONTENT_IN_PERIOD_FNC_NAME)(
                            baseAsset = baseAsset,
                            startDate = date,
                            endDate = date,
                            dividendCapsule = dividendCapsule,
                            limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
                            assetGroupAndPathPricingData = assetGroupAndPathPricingData,
                            time = time)
    return containedSharesOfIsomorphicAssetInPeriod[date]
  setattr(assetCapsule, GET_ISOMORPHIC_ASSET_CONTENT_ON_DATE_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getIsomorphicAssetContentOnDateFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getContainedSharesOfIsomorphicAssetInPeriodFnc(self: T,
                                                    baseAsset: U,
                                                    startDate: datetime.date, 
                                                    endDate: datetime.date,
                                                    dividendCapsule: V = None,
                                                    limitToHoldingPeriodAnalysis: bool = False,
                                                    assetGroupAndPathPricingData: dict[str, asset_time_series.AssetTimeSeries] = {},
                                                    time: datetime.time = datetime.datetime.max.time()
                                                    ) -> typing.Dict[datetime.date, decimal.Decimal]:
    result = dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal)
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset=baseAsset,
                                      fncName=GET_CONTAINED_SHARES_OF_ISOMORPHIC_ASSET_IN_PERIOD_FNC_NAME)
    # 1.0 if 'self' is the base asset:
    if self.name == baseAsset.name: 
      return dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal,
                                  value = decimal.Decimal("1.0"))
    # other base assets imply a 'zero' participation rate
    if getattr(self, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME):
      return dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal,
                                  value = decimal.Decimal("0.0"))
    preliminaryAssetGroupAndPathPricingData = getattr(self, GET_BASIC_PRICING_DATA_IN_PERIOD_OF_ASSETS_ON_PATH_OF_BASE_ASSET_FNC_NAME)(
                                                      baseAsset = baseAsset,
                                                      dividendCapsule = dividendCapsule,
                                                      limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
                                                      assetGroupAndPathPricingData = assetGroupAndPathPricingData,
                                                      startDate = startDate,
                                                      endDate = endDate,
                                                      time = time)
    if assetGroupAndPathPricingData is None: assetGroupAndPathPricingData = {}
    for key in preliminaryAssetGroupAndPathPricingData.keys():
      if not key in assetGroupAndPathPricingData:
        assetGroupAndPathPricingData[key] = preliminaryAssetGroupAndPathPricingData[key]
    assetPricingData = assetGroupAndPathPricingData[self.name]
    result = dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal,
                                  value = decimal.Decimal("0.0"))
    # -> there is at least one underlying state
    underlyingStateValidities = getattr(self, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME)
    for underlyingStateValidity in underlyingStateValidities:
      validFrom, validTo, underlyingState = underlyingStateValidity
      validFrom = max(validFrom, startDate)
      validTo = min(validTo, endDate)
      if underlyingState is None:
        raise Exception(f"\n[{GET_CONTAINED_SHARES_OF_ISOMORPHIC_ASSET_IN_PERIOD_FNC_NAME}] " + \
                        f"No underlying state of asset '{self.name}' found for period " + \
                        f"from {validFrom} to {validTo}")
      isUnderlyingStateByPercentage = underlyingState.is_composed_by_percentage
      if isUnderlyingStateByPercentage: 
        thisAssetDenomination = decimal.Decimal("1") 
        # denomination not meaningful if the composition is defined in percentages
        # the number of shares of the underlying assets is defined upon the volume. 
      else:
        thisAssetDenomination = decimal.Decimal(str(underlyingState.denomination))
      for underlying in underlyingState.underlyings:
        underlyingAsset = underlying.asset
        underlyingIsInIsomorphicAssetGroup = getattr(underlyingAsset, IS_PART_OF_ISOMORPHIC_GROUP_IN_PERIOD_FNC_NAME)(
                                baseAsset = baseAsset,
                                startDate = validFrom,
                                endDate = validTo)
        if not underlyingIsInIsomorphicAssetGroup: continue
        containedSharesOfIsomorphicAssetInPeriod = getattr(underlyingAsset, GET_CONTAINED_SHARES_OF_ISOMORPHIC_ASSET_IN_PERIOD_FNC_NAME)(
                                baseAsset = baseAsset,
                                startDate = validFrom,
                                endDate = validTo,
                                assetGroupAndPathPricingData = assetGroupAndPathPricingData)
        underlyingComponentPricingData = assetGroupAndPathPricingData[underlyingAsset.name]
        currDate = validFrom
        while currDate <= validTo:


          # try:
          #   pass
          # except:
          #   tsXl = time_series_to_excel.TimeSeriesXl(fileName = "ding.xlsx")
          #   tsXl.addAssetTimeSeries(assetTimeSeries = {"asset": assetPricingData})
          #   tsXl.addAssetTimeSeries(assetTimeSeries = {"underlying": underlyingComponentPricingData})
          #   tsXl.writeValues()
          #   thisPath = os.path.dirname(__file__)
          #   debugDir = os.path.join(thisPath, "debug")
          #   tsXl.saveWkb(path = debugDir)
          #   print(f"[getContainedSharesOfIsomorphicAssetInPeriodFnc] " + \
          #         f"observation date = {currDate} ")
          #   appliedFx =  underlyingFX / assetFx


          if isUnderlyingStateByPercentage:
            assetPrice = assetPricingData.price_get_on_date(date = currDate)
            assetFx = assetPricingData.fx_rate_get_on_date(date = currDate)
            underlyingPrice = underlyingComponentPricingData.price_get_on_date(date = currDate)
            underlyingFX = underlyingComponentPricingData.fx_rate_get_on_date(date = currDate)
            if assetPrice is None or assetFx is None or underlyingPrice is None or underlyingFX is None:
              raise Exception(f"\n[{GET_CONTAINED_SHARES_OF_ISOMORPHIC_ASSET_IN_PERIOD_FNC_NAME}] " + \
                              f"No price data found for asset '{self.name}' and/or underlying '{underlyingAsset.name}' " + \
                              f"on date {currDate}\n" + \
                              f"assetPrice      = {assetPrice}\n" + \
                              f"assetFx         = {assetFx}\n" + \
                              f"underlyingPrice = {underlyingPrice}\n" + \
                              f"underlyingFX    = {underlyingFX}\n" + \
                              f"Please provide price and fx data on project input workbook.")
            appliedFx =  underlyingFX / assetFx
            underlyingPercentage = decimal.Decimal(str(underlying.participation_percentage))
            numberOfUnderlyingAssetInSelf = (assetPrice * underlyingPercentage) / \
                                            (underlyingPrice * appliedFx)
          else:
            numberOfUnderlyingAssetInSelf = decimal.Decimal(str(underlying.units)) * thisAssetDenomination
          result[currDate] = result[currDate] + \
                            numberOfUnderlyingAssetInSelf * containedSharesOfIsomorphicAssetInPeriod[currDate]
          currDate += datetime.timedelta(days=1) 
    return result
  setattr(assetCapsule, GET_CONTAINED_SHARES_OF_ISOMORPHIC_ASSET_IN_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getContainedSharesOfIsomorphicAssetInPeriodFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getConsecutiveProductOfDenominationInPeriodFnc(self: T,
                                                    baseAsset: U,
                                                    startDate: datetime.date, 
                                                    endDate: datetime.date) -> typing.Dict[datetime.date, decimal.Decimal]:
    result = dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal)
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset=baseAsset,
                                      fncName=GET_CONSECUTIVE_PRODUCT_OF_DENOMINATION_IN_PERIOD_FNC_NAME)
    # 1.0 if 'self' is the base asset:
    if self.name == baseAsset.name: 
      return dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal,
                                  value = decimal.Decimal("1.0"))
    # other base assets imply a 0.0 denomination (should actually not have any relevance as case excluded below)
    if getattr(self, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME):
      return dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal,
                                  value = decimal.Decimal("0.0"))
    result = dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal,
                                  value = decimal.Decimal("0.0"))
    # -> there is at least one underlying state
    underlyingStateValidities = getattr(self, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME)
    for underlyingStateValidity in underlyingStateValidities:
      validFrom, validTo, underlyingState = underlyingStateValidity
      validFrom = max(validFrom, startDate)
      validTo = min(validTo, endDate)
      thisAssetDenomination = decimal.Decimal("0.0") if underlyingState is None \
                                                else decimal.Decimal(underlyingState.denomination)
      for underlying in underlyingState.underlyings:
        underlyingAsset = underlying.asset
        if getattr(underlyingAsset, IS_PART_OF_ISOMORPHIC_GROUP_SERIES_IN_PERIOD_FNC_NAME)(
                                baseAsset = baseAsset,
                                startDate = validFrom,
                                endDate = validTo):
          consecutiveProductOfDenomination = getattr(underlyingAsset, GET_CONSECUTIVE_PRODUCT_OF_DENOMINATION_IN_PERIOD_FNC_NAME)(
                                  baseAsset = baseAsset,
                                  startDate = validFrom,
                                  endDate = validTo)
          currDate = validFrom
          while currDate <= validTo:
            underlyingConsecutiveProductOfDenomination = consecutiveProductOfDenomination[currDate]
            result[currDate] = result[currDate] + underlyingConsecutiveProductOfDenomination * thisAssetDenomination
            currDate += datetime.timedelta(days=1) 
    return result
  setattr(assetCapsule, GET_CONSECUTIVE_PRODUCT_OF_DENOMINATION_IN_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getConsecutiveProductOfDenominationInPeriodFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getPriceCapsuleOfDateFnc(self: T, 
                              date: datetime.date,
                              time: datetime.time = datetime.datetime.max.time()) -> typing.Dict[datetime.date, U]:
    lookupTimestamp = datetime.datetime.combine(date, time)
    for price in self.prices:
      if price.timestamp <= lookupTimestamp:
        return price
  setattr(assetCapsule, GET_PRICE_CAPSULE_ON_DATE_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getPriceCapsuleOfDateFnc)) 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getPriceCapsuleInPeriodFnc(self: T,
                                  startDate: datetime.date,
                                  endDate: datetime.date,
                                  time: datetime.time = datetime.datetime.max.time(),
                                  useLastIfNotAvailable: bool = False)  -> typing.Dict[datetime.date, U]:
    def getRelevantPricePos(prices: list[U],
                            date: datetime.date,
                            startPos: int = 0) -> int:
      result = -1
      for pos in range(max(startPos, 0), len(prices)):
        thisPrice = prices[pos]
        lookupTimestamp = datetime.datetime.combine(date, time)
        if thisPrice.timestamp < lookupTimestamp:
          result = pos
        else: return result
      return result
    result = dict[datetime.date, U]()
    currDate = startDate
    prices = list(self.prices)
    currDatePricePos = getRelevantPricePos(prices, currDate)
    if currDatePricePos == -1:
      thisPrice = None
      nextPrice = prices[0] if len(prices) > 0 else None
    else:
      thisPrice = prices[currDatePricePos]
      nextPrice = prices[currDatePricePos + 1] if len(prices) > currDatePricePos + 1 else None
    while currDate <= endDate:
      lookupTimestamp = datetime.datetime.combine(currDate, time)
      if not nextPrice is None:
        if nextPrice.timestamp < lookupTimestamp:
          currDatePricePos = getRelevantPricePos(prices, currDate, currDatePricePos)
          thisPrice = prices[currDatePricePos]
          nextPrice = prices[currDatePricePos + 1] if len(prices) > currDatePricePos + 1 else None
      resultValue = None  
      if not thisPrice is None:
        if useLastIfNotAvailable:
          resultValue = thisPrice
        else:
          if thisPrice.timestamp.date() == currDate:
            resultValue = thisPrice
      result[currDate] = resultValue
      currDate += datetime.timedelta(days = 1)
    return result
  setattr(assetCapsule, GET_PRICE_CAPSULE_IN_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getPriceCapsuleInPeriodFnc)) 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def getStaticAssetDataFnc(self: T,
                            startDate: datetime.date,
                            endDate: datetime.date,
                            features: asset_time_series.AssetTimeSeriesFeatures) -> asset_time_series.AssetStaticTimeSeries:
    result = asset_time_series.AssetStaticTimeSeries(startDate=startDate, endDate=endDate, features=features)
    result.fillSeriesWithConstant(label = asset_time_series.LABEL_ASSET,
                                  value = self.name) 
    result.fillSeriesWithConstant(label = asset_time_series.LABEL_ASSET_CLASS,
                                  value = self.asset_class.name) 
    result.fillSeriesWithConstant(label = asset_time_series.LABEL_IS_FINANCIAL_CONTRACT,
                                  value = self.asset_class.is_financial_contract) 
    result.fillSeriesWithConstant(label = asset_time_series.LABEL_IS_DIVIDEND_BEARING,
                                  value = self.asset_class.is_dividend_bearing) 
    assetActiveMarker = getattr(self, IS_ACTIVE_IN_PERIOD_FNC_NAME)(
                                startDate = startDate,
                                endDate = endDate)
    result.fillSeries(label = asset_time_series.LABEL_ACTIVE,
                      dataDict = assetActiveMarker) 

    underlyingStateValidities = getattr(self, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME)
    for underlyingStateValidity in underlyingStateValidities:
      partitionStartDate = max(underlyingStateValidity[0], startDate)
      partitionEndDate = min(underlyingStateValidity[1], endDate)
      underlyingState = underlyingStateValidity[2]
      underlyingStateStrikePrice = None if underlyingState is None \
                                        else underlyingState.strike_price
      underlyingStateStrikePrice = None if underlyingStateStrikePrice is None \
                                        else decimal.Decimal(underlyingStateStrikePrice)
      underlyingStateStrikePriceCurrency = None if underlyingState is None \
                                                else underlyingState.strike_price_currency
      underlyingStateStrikePriceCurrency = None if underlyingStateStrikePriceCurrency is None \
                                                else underlyingStateStrikePriceCurrency.name
      
      underlyingStateDenomination = decimal.Decimal("1") if underlyingState is None \
                                                        else decimal.Decimal(str(underlyingState.denomination))
      hasSingleUnderlying = True if underlyingState is None \
                                  else underlyingState.countOfUnderlyings <= 1
      
      currDate = partitionStartDate
      while currDate <= partitionEndDate:
        result.observationDate = currDate
        if not underlyingStateStrikePrice is None:
          result.strike_price = underlyingStateStrikePrice
          result.strike_price_currency = underlyingStateStrikePriceCurrency
        result.split_ratio = underlyingStateDenomination
        if not hasSingleUnderlying:
          result.is_composed_asset = True
          result.has_composed_underlying = False
          result.underlying_asset = None
        elif hasSingleUnderlying and underlyingState is None:
          result.is_composed_asset = False
          result.has_composed_underlying = False
          result.underlying_asset = None
        currDate += datetime.timedelta(days = 1)
          
      if hasSingleUnderlying and not underlyingState is None:
        underlyingAsset = underlyingState.getUnderlying(position = 0).asset
        underlyingStateValiditiesOfUnderlying = getattr(underlyingAsset, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME)
        for underlyingStateValidityOfUnderlying in underlyingStateValiditiesOfUnderlying:
          subPartitionStartDate = max(underlyingStateValidityOfUnderlying[0], partitionStartDate)
          subPartitionEndDate = min(underlyingStateValidityOfUnderlying[1], partitionEndDate)
          underlyingStateOfUnderlying = underlyingStateValidityOfUnderlying[2]
          hasSingleUnderlyingOfUnderlying = True if underlyingStateOfUnderlying is None \
                                                else underlyingStateOfUnderlying.countOfUnderlyings <= 1
          currDate = subPartitionStartDate
          while currDate <= subPartitionEndDate:
            result.observationDate = currDate
            result.is_composed_asset = False
            result.has_composed_underlying = not hasSingleUnderlyingOfUnderlying
            result.underlying_asset = underlyingAsset.name # if hasSingleUnderlyingOfUnderlying \
                                                          # else None
            currDate += datetime.timedelta(days = 1)
    result.resetObservationDate
    return result
  setattr(assetCapsule, GET_STATIC_ASSET_DATA_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getStaticAssetDataFnc))    
  # # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # def get
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def getLastAvailablePriceDataFnc(self: T,
                                    startDate: datetime.date,
                                    endDate: datetime.date,
                                    features: asset_time_series.AssetTimeSeriesFeatures,
                                    baseAsset: U = None,
                                    limitToHoldingPeriodAnalysis: bool = False,
                                    time: datetime.time = datetime.datetime.max.time()) -> asset_time_series.AssetAvailablePriceTimeSeries:
    if not baseAsset is None:
      if not getattr(self, IS_PART_OF_ISOMORPHIC_GROUP_IN_PERIOD_FNC_NAME)(baseAsset = baseAsset, 
                                                                          startDate=startDate,
                                                                          endDate=endDate):
        return None
    result = asset_time_series.AssetLastAvailablePriceTimeSeries(startDate=startDate, endDate=endDate, features=features)
    # 240903 SFR - always try to source prices; required to identify pricing requirements
    # if limitToHoldingPeriodAnalysis: return result
    pricingCapsules = getattr(self, GET_PRICE_CAPSULE_IN_PERIOD_FNC_NAME)(
                                  startDate = startDate,
                                  endDate = endDate,
                                  time = time,
                                  useLastIfNotAvailable = True)
    currDate = startDate
    while currDate <= endDate:
      result.observationDate = currDate
      pricingCapsule = pricingCapsules[currDate]
      result.price = None if pricingCapsule is None \
                              else decimal.Decimal(str(pricingCapsule.price))
      result.has_price = False if pricingCapsule is None else True
      if baseAsset is None:
        result.currency = None if pricingCapsule is None \
                                    else pricingCapsule.currency.name
      currDate += datetime.timedelta(days = 1)
    if not baseAsset is None:
      # asset prices and fx
      baseAssetPricingCapsules = getattr(baseAsset, GET_PRICE_CAPSULE_IN_PERIOD_FNC_NAME)(
                                    startDate = startDate,
                                    endDate = endDate,
                                    time = time,
                                    useLastIfNotAvailable = True)  
      fxPartitions = state_control.getFxPartitions(startDate=startDate,
                                                    endDate=endDate,
                                                    parentPriceCapsules = baseAssetPricingCapsules,
                                                    basketItemPriceCapsules = pricingCapsules)    
      for fxPartition in fxPartitions:
        partitionStartDate = max(fxPartition.startDate, startDate)
        partitionEndDate = min(fxPartition.endDate, endDate)
        partitionStartPriceCapsule = pricingCapsules[partitionStartDate]
        partitionStartBasePriceCapsule = baseAssetPricingCapsules[partitionStartDate]
        # The default currency of the current asset is assumed if there are no prices
        #     available for the current asset 
        #     -> this might lead to additional fx rate requirements once curr asset
        #        prices are available
        partitionCurrency = self.currency if partitionStartPriceCapsule is None \
                                          else partitionStartPriceCapsule.currency
        # The default currency of the base asset is assumed if there are no prices
        #     available for the base asset 
        #     -> this might lead to additional fx rate requirements once base asset
        #        prices are available
        partitionBaseCurrency = baseAsset.currency if partitionStartBasePriceCapsule is None \
                                          else partitionStartBasePriceCapsule.currency
        if partitionCurrency.name == partitionBaseCurrency.name:
          # partition currency is the same as the base currency => fx rate is 1 by definition
          currDate = partitionStartDate
          while currDate <= partitionEndDate:
            result.observationDate = currDate
            result.currency = partitionCurrency.name
            result.base_asset_currency = partitionBaseCurrency.name
            result.has_fx_rate = True
            result.fx_rate = decimal.Decimal("1")
            currDate += datetime.timedelta(days = 1)
        else:
          # partition currency is different from the base currency => get fx rate as specified by db entries
          partitionFxRates = getattr(partitionCurrency, spec_currency.GET_FX_RATE_IN_PERIOD_FNC_NAME)(
                          baseCurrencyName = partitionBaseCurrency.name,
                          startDate = partitionStartDate,
                          endDate = partitionEndDate,
                          useLastIfNotAvailable = True)
          for date, value in partitionFxRates.items():
            result.observationDate = date
            result.currency = partitionCurrency.name
            result.base_asset_currency = partitionBaseCurrency.name
            result.has_fx_rate = not(value is None)
            result.fx_rate = None if value is None else decimal.Decimal(str(value))
      # strike price fx:
      underlyingStateValidities = getattr(self, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME)
      strikePriceFxRates = dict[str, dict[datetime.date, V]]()
      for underlyingStateValidity in underlyingStateValidities:
        partitionStartDate = max(underlyingStateValidity[0], startDate)
        partitionEndDate = min(underlyingStateValidity[1], endDate)
        underlyingState = underlyingStateValidity[2]
        underlyingStateStrikePriceCurrency = None if underlyingState is None \
                                                  else underlyingState.strike_price_currency
        if underlyingStateStrikePriceCurrency is None: continue
        underlyingStateStrikePriceCurrencyName = underlyingStateStrikePriceCurrency.name

        currDate = partitionStartDate
        while currDate <= partitionEndDate:
          result.observationDate = currDate
          baseAssetPriceCapsule = None if not currDate in baseAssetPricingCapsules \
                                        else baseAssetPricingCapsules[currDate]
          # The default currency of the base asset is assumed if there are no prices
          #     available for the base asset 
          #     -> this might lead to additional fx rate requirements once base asset
          #        prices are available
          baseAssetCurrency = baseAsset.currency if baseAssetPriceCapsule is None \
                                                 else baseAssetPriceCapsule.currency
          baseAssetCurrencyName = baseAssetCurrency.name
          if baseAssetCurrencyName == underlyingStateStrikePriceCurrencyName:
            # default fx rate to 1 if base asset currency is the same as strike price currency
            result.has_strike_price_fx_on_date = True
            result.strike_price_fx_on_date = decimal.Decimal("1")
          elif result.currency == underlyingStateStrikePriceCurrencyName:
            # copy fx rates if currencies are the same as those of asset pricing
            result.strike_price_fx_on_date = result.fx_rate_on_date
            result.has_strike_price_fx_on_date = result.has_fx_rate_on_date       
          else:
            # Identify and log fx rates if currencies are different
            fxRateId = f"{baseAssetCurrencyName}/{underlyingStateStrikePriceCurrencyName}"
            if not fxRateId in strikePriceFxRates:
              fxRates = getattr(underlyingStateStrikePriceCurrency, spec_currency.GET_FX_RATE_IN_PERIOD_FNC_NAME)(
                                  baseCurrencyName = baseAssetCurrencyName,
                                  startDate = startDate,
                                  endDate = endDate,
                                  useLastIfNotAvailable = True)
              strikePriceFxRates[fxRateId] = fxRates
            fxRateSeries = strikePriceFxRates[fxRateId]
            fxRate = None if not currDate in fxRateSeries \
                          else decimal.Decimal(str(fxRateSeries[currDate]))
            # fxRate = None if fxRate is None \
            #               else decimal.Decimal(str(fxRate))
            result.strike_price_fx_on_date = fxRate
            result.has_strike_price_fx_on_date = not(fxRate is None) 
          currDate += datetime.timedelta(days = 1)

      underlyingStateValidities = getattr(self, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME)
      strikePriceFxRates = dict[str, dict[datetime.date, V]]()
      for underlyingStateValidity in underlyingStateValidities:
        partitionStartDate = max(underlyingStateValidity[0], startDate)
        partitionEndDate = min(underlyingStateValidity[1], endDate)
        underlyingState = underlyingStateValidity[2]
        underlyingStateStrikePriceCurrency = None if underlyingState is None \
                                                  else underlyingState.strike_price_currency
        if underlyingStateStrikePriceCurrency is None: continue
        underlyingStateStrikePriceCurrencyName = underlyingStateStrikePriceCurrency.name
        if underlyingStateStrikePriceCurrencyName == partitionBaseCurrency.name:
          # strike price currency is the same as the base currency => fx rate is 1 by definition
          currDate = partitionStartDate
          while currDate <= partitionEndDate:
            result.observationDate = currDate
            result.currency = partitionCurrency.name
            result.base_asset_currency = partitionBaseCurrency.name
            result.has_strike_price_fx = True
            result.strike_price_fx = decimal.Decimal("1")
            currDate += datetime.timedelta(days = 1)
        else:
          # strike price currency is different from the base currency => get fx rate as specified by db entries
          strikePriceFxRates = getattr(underlyingStateStrikePriceCurrency, spec_currency.GET_FX_RATE_IN_PERIOD_FNC_NAME)(
                    baseCurrencyName = partitionBaseCurrency.name,
                    startDate = partitionStartDate,
                    endDate = partitionEndDate,
                    useLastIfNotAvailable = True)
          currDate = partitionStartDate
          while currDate <= partitionEndDate:
            result.observationDate = currDate
            if result.currency == underlyingStateStrikePriceCurrencyName:
              # copy fx rates if currencies are the same
              result.strike_price_fx = result.fx_rate
              result.has_strike_price_fx = result.has_fx_rate       
            else:
              # Identify and log fx rates if currencies are different
              baseAssetPriceCapsule = baseAssetPricingCapsules[currDate]
              baseAssetCurrency = baseAsset.currency if baseAssetPriceCapsule is None \
                                                    else baseAssetPriceCapsule.currency
              baseAssetCurrencyName = baseAssetCurrency.name
              fxRateId = f"{baseAssetCurrencyName}/{underlyingStateStrikePriceCurrencyName}"
              if not fxRateId in strikePriceFxRates:
                fxRates = getattr(underlyingStateStrikePriceCurrency, spec_currency.GET_FX_RATE_IN_PERIOD_FNC_NAME)(
                                    baseCurrencyName = baseAssetCurrencyName,
                                    startDate = startDate,
                                    endDate = endDate,
                                    useLastIfNotAvailable = True)
                strikePriceFxRates[fxRateId] = fxRates
              fxRateCapsules = strikePriceFxRates[fxRateId]
              fxRateCapsule = fxRates[currDate]
              fxRate = None if fxRateCapsule \
                            else fxRateCapsule.rate
              fxRate = None if fxRate is None \
                            else decimal.Decimal(str(fxRate))
              result.strike_price_fx = fxRate
              result.has_strike_price_fx = not(fxRate is None) 
            # increment currDate
            currDate += datetime.timedelta(days = 1)
    result.resetObservationDate
    return result
  setattr(assetCapsule, GET_LAST_AVAILABLE_PRICE_DATA_FNC_NAME,
              _capsule_base.cleanAndCloseSession(getLastAvailablePriceDataFnc))  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def getAvailablePriceDataFnc(self: T,
                                startDate: datetime.date,
                                endDate: datetime.date,
                                features: asset_time_series.AssetTimeSeriesFeatures,
                                baseAsset: U = None,
                                limitToHoldingPeriodAnalysis: bool = False,
                                time: datetime.time = datetime.datetime.max.time()) -> asset_time_series.AssetAvailablePriceTimeSeries:
    if not baseAsset is None:
      if not getattr(self, IS_PART_OF_ISOMORPHIC_GROUP_IN_PERIOD_FNC_NAME)(baseAsset = baseAsset, 
                                                                          startDate=startDate,
                                                                          endDate=endDate):
        return None
    result = asset_time_series.AssetAvailablePriceTimeSeries(startDate=startDate, endDate=endDate, features=features)
    # 240903 SFR - always try to source prices; required to identify pricing requirements
    # if limitToHoldingPeriodAnalysis: return result
    pricingCapsules = getattr(self, GET_PRICE_CAPSULE_IN_PERIOD_FNC_NAME)(
                                  startDate = startDate,
                                  endDate = endDate,
                                  time = time,
                                  useLastIfNotAvailable = False)
    currDate = startDate
    while currDate <= endDate:
      result.observationDate = currDate
      pricingCapsule = pricingCapsules[currDate]
      result.price_on_date = None if pricingCapsule is None \
                                      else decimal.Decimal(str(pricingCapsule.price))
      result.has_price_on_date = False if pricingCapsule is None else True
      currDate += datetime.timedelta(days = 1)
    if not baseAsset is None:
      baseAssetPricingCapsules = getattr(baseAsset, GET_PRICE_CAPSULE_IN_PERIOD_FNC_NAME)(
                                    startDate = startDate,
                                    endDate = endDate,
                                    time = time,
                                    useLastIfNotAvailable = False)
      fxPartitions = state_control.getFxPartitions(startDate=startDate,
                                                    endDate=endDate,
                                                    parentPriceCapsules = baseAssetPricingCapsules,
                                                    basketItemPriceCapsules = pricingCapsules)
      identifiedCurrenciesDict = dict[datetime.date, str]()  
      for fxPartition in fxPartitions:
        partitionStartDate = max(fxPartition.startDate, startDate)
        partitionEndDate = min(fxPartition.endDate, endDate)
        partitionStartPriceCapsule = pricingCapsules[partitionStartDate]
        partitionStartBasePriceCapsule = baseAssetPricingCapsules[partitionStartDate]
        partitionCurrency = None if partitionStartPriceCapsule is None else partitionStartPriceCapsule.currency
        partitionBaseCurrency = None if partitionStartBasePriceCapsule is None else partitionStartBasePriceCapsule.currency
        if partitionCurrency is None or partitionBaseCurrency is None:          
          continue
        if partitionCurrency.name == partitionBaseCurrency.name:
          # partition currency is the same as the base currency => fx rate is 1 by definition
          currDate = partitionStartDate
          while currDate <= partitionEndDate:
            result.observationDate = currDate
            result.has_fx_rate_on_date = True
            result.fx_rate_on_date = decimal.Decimal("1")
            currDate += datetime.timedelta(days = 1)
        else:
          # partition currency is different from the base currency => get fx rate as specified by db entries
          partitionFxRates = getattr(partitionCurrency, spec_currency.GET_FX_RATE_IN_PERIOD_FNC_NAME)(
                          baseCurrencyName = partitionBaseCurrency.name,
                          startDate = partitionStartDate,
                          endDate = partitionEndDate,
                          useLastIfNotAvailable = False)
          for date, value in partitionFxRates.items():
            identifiedCurrenciesDict[date] = partitionCurrency.name
            result.observationDate = date
            result.fx_rate_on_date = value
            result.has_fx_rate_on_date = not (value is None)
      # strike price fx:
      underlyingStateValidities = getattr(self, GET_UNDERLYING_STATE_VALIDITY_PROP_NAME)
      strikePriceFxRates = dict[str, dict[datetime.date, V]]()
      for underlyingStateValidity in underlyingStateValidities:
        partitionStartDate = max(underlyingStateValidity[0], startDate)
        partitionEndDate = min(underlyingStateValidity[1], endDate)
        underlyingState = underlyingStateValidity[2]
        underlyingStateStrikePriceCurrency = None if underlyingState is None \
                                                  else underlyingState.strike_price_currency
        if underlyingStateStrikePriceCurrency is None: continue
        underlyingStateStrikePriceCurrencyName = underlyingStateStrikePriceCurrency.name

        currDate = partitionStartDate
        lastIdentifiedCurrency = ""
        while currDate <= partitionEndDate:
          result.observationDate = currDate
          baseAssetPriceCapsule = baseAssetPricingCapsules[currDate]
          baseAssetCurrency = baseAsset.currency if baseAssetPriceCapsule is None else baseAssetPriceCapsule.currency
          baseAssetCurrencyName = baseAssetCurrency.name
          lastIdentifiedCurrency = identifiedCurrenciesDict[currDate] if currDate in identifiedCurrenciesDict \
                                                                      else lastIdentifiedCurrency
          if baseAssetCurrencyName == underlyingStateStrikePriceCurrencyName:
            # default fx rate to 1 if base asset currency is the same as strike price currency
            result.has_strike_price_fx_on_date = True
            result.strike_price_fx_on_date = decimal.Decimal("1")
          elif lastIdentifiedCurrency == underlyingStateStrikePriceCurrencyName:
            # copy fx rates if currencies are the same as those of asset pricing
            result.strike_price_fx_on_date = result.fx_rate_on_date
            result.has_strike_price_fx_on_date = result.has_fx_rate_on_date       
          else:
            # Identify and log fx rates if currencies are different
            fxRateId = f"{baseAssetCurrencyName}/{underlyingStateStrikePriceCurrencyName}"
            if not fxRateId in strikePriceFxRates:
              fxRates = getattr(underlyingStateStrikePriceCurrency, spec_currency.GET_FX_RATE_IN_PERIOD_FNC_NAME)(
                                  baseCurrencyName = baseAssetCurrencyName,
                                  startDate = startDate,
                                  endDate = endDate,
                                  useLastIfNotAvailable = False)
              strikePriceFxRates[fxRateId] = fxRates
            fxRateSeries = strikePriceFxRates[fxRateId]
            fxRate = None if not currDate in fxRateSeries \
                          else fxRateSeries[currDate]
            fxRate = None if fxRate is None \
                          else decimal.Decimal(str(fxRate))
            result.strike_price_fx_on_date = fxRate
            result.has_strike_price_fx_on_date = not(fxRate is None) 
          currDate += datetime.timedelta(days = 1)
    result.resetObservationDate
    return result
  setattr(assetCapsule, GET_AVAILABLE_PRICE_DATA_FNC_NAME,
              _capsule_base.cleanAndCloseSession(getAvailablePriceDataFnc))  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def getDividendDataFnc(self: T,
                        startDate: datetime.date,
                        endDate: datetime.date,
                        features: asset_time_series.AssetTimeSeriesFeatures,
                        dividendCapsule: V = None
                        ) -> asset_time_series.AssetDividendTimeSeries:
    result = asset_time_series.AssetDividendTimeSeries(startDate=startDate, endDate=endDate, features=features)
    if dividendCapsule is None: return result
    dividendAsset = dividendCapsule.asset
    if not dividendAsset.name == self.name: return result
    exDate = dividendCapsule.ex_date
    if not exDate in result._dates: return result
    result.dividendExDate = exDate
    result.observationDate = exDate
    result.has_dividend = True
    result.dividend_ex_date = exDate
    result.dividend_pay_date = dividendCapsule.pay_date
    result.dividend_number_of_shares = dividendCapsule.number_of_shares
    result.dividend_gross = decimal.Decimal(str(dividendCapsule.gross_dividend))
    result.dividend_wht = decimal.Decimal(str(dividendCapsule.wht))
    result.dividend_net = None if dividendCapsule.net_dividend is None else decimal.Decimal(str(dividendCapsule.net_dividend))
    result.dividend_gross_per_share = decimal.Decimal(str(dividendCapsule.gross_dividend_per_share))
    result.dividend_wht_per_share = decimal.Decimal(str(dividendCapsule.wht_per_share))
    result.resetObservationDate
    return result
  setattr(assetCapsule, GET_DIVIDEND_DATA_FNC_NAME,
              _capsule_base.cleanAndCloseSession(getDividendDataFnc))  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def getPricingDataInPeriodBasicFnc(self: T,
                                    startDate: datetime.date,
                                    endDate: datetime.date,
                                    baseAsset: U = None,
                                    dividendCapsule: V = None,
                                    limitToHoldingPeriodAnalysis: bool = False,
                                    time: datetime.time = datetime.datetime.max.time()) -> asset_time_series.AssetTimeSeries:
    if not baseAsset is None:
      if not getattr(baseAsset, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME):
        raise Exception("Base asset must be the base asset of an isomorphic asset group.\n" +
                        f"Base asset provided to '{GET_PRICING_DATA_IN_PERIOD_FNC_NAME}': {baseAsset.name}")
    result = asset_time_series.AssetTimeSeries(startDate=startDate, endDate=endDate)
    result.limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis
    assetStaticTimeSeries = getattr(self, GET_STATIC_ASSET_DATA_FNC_NAME)(startDate, endDate, result._features)
    if assetStaticTimeSeries is None: return None
    result._staticTimeSeries = assetStaticTimeSeries
    availablePriceTimeSeries = getattr(self, GET_AVAILABLE_PRICE_DATA_FNC_NAME)(startDate, endDate, result._features, baseAsset, 
                                                                                limitToHoldingPeriodAnalysis, time)
    if availablePriceTimeSeries is None: return None
    result._availablePriceTimeSeries = availablePriceTimeSeries
    lastAvailablePriceTimeSeries = getattr(self, GET_LAST_AVAILABLE_PRICE_DATA_FNC_NAME)(startDate, endDate, result._features, baseAsset, 
                                                                                        limitToHoldingPeriodAnalysis, time)
    if lastAvailablePriceTimeSeries is None: return None
    result._lastAvailablePriceTimeSeries = lastAvailablePriceTimeSeries
    dividendTimeSeries = getattr(self, GET_DIVIDEND_DATA_FNC_NAME)(startDate, endDate, result._features, dividendCapsule)
    if dividendTimeSeries is None: return None
    result._dividendTimeSeries = dividendTimeSeries
    result.dividendExDate = dividendTimeSeries.dividendExDate
    return result
  setattr(assetCapsule, GET_PRICING_DATA_IN_PERIOD_BASIC_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getPricingDataInPeriodBasicFnc)) 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getPricingDataInPeriodFnc(self: T,
                                startDate: datetime.date,
                                endDate: datetime.date,
                                baseAsset: U = None,
                                dividendCapsule: V = None,
                                limitToHoldingPeriodAnalysis: bool = False,
                                assetGroupAndPathPricingData: dict[str, asset_time_series.AssetTimeSeries] = {},
                                time: datetime.time = datetime.datetime.max.time()) -> asset_time_series.AssetTimeSeries:
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset=baseAsset,
                                      fncName=GET_PRICING_DATA_IN_PERIOD_FNC_NAME)
    preliminaryAssetGroupAndPathPricingData = getattr(self, GET_BASIC_PRICING_DATA_IN_PERIOD_OF_ASSETS_ON_PATH_OF_BASE_ASSET_FNC_NAME)(
                                                      baseAsset = baseAsset,
                                                      dividendCapsule = dividendCapsule,
                                                      limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
                                                      assetGroupAndPathPricingData = assetGroupAndPathPricingData,
                                                      startDate = startDate,
                                                      endDate = endDate,
                                                      time = time)
    if assetGroupAndPathPricingData is None: assetGroupAndPathPricingData = {}
    for key in preliminaryAssetGroupAndPathPricingData.keys():
      if not key in assetGroupAndPathPricingData:
        assetGroupAndPathPricingData[key] = preliminaryAssetGroupAndPathPricingData[key]
    if not self.name in assetGroupAndPathPricingData: return None
    result = assetGroupAndPathPricingData[self.name]
    if not limitToHoldingPeriodAnalysis:
      isomorphicAssetContent = getattr(self, GET_CONTAINED_SHARES_OF_ISOMORPHIC_ASSET_IN_PERIOD_FNC_NAME)(
                                      baseAsset = baseAsset,                                  
                                      startDate = startDate,
                                      endDate = endDate,
                                      assetGroupAndPathPricingData = assetGroupAndPathPricingData)
      result._compositionTimeSeries.fillSeries(label = asset_time_series.LABEL_RELATIVE_ISOMORPHIC_ASSET_CONTENT,
                        dataDict = isomorphicAssetContent)
    result._baseAssetTimeSeries.fillSeriesWithConstant(label = asset_time_series.LABEL_BASE_ASSET,
                                  value = baseAsset.name)
    return result
  setattr(assetCapsule, GET_PRICING_DATA_IN_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getPricingDataInPeriodFnc)) 
  # ~~~~~~~~~~~~~~~~~~~~ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def __completeBasicDictionaryOfAssetTimeSeries(
                          dictOfAssetTimeSeries:  typing.Dict[str, asset_time_series.AssetTimeSeries],
                          assets: typing.Dict[str, V],
                          baseAsset: U,
                          limitToHoldingPeriodAnalysis: bool):
    def addBaseAssetData(timeSeries: asset_time_series.AssetTimeSeries):
      currDate = timeSeries.startDate
      baseAssetTimeSeries = dictOfAssetTimeSeries[baseAsset.name]
      while currDate <= timeSeries.endDate:
        timeSeries.setObservationDate(observationDate=currDate)
        baseAssetTimeSeries.setObservationDate(observationDate=currDate)
        if baseAssetTimeSeries.asset != timeSeries.asset:
          timeSeries.base_asset = baseAssetTimeSeries.asset
          timeSeries.base_asset_active = baseAssetTimeSeries.active
          timeSeries.base_asset_class = baseAssetTimeSeries.asset_class
          timeSeries.base_asset_currency = baseAssetTimeSeries.currency
          timeSeries.base_asset_price = baseAssetTimeSeries.price
          timeSeries.base_asset_has_price = baseAssetTimeSeries.has_price
          timeSeries.base_asset_fx_rate = baseAssetTimeSeries.fx_rate
          timeSeries.base_asset_has_fx_rate = baseAssetTimeSeries.has_fx_rate
        currDate += datetime.timedelta(days = 1)  
      timeSeries.resetObservationDate
    for assetTimeSeries in dictOfAssetTimeSeries.values():
      if assetTimeSeries.hasCompletedAssetCompositionData: continue
      addBaseAssetData(timeSeries = assetTimeSeries)
      assetName = assetTimeSeries.asset
      asset = assets[assetName]
      isFinancialContract = asset.asset_class.is_financial_contract
      currDate = assetTimeSeries.startDate
      while currDate <= assetTimeSeries.endDate:
        assetTimeSeries.setObservationDate(observationDate=currDate)
        assetTimeSeries.resetLineDictCount
        assetUnderlyingState = getattr(asset, GET_UNDERLYING_STATE_OF_DATE_FNC_NAME)(date=currDate)
        if not assetUnderlyingState is None: # this is an inactive asset or a base asset   
          isUnderlyingStateByPercentage = assetUnderlyingState.is_composed_by_percentage

          if assetName == "IDX-1 Put":
            print(f"\nassetName: {asset.name} - currDate: {currDate}")
            for underlying in assetUnderlyingState.underlyings:
              print(f"underlying: {underlying.asset.name} - underlying.units: {underlying.units} - underlying.participation_percentage: {underlying.participation_percentage}")
            print(f"isUnderlyingStateByPercentage: {isUnderlyingStateByPercentage}\n")

          underlyings = list(assetUnderlyingState.underlyings)
          if len(underlyings) != 1 and isFinancialContract:
            raise Exception(f"Asset {assetName} is a financial contract with multiple underlyings.\n" + \
                            f"Asset class: {asset.asset_class.name}\n" + \
                            f"Date: {currDate}\n" + \
                            f"Number of underlyings: {len(underlyings)}\n" + \
                            f"underlyings: {underlyings}")
          
          if assetTimeSeries.is_composed_asset:
            assetTimeSeries.underlying_is_composed_underlying = True
            assetTimeSeries.underlying_name = "asset used as underlying"
            assetTimeSeries.underlying_price = assetTimeSeries.price
            assetTimeSeries.underlying_fx = assetTimeSeries.fx_rate
            assetTimeSeries.underlying_composed_by_percentage = isUnderlyingStateByPercentage
          elif assetTimeSeries.has_composed_underlying:
            assetTimeSeries.underlying_is_composed_underlying = True
            composedUnderlyingAsset = underlyings[0]
            composedUnderlyingAsset = composedUnderlyingAsset.asset
            composedUnderlyingAssetName = composedUnderlyingAsset.name
            composedUnderlyingAssetTimeSeries = dictOfAssetTimeSeries[composedUnderlyingAssetName]
            composedUnderlyingAssetTimeSeries.setObservationDate(observationDate=currDate)
            assetTimeSeries.underlying_name = composedUnderlyingAssetName
            assetTimeSeries.underlying_price = composedUnderlyingAssetTimeSeries.price
            assetTimeSeries.underlying_fx = composedUnderlyingAssetTimeSeries.fx_rate
            underlyingAssetState = getattr(composedUnderlyingAsset, GET_UNDERLYING_STATE_OF_DATE_FNC_NAME)(date=currDate)
            # Update calculation parameters
            isUnderlyingStateByPercentage = underlyingAssetState.is_composed_by_percentage
            underlyings = list(underlyingAssetState.underlyings)
            assetTimeSeries.underlying_composed_by_percentage = isUnderlyingStateByPercentage
          elif len(underlyings) == 0:
            assetTimeSeries.underlying_is_composed_underlying = False 
            assetTimeSeries.underlying_name = "no underlying"
            assetTimeSeries.underlying_price = assetTimeSeries.price
            assetTimeSeries.underlying_fx = assetTimeSeries.fx_rate
            assetTimeSeries.underlying_composed_by_percentage = False
          else:
            assetTimeSeries.underlying_is_composed_underlying = False 
            assetTimeSeries.underlying_name = "asset not composed"
            assetTimeSeries.underlying_price = assetTimeSeries.price
            assetTimeSeries.underlying_fx = assetTimeSeries.fx_rate 
            assetTimeSeries.underlying_composed_by_percentage = isUnderlyingStateByPercentage
          for underlying in underlyings:
            underlyingAsset = underlying.asset
            underlyingName = underlyingAsset.name
            if underlyingName in dictOfAssetTimeSeries: # not in base asset group
              underlyingTimeSeries = dictOfAssetTimeSeries[underlyingName]
              underlyingTimeSeries.setObservationDate(observationDate=currDate)
              assetTimeSeries.incLineDictMax()
              underlyingTimeSeries.setObservationDate(observationDate=currDate)
              assetTimeSeries.composition_defined_by_percentages = isUnderlyingStateByPercentage
              assetTimeSeries.underlying_component_name = underlyingTimeSeries.asset
              if not limitToHoldingPeriodAnalysis: 
                # Pricing data may be missing if limitToHoldingPeriodAnalysis
                assetTimeSeries.underlying_component_price = underlyingTimeSeries.price
                assetTimeSeries.underlying_component_price_currency = underlyingTimeSeries.currency
                assetTimeSeries.underlying_component_price_fx = underlyingTimeSeries.fx_rate
              assetTimeSeries.underlying_component_contained_shares_of_base_asset = None # not defined in this processing step
              if not limitToHoldingPeriodAnalysis:  
                if isUnderlyingStateByPercentage:
                  # Pricing data may be missing if limitToHoldingPeriodAnalysis
                  assetTimeSeries.underlying_component_participation_percentage = decimal.Decimal(str(underlying.participation_percentage))
                  hasSomeElementNone = (assetTimeSeries.underlying_component_price is None) or \
                                        (assetTimeSeries.underlying_component_price_fx is None) or \
                                        (underlyingTimeSeries.price is None) or \
                                        (underlyingTimeSeries.fx_rate is None) or \
                                        (assetTimeSeries.underlying_component_participation_percentage is None) 
                  if hasSomeElementNone:
                    raise Exception(f"Can't determine number of shares underlying component contained in underlying.\n" + \
                                    f"Some inputs carry 'None' values.\n" + \
                                    f"Date: {currDate}\n" + \
                                    f"Asset     : {assetName:<30} - price: {assetTimeSeries.underlying_component_price} - fx: {assetTimeSeries.underlying_component_price_fx}\n" + \
                                    f"isUnderlyingStateByPercentage: {isUnderlyingStateByPercentage}")
                  numberOfUnderlyingAssetInUnderlying = (underlyingTimeSeries.price * underlyingTimeSeries.fx_rate) / \
                                                        (assetTimeSeries.underlying_component_price * assetTimeSeries.underlying_component_price_fx) \
                                                        * assetTimeSeries.underlying_component_participation_percentage
                  assetTimeSeries.underlying_component_shares_in_asset = numberOfUnderlyingAssetInUnderlying
                else:
                  try:
                    assetTimeSeries.underlying_component_units = decimal.Decimal(str(underlying.units))
                    assetTimeSeries.underlying_component_shares_in_asset = assetTimeSeries.underlying_component_units
                  except:
                    raise Exception(f"Can't determine number of shares underlying component contained in underlying.\n" + \
                                    f"Some inputs carry 'None' values.\n" + \
                                    f"Date: {currDate}\n" + \
                                    f"Asset     : {assetName:<30}\n" + \
                                    f"Underlying: {underlyingName:<30} - \n" + \
                                    f"underlying.units: {underlying.units} - type(underlying.units): {type(underlying.units)} - \n" + \
                                    f"isUnderlyingStateByPercentage     : {isUnderlyingStateByPercentage} - \n" + \
                                    f"limitToHoldingPeriodAnalysis: {limitToHoldingPeriodAnalysis} - \n" + \
                                    f"isUnderlyingStateByPercentage and not limitToHoldingPeriodAnalysis: {isUnderlyingStateByPercentage and not limitToHoldingPeriodAnalysis}")
              assetTimeSeries.implicitly_contained_shares_of_base_asset = None # not defined in this processing step
              assetTimeSeries.relative_isomorphic_asset_content = None # not defined in this processing step
        currDate += datetime.timedelta(days=1)  
      assetTimeSeries.hasCompletedAssetCompositionData = True
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def __completeDictionaryOfAssetTimeSeriesWithIsoContent(
                          dictOfAssetTimeSeries: typing.Dict[str, asset_time_series.AssetTimeSeries],
                          baseAsset: U,
                          thisAssetName: str = None):
    for assetName, assetTimeSeries in dictOfAssetTimeSeries.items():
      if assetTimeSeries.hasCompletedIsoContent: continue
      # limit analysis if specified (see input variables):
      if not thisAssetName is None:
        if not thisAssetName == assetName: continue
      # start determination exercise:
      originalObservationDate = assetTimeSeries.observationDate
      currDate = assetTimeSeries.startDate
      while currDate <= assetTimeSeries.endDate:
        assetTimeSeries.observationDate = currDate
        assetTimeSeries.setLineDictMax
        # default to '1' if asset is base asset
        if assetName == baseAsset.name: 
          assetTimeSeries.implicitly_contained_shares_of_base_asset = decimal.Decimal("1.0") 
        else:
          assetTimeSeries.setLineDictMaxOfDate(date=currDate)
          implicitlyContainedSharesOfBaseAsset = decimal.Decimal("0.0")
          assetTimeSeries.resetLineDictCount
          while not assetTimeSeries.isBeyondLineDictMax:
            underlyingComponentContainedSharesOfBaseAsset = assetTimeSeries.underlying_component_contained_shares_of_base_asset
            if underlyingComponentContainedSharesOfBaseAsset is None:
              underlyingComponentName = assetTimeSeries.underlying_component_name
              underlyingComponentTimeSeries = dictOfAssetTimeSeries[underlyingComponentName]
              underlyingComponentContainedSharesOfBaseAsset2= underlyingComponentTimeSeries.implicitly_contained_shares_of_base_asset
              if underlyingComponentContainedSharesOfBaseAsset2 is None:
                __completeDictionaryOfAssetTimeSeriesWithIsoContent(dictOfAssetTimeSeries = dictOfAssetTimeSeries,
                                                                    baseAsset = baseAsset,
                                                                    thisAssetName = underlyingComponentName)
              underlyingComponentContainedSharesOfBaseAsset2= underlyingComponentTimeSeries.implicitly_contained_shares_of_base_asset
              assetTimeSeries.underlying_component_contained_shares_of_base_asset = underlyingComponentContainedSharesOfBaseAsset2
              assetSplitRatio = assetTimeSeries.split_ratio
              componentUnitsInAsset = assetTimeSeries.underlying_component_shares_in_asset
              if not componentUnitsInAsset is None:  
                implicitlyContainedSharesOfBaseAsset += assetSplitRatio * \
                                                        componentUnitsInAsset * \
                                                        underlyingComponentContainedSharesOfBaseAsset2
            assetTimeSeries.implicitly_contained_shares_of_base_asset = implicitlyContainedSharesOfBaseAsset 
            assetTimeSeries.incLineDictCount  
        currDate += datetime.timedelta(days=1)
      assetTimeSeries.hasCompletedIsoContent = True
      assetTimeSeries.observationDate = originalObservationDate
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getPricingDataInPeriodOfAssetsOnPathFnc(self: T,
                                              startDate: datetime.date,
                                              endDate: datetime.datetime,
                                              baseAsset: U = None, 
                                              dividendCapsule: V = None,
                                              limitToHoldingPeriodAnalysis: bool = False,
                                              assetGroupAndPathPricingData: dict[str, asset_time_series.AssetTimeSeries] = {},
                                              time: datetime.time = datetime.datetime.max.time()
                                              ) -> typing.Dict[str, asset_time_series.AssetTimeSeries]:
    result = {}
    assetsOnPathToBaseAsset = getattr(self, GET_ASSETS_ON_PATH_TO_BASE_ASSET)(baseAsset = baseAsset,
                                                                              startDate = startDate,
                                                                              endDate = endDate)
    if assetGroupAndPathPricingData is None:
      assetGroupAndPathPricingData = {}
    if len(assetsOnPathToBaseAsset) == 0: return {}
    for subPathAssetName, assetOnPathToBaseAsset in assetsOnPathToBaseAsset.items():
      if not subPathAssetName in result:
        if subPathAssetName in assetGroupAndPathPricingData:
          pricingData = assetGroupAndPathPricingData[subPathAssetName]
        else:
          pricingData = getattr(assetOnPathToBaseAsset, GET_PRICING_DATA_IN_PERIOD_BASIC_FNC_NAME)(
                                        baseAsset = baseAsset,
                                        dividendCapsule = dividendCapsule,
                                        limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
                                        startDate = startDate,
                                        endDate = endDate,
                                        time = time)
        result[subPathAssetName] = pricingData
    if not baseAsset is None:
      __completeBasicDictionaryOfAssetTimeSeries(dictOfAssetTimeSeries=result,
                                                assets = assetsOnPathToBaseAsset,
                                                baseAsset=baseAsset,
                                                limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis) 
      if not limitToHoldingPeriodAnalysis:
        __completeDictionaryOfAssetTimeSeriesWithIsoContent(dictOfAssetTimeSeries=result,
                                                            baseAsset=baseAsset)
    return result
  setattr(assetCapsule, GET_BASIC_PRICING_DATA_IN_PERIOD_OF_ASSETS_ON_PATH_OF_BASE_ASSET_FNC_NAME,
              _capsule_base.cleanAndCloseSession(getPricingDataInPeriodOfAssetsOnPathFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # This might require pricing data far beyond the project's scope - not all assets included in project
  # def getPricingDataInPeriodOfBaseAssetGroupFnc(self: T,
  #                                               startDate: datetime.date,
  #                                               endDate: datetime.datetime, 
  #                                               dividendCapsule: V = None,
  #                                               limitToHoldingPeriodAnalysis: bool = False,
  #                                               assetGroupAndPathPricingData: dict[str, asset_time_series.AssetTimeSeries] = {},
  #                                               time: datetime.time = datetime.datetime.max.time()) -> typing.Dict[str, asset_time_series.AssetTimeSeries]:
  #   result = {}
  #   assetsOfBaseAssetGroup = getattr(self, GET_ASSETS_OF_BASE_ASSET_GROUP)(startDate = startDate,
  #                                                                         endDate = endDate)
  #   if len(assetsOfBaseAssetGroup) == 0: return {}
  #   for assetName, asset in assetsOfBaseAssetGroup.items():
  #     if not assetName in assetGroupAndPathPricingData:
  #       pricingData = getattr(asset, GET_PRICING_DATA_IN_PERIOD_BASIC_FNC_NAME)(
  #                             baseAsset = self,
  #                             startDate = startDate,
  #                             endDate = endDate,
  #                             dividendCapsule = dividendCapsule,
  #                             limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
  #                             assetGroupAndPathPricingData = assetGroupAndPathPricingData,
  #                             time = time)
  #     else:
  #       pricingData = assetGroupAndPathPricingData[assetName]
  #     result[assetName] = pricingData
  #   if not self is None:
  #     __completeBasicDictionaryOfAssetTimeSeries(dictOfAssetTimeSeries=result,
  #                                               assets = assetsOfBaseAssetGroup,
  #                                               baseAsset=self,
  #                                               dividendCapsule = dividendCapsule,
  #                                               limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
  #                                               time = time)    
  #   return result
  # setattr(assetCapsule, GET_BASIC_PRICING_DATA_IN_PERIOD_OF_BASE_ASSET_GROUP_FNC_NAME,
  #             _capsule_base.cleanAndCloseSession(getPricingDataInPeriodOfBaseAssetGroupFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getIsomorphicAssetContentInPeriodFnc(self: T,
                                          baseAsset: U,
                                          startDate: datetime.date, 
                                          endDate: datetime.date,
                                          dividendCapsule: V = None,
                                          limitToHoldingPeriodAnalysis: bool = False,
                                          assetGroupAndPathPricingData: dict[str, asset_time_series.AssetTimeSeries] = {},
                                          time: datetime.time = datetime.datetime.max.time()
                                          ) -> typing.Dict[datetime.date, decimal.Decimal]:
    isPartOfBaseAssetsIsoGroup = getattr(self, IS_PART_OF_ISOMORPHIC_GROUP_IN_PERIOD_FNC_NAME)(
                                baseAsset = baseAsset,
                                startDate = startDate,
                                endDate = endDate)
    if getattr(self, IS_ISOMORPHIC_BASE_ASSET_PROP_NAME) or not isPartOfBaseAssetsIsoGroup:
      return dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal,
                                  value = decimal.Decimal("0.0"))
    result = dt_init.initDateDict(startDate = startDate,
                                  endDate = endDate,
                                  valueType = decimal.Decimal)
    __ensureIsBaseAssetOfIsomorphicGroup(baseAsset=baseAsset,
                                      fncName=GET_CONTAINED_SHARES_OF_ISOMORPHIC_ASSET_IN_PERIOD_FNC_NAME)
    if not getattr(self, IS_PART_OF_ISOMORPHIC_GROUP_SERIES_IN_PERIOD_FNC_NAME)(
                                                baseAsset = baseAsset,
                                                startDate = startDate,
                                                endDate = endDate): return result
    preliminaryAssetGroupAndPathPricingData = getattr(self, GET_BASIC_PRICING_DATA_IN_PERIOD_OF_ASSETS_ON_PATH_OF_BASE_ASSET_FNC_NAME)(
                                                      baseAsset = baseAsset,
                                                      dividendCapsule = dividendCapsule,
                                                      limitToHoldingPeriodAnalysis = limitToHoldingPeriodAnalysis,
                                                      assetGroupAndPathPricingData = assetGroupAndPathPricingData,
                                                      startDate = startDate,
                                                      endDate = endDate,
                                                      time = time)
    if assetGroupAndPathPricingData is None: assetGroupAndPathPricingData = {}
    for key in preliminaryAssetGroupAndPathPricingData.keys():
      if not key in assetGroupAndPathPricingData:
        assetGroupAndPathPricingData[key] = preliminaryAssetGroupAndPathPricingData[key]
    containedSharesOfIsomorphicAssetInPeriod = getattr(self, GET_CONTAINED_SHARES_OF_ISOMORPHIC_ASSET_IN_PERIOD_FNC_NAME)(
                            baseAsset = baseAsset,
                            startDate = startDate,
                            endDate = endDate,
                            assetGroupAndPathPricingData = assetGroupAndPathPricingData)
    baseAssetTimeSeries = assetGroupAndPathPricingData[baseAsset.name]
    currDate = startDate

    while currDate <= endDate:
      containedSharesOfIsomorphicAssetOfDate = containedSharesOfIsomorphicAssetInPeriod[currDate]
      baseAssetPriceOfDate = baseAssetTimeSeries.price_get_on_date(date = currDate)
    result[currDate] = containedSharesOfIsomorphicAssetOfDate * baseAssetPriceOfDate
    currDate += datetime.timedelta(days = 1)
    return result
  setattr(assetCapsule, GET_ISOMORPHIC_ASSET_CONTENT_IN_PERIOD_FNC_NAME,
              _capsule_base.cleanAndCloseSession(getIsomorphicAssetContentInPeriodFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def isActiveInPeriodFnc(self: T,
                      startDate: datetime.date,
                      endDate: datetime.date) -> typing.Dict[datetime.date, bool]:
      result = dict[datetime.date, bool]()
      hasUnderlyingState = self.countOfUnderlyingStates > 0
      if hasUnderlyingState:
        firstUnderlyingState = self.getUnderlyingState(0)
        activationDate = firstUnderlyingState.timestamp.date()
      else:
        activationDate = date_utils.beginningOfTime()
      if self.expiration_date is None:
        deactivationDate = date_utils.endOfTime()
      else:
        deactivationDate = self.expiration_date.date()
      currDate = startDate
      while currDate <= endDate:
        isActive = (currDate >= activationDate) and (currDate <= deactivationDate)
        result[currDate] = isActive
        currDate += datetime.timedelta(days = 1)
      return result 
  setattr(assetCapsule, IS_ACTIVE_IN_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(isActiveInPeriodFnc))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  def isActiveOnDateFnc(self: T,
                      date: datetime.date) -> bool:
      isActiveSeries = getattr(self, IS_ACTIVE_IN_PERIOD_FNC_NAME)(
                  startDate = date,
                  endDate = date)
      return isActiveSeries[date]              
  setattr(assetCapsule, IS_ACTIVE_ON_DATE_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(isActiveOnDateFnc))

