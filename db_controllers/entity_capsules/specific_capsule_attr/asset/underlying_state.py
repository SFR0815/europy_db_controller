from __future__ import annotations

import sys, uuid, typing, datetime, \
       decimal

from dateutil import relativedelta as dateutil_rd

from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from src.utils import date_utils

from db_controllers.entity_capsules.specific_capsule_attr.basic_specification import currency as spec_currency
from db_controllers.entity_capsules import _capsule_base, _capsule_utils 
from src.process_data.utils import time_series
from src.process_data.utils.specific_time_series import asset_time_series
from db_controllers.entity_capsules.specific_capsule_attr.utils import state_control

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)

UNIT_BASKET_VOLUME_KEY = "volume"

IS_COMPOSED_BY_PERCENTAGE_PROP_NAME = 'is_composed_by_percentage'

GET_UNIT_BASKET_COMPOSITION_FNC_NAME = 'getUnitBasketComposition'

def addAttributes(underlyingStateCapsule: typing.Type[T]) -> None:
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def isComposedByPercentageFnc(self: T) -> bool:
    result = None
    for underlying in self.underlyings:
      if underlying.participation_percentage is None and \
            underlying.units is None:
        raise Exception(f"Badly specified underlyings in underlying state of asset '{self.parent_asset.name}'.\n" + \
                        f"Underlying state timestamp: {self.timestamp}.\n" + \
                        f"Neither 'units' or 'participation_percentage' defined on underlying asset '{underlying.asset.name}'")
      elif not (underlying.participation_percentage is None) and \
            not (underlying.units is None):
        raise Exception(f"Badly specified underlyings in underlying state of asset '{self.parent_asset.name}'.\n" + \
                        f"Underlying state timestamp: {self.timestamp}.\n" + \
                        f"Both 'units' and 'participation_percentage' defined on underlying asset '{underlying.asset.name}'")
      isComposedByPercentage = not (underlying.participation_percentage is None)
      if not result is None:
        if not result == isComposedByPercentage:
          raise Exception(f"Badly specified underlying state of asset '{self.parent_asset.name}'.\n" + \
                          f"Underlying state timestamp: {self.timestamp}.\n" + \
                          f"Some underlyings defined with 'participation_percentage' and others with 'units'")
      result = isComposedByPercentage
    return result
  setattr(underlyingStateCapsule, IS_COMPOSED_BY_PERCENTAGE_PROP_NAME, 
              property(_capsule_base.cleanAndCloseSession(isComposedByPercentageFnc)))
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getUnitBasketCompositionFnc(self: T,
                             startDate: datetime.date,
                             endDate: datetime.date,
                             time: datetime.time = datetime.datetime.max.time()) -> typing.Dict[datetime.date, typing.Dict[str, decimal.Decimal]]:
    if getattr(self, IS_COMPOSED_BY_PERCENTAGE_PROP_NAME): return None
    parentAsset = self.parent_asset
    parentAssetPriceCapsules = parentAsset.getPriceCapsuleInPeriod(startDate = startDate,
                                                              endDate = endDate,
                                                              time = time,
                                                              useLastIfNotAvailable = True)
    result = dict[datetime.date, dict[str, decimal.Decimal]]()
    for underlying in self.underlyings:
      underlyingAsset = underlying.asset
      basketItemPriceCapsules = underlyingAsset.getPriceCapsuleInPeriod(startDate = startDate,
                                                              endDate = endDate,
                                                              time = time,
                                                              useLastIfNotAvailable = True)
      currencyPartitions = state_control.getFxPartitions(startDate = startDate,
                                                         endDate = endDate,
                                                         basketItemPriceCapsules = basketItemPriceCapsules,
                                                         parentPriceCapsules = parentAssetPriceCapsules)
      fxRates = dict[datetime.date, decimal.Decimal]()
      for currencyPartition in currencyPartitions:
        # print(f"partition boundaries: {currencyPartition.startDate} - {currencyPartition.endDate}")
        partitionStartDate = currencyPartition.startDate
        partitionEndDate = currencyPartition.endDate
        basketItemPriceCapsule = basketItemPriceCapsules[partitionStartDate]
        parentAssetPriceCapsule = parentAssetPriceCapsules[partitionStartDate]
        basketItemCurrency = None if basketItemPriceCapsule is None else basketItemPriceCapsules[partitionStartDate].currency
        parentAssetCurrency = None if parentAssetPriceCapsule is None else parentAssetPriceCapsules[partitionStartDate].currency
        if basketItemCurrency is None or parentAssetCurrency is None:
          currDate = partitionStartDate
          while currDate <= partitionEndDate:
            fxRates[currDate] = None
            currDate += datetime.timedelta(days = 1)
        else:
          partitionFxRates = getattr(basketItemCurrency, spec_currency.GET_FX_RATE_IN_PERIOD_FNC_NAME)(
                          baseCurrencyName = parentAssetCurrency.name,
                          startDate = partitionStartDate,
                          endDate = partitionEndDate,
                          useLastIfNotAvailable = True)
          for date, value in partitionFxRates.items():
            fxRates[date] = value

      count = 0

      currDate = startDate
      while currDate <= endDate:

        doPrint = currDate == datetime.date(2024, 3, 27)

        if not currDate in result: result[currDate] = {}
        basketItemPriceCapsule = basketItemPriceCapsules[currDate]
        parentAssetPriceCapsule = parentAssetPriceCapsules[currDate]
        fxRate = fxRates[currDate]
        if fxRate is None or basketItemPriceCapsule is None:
          underlyingAssetVolume = None
        else:

          if doPrint:        
            print(f"underlying asset: {underlyingAsset.name} - underlyingAsset currency: {basketItemPriceCapsule.currency.name}")
            print(f"                                           parent asset    currency: {parentAssetPriceCapsule.currency.name}")
            print(f"underlying asset: {underlyingAsset.name} - fxRate: {fxRate} " + \
                            f"- price: {basketItemPriceCapsule.price} - units: {underlying.units}")

          underlyingAssetVolume = fxRate * decimal.Decimal(basketItemPriceCapsule.price) * \
                                           decimal.Decimal(underlying.units)
          
          if doPrint: print(f"   result: {underlyingAssetVolume}")

        if UNIT_BASKET_VOLUME_KEY in result[currDate]:
          basketVolume = result[currDate][UNIT_BASKET_VOLUME_KEY]
        else:
          basketVolume = decimal.Decimal("0.0")
        if not basketVolume is None:
          if parentAssetPriceCapsule is None or underlyingAssetVolume is None:
            basketVolume = None
          else:
            basketVolume = basketVolume + underlyingAssetVolume
        if doPrint: print(f"   basketVolume: {basketVolume}")
        result[currDate][underlyingAsset.name] = underlyingAssetVolume
        result[currDate][UNIT_BASKET_VOLUME_KEY] = basketVolume
        
        count += 1

        currDate += datetime.timedelta(days = 1)

    for underlying in self.underlyings:
      underlyingAsset = underlying.asset
      currDate = startDate
      while currDate <= endDate:
        basketVolume = result[currDate][UNIT_BASKET_VOLUME_KEY]
        if not basketVolume is None:
           result[currDate][underlyingAsset.name]  = result[currDate][underlyingAsset.name] / basketVolume 
        currDate += datetime.timedelta(days = 1)
    return result
  setattr(underlyingStateCapsule, GET_UNIT_BASKET_COMPOSITION_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getUnitBasketCompositionFnc)) 


