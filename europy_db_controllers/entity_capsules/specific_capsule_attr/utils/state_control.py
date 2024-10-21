from __future__ import annotations

import typing, sys, datetime

sys.path.insert(0, '..\..\..')

from europy_db_controllers.entity_capsules import _capsule_base 

U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)

class PriceFxStateLog():
  def __init__(self, 
               startDate: datetime.date,
               endDate: datetime.date,
               basketItemCurrencyName: str,
               parentCurrencyName: str) -> None:
    self.startDate = startDate
    self.endDate = endDate
    self.basketItemCurrencyName = basketItemCurrencyName
    self.parentCurrencyName = parentCurrencyName
    pass
  @property
  def isSpecified(self) -> bool:
    return not (self.basketItemCurrencyName is None and self.parentCurrencyName is None)
  def hasSameCurrencies(self, comparedTo: PriceFxStateLog) -> bool:
    return self.basketItemCurrencyName == comparedTo.basketItemCurrencyName and \
            self.parentCurrencyName == comparedTo.parentCurrencyName

  def __repr__(self) -> str:
    return f"[PriceFxStateLog] startDate: {self.startDate}, endDate: {self.endDate}, " + \
           f"basketItemCurrencyName: {self.basketItemCurrencyName}, parentCurrencyName: {self.parentCurrencyName}"

class PriceFxState():
  @classmethod
  def getCurrencyName(self, priceCapsule: U) -> str:
    return None if priceCapsule is None else priceCapsule.currency.name
  @classmethod
  def getId(self, basketItemCurrencyName, parentCurrencyName) -> str:
    return ("" if basketItemCurrencyName is None else basketItemCurrencyName) + '.' + \
            ("" if parentCurrencyName is None else parentCurrencyName)
  def __init__(self, 
                basketItemPriceCapsule: U, 
                parentPriceCapsule: U,
                dateOfState: datetime.date) -> None:
    self.basketItemCurrencyName = self.__class__.getCurrencyName(basketItemPriceCapsule)
    self.parentCurrencyName =  self.__class__.getCurrencyName(parentPriceCapsule)
    self.dateOfState = dateOfState
  @property
  def id(self) -> str: return self.__class__.getId(self.basketItemCurrencyName, self.parentCurrencyName)
  def requiresUpdate(self, basketItemPriceCapsule: U, parentPriceCapsule: U) -> bool:
    return self.id != self.__class__.getId(self.__class__.getCurrencyName(basketItemPriceCapsule),
                                            self.__class__.getCurrencyName(parentPriceCapsule))
  def update(self, 
             basketItemPriceCapsule: U, 
             parentPriceCapsule: U,
             dateOfState: datetime.date) -> None:
    self.basketItemCurrencyName = self.__class__.getCurrencyName(basketItemPriceCapsule)
    self.parentCurrencyName = self.__class__.getCurrencyName(parentPriceCapsule)
    self.dateOfState = dateOfState
  def logAndUpdate(self, 
             basketItemPriceCapsule: U, 
             parentPriceCapsule: U,
             currDate: datetime.date) -> PriceFxStateLog:
    result = PriceFxStateLog(startDate = self.dateOfState, 
                             endDate = currDate - datetime.timedelta(days = 1), 
                             basketItemCurrencyName = self.basketItemCurrencyName,
                             parentCurrencyName = self.parentCurrencyName)
    self.update(basketItemPriceCapsule = basketItemPriceCapsule,
                parentPriceCapsule = parentPriceCapsule,
                dateOfState = currDate)
    return result
  
def getFxPartitions(startDate: datetime.date,
                    endDate: datetime.date,
                    parentPriceCapsules: typing.Dict[datetime.date, U],
                    basketItemPriceCapsules: typing.Dict[datetime.date, U]) -> typing.List[PriceFxStateLog]:
  currPriceFxState: PriceFxState = None
  currBasketItemPriceCapsule: U = None
  currParentPriceCapsule: U = None
  currDate: datetime.date = startDate
  result = []
  def recordPartition() -> None:
    nonlocal currPriceFxState, currDate, \
              currBasketItemPriceCapsule, currParentPriceCapsule
    resultItem = currPriceFxState.logAndUpdate(
                          basketItemPriceCapsule = currBasketItemPriceCapsule,
                          parentPriceCapsule = currParentPriceCapsule,
                          currDate = currDate)
    result.append(resultItem)
  def recordLastPartition() -> None:
    nonlocal currPriceFxState
    resultItem = PriceFxStateLog(
                        startDate = currPriceFxState.dateOfState,
                        endDate = endDate,
                        basketItemCurrencyName = currPriceFxState.basketItemCurrencyName,
                        parentCurrencyName = currPriceFxState.parentCurrencyName)
    result.append(resultItem)
  while currDate <= endDate:
    # print(f"currDate: {currDate}")
    currBasketItemPriceCapsule = basketItemPriceCapsules[currDate]
    currParentPriceCapsule = parentPriceCapsules[currDate]
    if currDate == startDate: 
      currPriceFxState = PriceFxState(currParentPriceCapsule, currBasketItemPriceCapsule, currDate)
      if startDate == endDate: 
        recordLastPartition()
    elif currDate == endDate:  
      recordLastPartition() 
      break
    else:
      if currPriceFxState.requiresUpdate(basketItemPriceCapsule = currBasketItemPriceCapsule,
                                          parentPriceCapsule = currParentPriceCapsule): recordPartition()
    currDate += datetime.timedelta(days = 1)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Exit if one (or no) PriceFxStateLog present in result
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  if len(result) <= 1: return result  
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Reduce partitions if all identified fx rates are the same
  # 'None' type currencies are omitted.
  #   -> Holes in available pricing data are omitted
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  homogeneFxStateLog = PriceFxStateLog(
                          startDate = startDate,
                          endDate = endDate,
                          basketItemCurrencyName = None,
                          parentCurrencyName = None)
  for fxPartition in result:
    if fxPartition.isSpecified:
      if not homogeneFxStateLog.isSpecified:
        homogeneFxStateLog.startDate = fxPartition.startDate
        homogeneFxStateLog.basketItemCurrencyName = fxPartition.basketItemCurrencyName
        homogeneFxStateLog.parentCurrencyName = fxPartition.parentCurrencyName
      else:
        if not homogeneFxStateLog.hasSameCurrencies(comparedTo = fxPartition):
          return result # exit with originally specified result
  return [homogeneFxStateLog] # exit with single 
