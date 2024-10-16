from __future__ import annotations

import sys, uuid, typing, datetime, \
       holidays, decimal

from dateutil import relativedelta as dateutil_rd

from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy

from src.utils import date_utils
from db_controllers.entity_capsules import _capsule_base, _capsule_utils 

from src.process_data.utils.specific_time_series import asset_time_series, lot_time_series

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

GET_HOLDING_PERIOD_PROP_NAME = 'holding_period'

IS_HELD_AT_DATE_FNC_NAME = 'isHeldAtDate'
IS_HELD_FOR_YEAR_BEFORE_DATE_FNC_NAME = 'isHeldForYearBeforeDate'
IS_OPEN_IN_PERIOD = 'isOpenInPeriod'
HAS_COMPLIANT_MINIMUM_HOLDING_PERIOD_FNC_NAME = 'hasCompliantMinimumHoldingPeriod'

GET_FULL_MINIMUM_HOLDING_PERIOD_FNC_NAME = 'getFullMinimumHoldingPeriod'
GET_MINIMUM_HOLDING_PERIOD_FNC_NAME = 'getMinimumHoldingPeriod'
GET_QUANTITIES_TRADED_FNC_NAME = 'quantitiesTraded'
GET_QUANTITIES_HELD_FNC_NAME = 'quantitiesHeld'
GET_QUANTITY_HELD_AT_DATE_FNC_NAME = 'quantityHeldAtDate'
GET_MINIMUM_HOLDING_PERIOD_AND_HOLDINGS_AT_END_FNC_NAME  = 'getMinimumHoldingPeriodAndHoldingsAtEdn'
GET_HOLDINGS = 'getHoldingsDict'

GET_LOT_HOLDINGS_DATA = 'getLotHoldingsData'

PERIOD_START_KEY = 'start'
PERIOD_END_KEY = 'end'

def addAttributes(fifoLotCapsule: typing.Type[T]) -> None:
  def getHoldingPeriodFnc(self: T) -> date_utils.DayPeriod:
    return date_utils.DayPeriod(start = self.holding_period_start,
                                end = self.holding_period_end)
  setattr(fifoLotCapsule, GET_HOLDING_PERIOD_PROP_NAME, 
              property(_capsule_base.cleanAndCloseSession(getHoldingPeriodFnc)))
  # Comment: The holding period already treats any public holidays and weekends that 
  #          might follow the 'opening_timestamp' (start) or precede (end) the 
  #          'closing_timestamp'. (Both already adjusted for possible forward trades)
  #          Thus, any weekend or public holiday that might constitute either end of
  #          a 45 (or other) days period around some date within the holding period
  #          is already dismissed when intersecting with the holding period, see below.
  # -> §108 Abs.3 AO is applied to the definition of the holding period and, thus, 
  #    also implicitly applies to the (intersected) minimum holding period (without
  #    actually relying on the regulation while determining it). Rz. 8 is thus deemed 
  #    dealt with. 

  def isHeldAtDateFnc(self: T,
                      date: datetime.date) -> bool:
    holdingPeriod = getattr(self, GET_HOLDING_PERIOD_PROP_NAME)
    return holdingPeriod.containsDate(date = date)
  setattr(fifoLotCapsule, IS_HELD_AT_DATE_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(isHeldAtDateFnc))

  def isHeldForYearBeforeDateFnc(self: T,
                                 date: datetime.date) -> bool:
    if self.is_short: return False
    isHeldAtDate = getattr(self, IS_HELD_AT_DATE_FNC_NAME)(date = date)
    if not isHeldAtDate: return False
    oneYearAgo = date - dateutil_rd.relativedelta(years = 1)
    return self.holding_period_start <= oneYearAgo 
  setattr(fifoLotCapsule, IS_HELD_FOR_YEAR_BEFORE_DATE_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(isHeldForYearBeforeDateFnc))

  def isHeldInPeriod(self: T, 
                     startDate: datetime.date,
                     endDate: datetime.date) -> bool:
    return startDate <= self.holding_period_end and endDate >= self.holding_period_start
  setattr(fifoLotCapsule, IS_OPEN_IN_PERIOD, isHeldInPeriod)

  def getFullMinimumHoldingPeriodFnc(self: T,
                                              date: datetime.date,
                                              numberOfDaysBeforeAndAfter: int = 45) -> date_utils.DayPeriod:
    if self.is_short: return None
    if not getattr(self, IS_HELD_AT_DATE_FNC_NAME)(date = date): return None
    periodAroundDate = date_utils.getPeriodAroundDay(
                                      date = date,
                                      dayShift = numberOfDaysBeforeAndAfter)
    return getattr(self, GET_HOLDING_PERIOD_PROP_NAME).overlap(periodAroundDate)
  setattr(fifoLotCapsule, GET_FULL_MINIMUM_HOLDING_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getFullMinimumHoldingPeriodFnc))

  def getMinimumHoldingPeriodFnc(self: T,
                                 date: datetime.date,
                                 numberOfDaysBeforeAndAfter: int = 45) -> date_utils.DayPeriod:
    if self.is_short: return None
    holdingPeriodAround = getattr(self, GET_FULL_MINIMUM_HOLDING_PERIOD_FNC_NAME)(
                                        numberOfDaysBeforeAndAfter = numberOfDaysBeforeAndAfter,
                                        date = date)
    if holdingPeriodAround is None: return None
    return holdingPeriodAround.getDaysBeforeOrAround(date = date, numberOfDays = numberOfDaysBeforeAndAfter)
  setattr(fifoLotCapsule, GET_MINIMUM_HOLDING_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getMinimumHoldingPeriodFnc))

  def hasCompliantMinimumHoldingPeriodFnc(self: T,
                                          date: datetime.date,
                                          numberOfDaysBeforeAndAfter: int = 45
                                          ) -> bool: 
    if self.is_short: return False
    minimumHoldingPeriod = getattr(self, GET_MINIMUM_HOLDING_PERIOD_FNC_NAME)(
                                        date = date,
                                        numberOfDaysBeforeAndAfter = numberOfDaysBeforeAndAfter)
    if minimumHoldingPeriod is None: return False
    return minimumHoldingPeriod.numberOfDays >= numberOfDaysBeforeAndAfter
  setattr(fifoLotCapsule, HAS_COMPLIANT_MINIMUM_HOLDING_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(hasCompliantMinimumHoldingPeriodFnc))

  def getQuantitiesTradedFnc(self: T) -> typing.Dict[datetime.date, typing.Tuple[decimal.Decimal, bool]]:
    tradedQuantity = dict[datetime.date, decimal.Decimal]()
    firstTradeDate: datetime.date = None
    lastTradeDate: datetime.date = None
    for fifoTransaction in self.fifo_transactions:
      thisTxDate = fifoTransaction.adjusted_trade_timestamp.date()
      if firstTradeDate is None: firstTradeDate = thisTxDate
      lastTradeDate = thisTxDate
      thisTxTradedQuantity = decimal.Decimal(str(fifoTransaction.traded_quantity))
      previousTradedQuantity = tradedQuantity[thisTxDate] if thisTxDate in tradedQuantity \
                                                          else decimal.Decimal("0.0")
      tradedQuantity[thisTxDate] = thisTxTradedQuantity + previousTradedQuantity
    result = dict[datetime.date, typing.Tuple[decimal.Decimal, bool]]()
    if firstTradeDate is None: 
      raise Exception("Lot without transactions ....")
    currDate = firstTradeDate
    while currDate <= lastTradeDate:
      thisDateTradedSQuantity = (tradedQuantity[currDate], True) if currDate in tradedQuantity \
                                                                 else (decimal.Decimal("0.0"), False)
      result[currDate] = thisDateTradedSQuantity
      currDate += datetime.timedelta(days=1)
    return result    
  setattr(fifoLotCapsule, GET_QUANTITIES_TRADED_FNC_NAME, getQuantitiesTradedFnc)

  def getQuantitiesHeldFnc(self: T) -> typing.Dict[datetime.date, typing.Tuple[decimal.Decimal, bool]]:
    result = dict[datetime.date, typing.Tuple[decimal.Decimal, bool]]()
    quantitiesTraded = getattr(self, GET_QUANTITIES_TRADED_FNC_NAME)()
    thisHoldings = decimal.Decimal("0.0")
    for date, quantityTraded in quantitiesTraded.items():
      thisHoldings = thisHoldings + quantityTraded[0]
      result[date] = (thisHoldings, quantityTraded[1])
    return result    
  setattr(fifoLotCapsule, GET_QUANTITIES_HELD_FNC_NAME, getQuantitiesHeldFnc)


  def getQuantityHeldAtDateFnc(self: T,
                               date: datetime.date) -> decimal.Decimal:
    quantitiesHeld = getattr(self, GET_QUANTITIES_HELD_FNC_NAME)()
    return quantitiesHeld[date][0] if date in quantitiesHeld else decimal.Decimal("0.0")
  setattr(fifoLotCapsule, GET_QUANTITY_HELD_AT_DATE_FNC_NAME, getQuantityHeldAtDateFnc)

  def getMinimumHoldingAndHoldingsAtEndFnc(self: T,
                                          date: datetime.date,
                                          numberOfDaysBeforeAndAfter: int = 45
                                          ) -> typing.Tuple[date_utils.DayPeriod, int]:
    if getattr(self, HAS_COMPLIANT_MINIMUM_HOLDING_PERIOD_FNC_NAME)(
                            date = date,
                            numberOfDaysBeforeAndAfter = numberOfDaysBeforeAndAfter):
      minimumHoldingPeriod = getattr(self, GET_MINIMUM_HOLDING_PERIOD_FNC_NAME)(
                            date = date,
                            numberOfDaysBeforeAndAfter = numberOfDaysBeforeAndAfter)
      holdingsAtEnd = getattr(self, GET_QUANTITY_HELD_AT_DATE_FNC_NAME)(
                            date = minimumHoldingPeriod.end)
      return(minimumHoldingPeriod, holdingsAtEnd)
    return None
  setattr(fifoLotCapsule, GET_MINIMUM_HOLDING_PERIOD_AND_HOLDINGS_AT_END_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getMinimumHoldingAndHoldingsAtEndFnc))  

  def getHoldingsFnc(self: T,
                     startDate: datetime.date,
                     endDate: datetime.date) -> typing.Dict[datetime.date, typing.Tuple[decimal.Decimal, bool]]:
    currDate = startDate
    result = dict[datetime.date, typing.Tuple[decimal.Decimal, bool]]()
    lotHoldings = getattr(self, GET_QUANTITIES_HELD_FNC_NAME)()
    lotHoldingsDates = list(lotHoldings.keys())
    lotStartDate = min(lotHoldingsDates)
    lotEndDate = max(lotHoldingsDates)
    previousHoldings = decimal.Decimal("0.0")
    while currDate <= endDate:
      if currDate < lotStartDate:
        result[currDate] = (decimal.Decimal("0.0"), False)
      if currDate >= lotStartDate and currDate <= lotEndDate:
        if currDate in lotHoldings:
          lotHoldingsEntry = lotHoldings[currDate]
          result[currDate] = lotHoldingsEntry
          previousHoldings = lotHoldingsEntry[0]
        else:
          result[currDate] = (previousHoldings, False)
      else:
        result[currDate] = (previousHoldings, False)
      currDate += datetime.timedelta(days = 1)
    return result
  setattr(fifoLotCapsule, GET_HOLDINGS, 
              _capsule_base.cleanAndCloseSession(getHoldingsFnc))  
  

  def getLotHoldingsData(self: T,
                         assetHoldingData: asset_time_series.AssetTimeSeries) -> lot_time_series.LotTimeSeries:
    result = lot_time_series.LotTimeSeries(assetTimeSeries=assetHoldingData)
    lotHoldings = getattr(self, GET_HOLDINGS)(startDate = assetHoldingData.startDate,
                                              endDate = assetHoldingData.endDate)
    lotHoldings = getattr(self, GET_QUANTITIES_HELD_FNC_NAME)()
    lotHoldingsDates = list(lotHoldings.keys())
    lotStartDate = min(lotHoldingsDates)
    lotEndDate = max(lotHoldingsDates)
    previousHoldings = decimal.Decimal("0.0")
    lotHoldingPeriod = getattr(self, GET_HOLDING_PERIOD_PROP_NAME)
    result.lotHoldingPeriodStartDate = lotHoldingPeriod.start  
    currDate = assetHoldingData.startDate
    while currDate <= assetHoldingData.endDate:
      holdings = decimal.Decimal("0.0")
      hasTrade = False
      if currDate < lotStartDate:
        pass
      if currDate >= lotStartDate and currDate <= lotEndDate:
        if currDate in lotHoldings:
          lotHoldingsEntry = lotHoldings[currDate]
          holdings = lotHoldingsEntry[0]
          hasTrade = lotHoldingsEntry[1]
          previousHoldings = holdings
        else:
          holdings = previousHoldings
      else:
        holdings = previousHoldings
      result.lot_holdings_set_on_date(date = currDate, 
                                      value = holdings)
      result.lot_has_trade_set_on_date(date = currDate, 
                                      value = hasTrade)
      result.lot_number_set_on_date(date = currDate, 
                                    value = self.number)
      result.lot_id_set_on_date(date = currDate, 
                                value = self.id)
      result.lot_is_long_set_on_date(date = currDate, 
                                    value = self.is_long)
      if self.asset.asset_class.is_dividend_bearing and self.is_long:
        dateIsInHoldingPeriod = lotHoldingPeriod.containsDate(date = currDate)
        result._holdingPeriodTimeSeries.lot_holding_period_active_set_on_date(date = currDate,
                                                     value = dateIsInHoldingPeriod)
      currDate += datetime.timedelta(days = 1)
    return result
  setattr(fifoLotCapsule, GET_LOT_HOLDINGS_DATA, 
              _capsule_base.cleanAndCloseSession(getLotHoldingsData))  


  

