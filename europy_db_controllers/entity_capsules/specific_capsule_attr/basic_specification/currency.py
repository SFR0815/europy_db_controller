from __future__ import annotations

import sys, uuid, typing, datetime, \
       decimal

from dateutil import relativedelta as dateutil_rd

from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from src.utils import date_utils
from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils 
from src.process_data.utils import time_series
from src.process_data.utils.specific_time_series import asset_time_series

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)

GET_FX_RATE_IN_PERIOD_FNC_NAME = 'getFxRateInPeriod'

def addAttributes(currencyCapsule: typing.Type[T]) -> None:
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  def getFxRateInPeriodFnc(self: T,
                           baseCurrencyName: str,
                           startDate: datetime.date,
                           endDate: datetime.date,
                           time: datetime.time = datetime.datetime.max.time(),
                           useLastIfNotAvailable: bool = False) -> typing.Dict[datetime.date, U]:
    # **************************************  
    def getRelevantFxRatePos(fxRates: list[U],
                            date: datetime.date,
                            startPos: int = 0) -> int:
      result = -1
      for pos in range(max(startPos, 0), len(fxRates)):
        thisFxRate = fxRates[pos]
        lookupTimestamp = datetime.datetime.combine(date, time)
        # print(f"       pos: {pos} - thisFxRate.timestamp: {thisFxRate.timestamp} - lookupTimestamp: {lookupTimestamp} - condition: {thisFxRate.timestamp < lookupTimestamp}")
        if thisFxRate.timestamp < lookupTimestamp:
          result = pos
        else: return result
      return result
    # **************************************  
    def getFxRatesOfBaseCurrency() -> list[U]:
      allFxRates = list(self.fx_rates)
      result = []
      for fxRate in allFxRates:
        if fxRate.numerator_currency.name == baseCurrencyName:
          result.append(fxRate)
      return result
    # **************************************  
    # print(f"   [getFxRateInPeriodFnc] currency: {self.name} - startDate: {startDate} - " + \
    #       f"endDate: {endDate}")
    result = dict[datetime.date, U]()
    currDate = startDate
    fxRates = getFxRatesOfBaseCurrency()
    # print(f"   [getFxRateInPeriodFnc] currency: {self.name} - len fx rates: {len(fxRates)}")
    currDateFxRatePos = getRelevantFxRatePos(fxRates, currDate)
    # print(f"   [getFxRateInPeriodFnc] currency: {self.name} - baseCurrencyName: {baseCurrencyName} - " + \
    #       f"currDate: {currDate} - currDateFxRatePos: {currDateFxRatePos}")
    if currDateFxRatePos == -1:
      thisFxRate = None
      nextFxRate = fxRates[0] if len(fxRates) > 0 else None
    else:
      thisFxRate = fxRates[currDateFxRatePos]
      nextFxRate = fxRates[currDateFxRatePos + 1] if len(fxRates) > currDateFxRatePos + 2 else None
    while currDate <= endDate:
      lookupTimestamp = datetime.datetime.combine(currDate, time)
      # print(f"nextFxRate is None : {nextFxRate is None}")
      if not nextFxRate is None:
        if nextFxRate.timestamp < lookupTimestamp:
          currDateFxRatePos = getRelevantFxRatePos(fxRates, currDate, currDateFxRatePos)
          thisFxRate = fxRates[currDateFxRatePos]
          nextFxRate = fxRates[currDateFxRatePos + 1] if len(fxRates) > currDateFxRatePos + 2 else None
      # print(f"\n   currDate: {currDate} - currDateFxRatePos: {currDateFxRatePos}")
      # print(f"   thisFxRate: {thisFxRate if thisFxRate is None else thisFxRate.sqlalchemyTable}")
      # print(f"   nextFxRate: {nextFxRate if nextFxRate is None else nextFxRate.sqlalchemyTable}")
      resultValue = None  
      if not thisFxRate is None:
        if useLastIfNotAvailable:
          resultValue = thisFxRate
        else:
          if thisFxRate.timestamp.date() == currDate:
            resultValue = thisFxRate
      # else:
      #   raise Exception(f"\nNo Fx rate available from {self.name} (denominator) to {baseCurrencyName} (nominator) on date {currDate} - please check fx inputs.")
      result[currDate] = None if resultValue is None \
                              else decimal.Decimal(str(resultValue.rate))
      # print(f"   adding date to time series: {dateString} - period: [{startDate} to {endDate}]")
      currDate += datetime.timedelta(days = 1)
    return result
  setattr(currencyCapsule, GET_FX_RATE_IN_PERIOD_FNC_NAME, 
              _capsule_base.cleanAndCloseSession(getFxRateInPeriodFnc)) 
  
