import datetime, typing

def initDateDict(
        startDate: datetime.date,
        endDate: datetime.date,
        valueType: type, 
        value: any = None) -> typing.Dict[datetime.date, any]:
    result = dict[datetime.date, valueType]()
    currDate = startDate
    while currDate <= endDate:
      result[currDate] = value
      currDate += datetime.timedelta(days = 1)
    return result