from __future__ import annotations

import sys, uuid, typing, datetime, \
       holidays

from dateutil import relativedelta as dateutil_rd

from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils 

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

ADJUSTED_OPENING_DATE_PROP_NAME = 'adjusted_trade_timestamp'


def addAttributes(marketTransactionCapsule: typing.Type[T]) -> None:
  pass
