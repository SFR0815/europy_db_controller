from __future__ import annotations

import sys, uuid
from sqlalchemy import orm as sqlalchemy_orm


sys.path.insert(0, '..\..')

from europy_db_controllers.entity_capsules import transaction
from europy_db_controllers.entity_catalog import entity_catalog
  
class TransactionType():
  def __init__(self,
               session: sqlalchemy_orm.Session,
               entityCatalog: entity_catalog.EntityCatalog,
               id: uuid.UUID = None,
               name: str = None) -> None:
    self.session = session
    self.entityCatalog = entityCatalog.basicSpecifications.transactionTypeArray
    self.entityArray = self.entityCatalog
    self._entityCapsule = transaction.TransactionTypeCapsule(
                          session = session,
                          name = name,
                          id = id)
    self.entityArray.addOrRefresh(controllerCapsule = self)
    