from __future__ import annotations

import sys

sys.path.insert(0, '..\..')

from db_controllers.entity_catalog import entity_arrays

class BasicSpecificationsData():
  # Basic system specification data (shared by all rarely changed/added - mainly setup)
  #   Not used for overwrite but for completion only
  #   Deletion of entities is left for manual sql - for now
  def __init__(self) -> None:
    # FIXME self.currencyArray
    self.transactionTypeArray = entity_arrays.TransactionTypeCapsuleArray() 
    # FIXME self.assetClassArray: BS_ACA = bs_aca(session = session)

class SharedData():
  # Data shared across all projects (regular add-ons)
  #   Not used for overwrite but for completion only
  #   Deletion of entities is left for manual sql - for now
  def __init__(self) -> None:
    # FIXME self.assets
    # FIXME self.dividendEvents
    pass

class entityCatalog():
  def __init__(self) -> None:
    self.basicSpecifications = BasicSpecificationsData()
    self.sharedData = SharedData()
    # Project specific data goes here:
    # FIXME self.transactions
    # FIXME self.dividendTransactions