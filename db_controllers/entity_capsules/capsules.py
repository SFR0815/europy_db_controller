from __future__ import annotations

import sys, types


from src.model import base_model
from src.model.basic_specification import TransactionTypeTable, CurrencyTable, CountryTable, \
                                          GermanStockExchangeTable
from src.model.asset_class import AssetClassTable, TriggeredActionTable
from src.model.asset import AssetTable, UnderlyingTable, UnderlyingStateTable, AssetPriceTable, \
                            CurrencyConversionRateTable
from src.model.client import ClientTable, ProjectTable
from src.model.transaction import MicroHedgeTable, MarketTransactionTable, FifoLotTable, \
                                  FifoTransactionTable, MarketAndForwardTransactionTable, \
                                  DividendTable
from src.model.accounts import CoreAccountTable      
                                  

from db_controllers.entity_capsules import _capsule_base, _generic_capsule_attr, _capsule_utils, \
                                            _capsule_init, _capsule_consistency, _capsule_json, \
                                            capsule_main  


from db_controllers.entity_capsules.specific_capsule_attr.basic_specification import currency as spec_currency
from db_controllers.entity_capsules.specific_capsule_attr.transactions import market_transaction as spec_market_transaction
from db_controllers.entity_capsules.specific_capsule_attr.transactions import fifo_lot as spec_fifo_lot
from db_controllers.entity_capsules.specific_capsule_attr.asset import asset as spec_asset
from db_controllers.entity_capsules.specific_capsule_attr.asset import underlying_state as spec_underlying_state



# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The list of capsule objects:
_capsule_list = []

capsule_main.setupCapsules(declarativeBase = base_model.Base,
                           capsuleList = _capsule_list,
                           callingGlobals = globals())  
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # 1. Defining basic capsule objects:
# for key, table in base_model.Base.metadata.tables.items():
#   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # a. Capsule class definition:
#   capsuleClassName = _capsule_utils.getCapsuleClassName(table)
#   capsuleBaseClass = _capsule_base.CapsuleBaseWithName if _capsule_utils.hasName(table) else \
#               _capsule_base.CapsuleBase
#   capsuleType = types.new_class(capsuleClassName, (capsuleBaseClass,), {})
#   globals()[capsuleClassName] = capsuleType
#   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # b. Setting the attribute of the type of the sqlalchemy table represented by the capsule
#   #    ('sqlalchemyTableType')
#   sqlalchemyTableTypeName = _capsule_utils.getSqlalchemyTableTypeName(table)
#   sqlalchemyTableType = globals()[sqlalchemyTableTypeName]
#   setattr(capsuleType, 'sqlalchemyTableType', globals()[sqlalchemyTableTypeName])
#   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#   # e. Log capsule object to capsule list:
#   _capsule_list.append(globals()[capsuleClassName])


# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # a. __init__ methods
# _capsule_init.addInitMethods(capsuleList=_capsule_list,
#                              callingGlobals=globals())
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # b. Add getter, setter properties and omit-if-none methods all data columns not 
# #    named 'name' or 'id' (latter with special treatment)
# _generic_capsule_attr.addDataColumnAttributes(capsuleList=_capsule_list,
#                                               callingGlobals=globals())
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # c. Add getter, setter properties and omit-if-none methods for relationships (for id, name,
# #    and the capsule object) defined by foreign key columns 
# #    (the 'one' part of 'one'-to-'many' relationships - 1-to-1 out of scope!)
# _generic_capsule_attr.addRelationshipAttributes(capsuleList=_capsule_list,
#                                                 callingGlobals=globals())
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # d. Add getter, setter properties and omit-if-none methods for relationships (for id, name,
# #    and the capsule object) defined by foreign key columns 
# #    (the 'one' part of 'one'-to-'many' relationships - 1-to-1 out of scope!)
# _capsule_consistency.addRelationshipConsistencyChecks(capsuleList=_capsule_list,
#                                                       callingGlobals=globals())
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # 3. Add attributes related to relationships defined as list of entities (for access only
# #    - no manipulations permitted)
# #    (the 'many' part of 'one'-to-'many' relationships)
# _generic_capsule_attr.addListAttributes(capsuleList=_capsule_list,
#                                                       callingGlobals=globals())
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# _capsule_json.addJsonFunctions(capsuleList = _capsule_list,
#                                 callingGlobals = globals())


# spec_fifo_lot.addAttributes(fifoLotCapsule = FifoLotCapsule)
# spec_asset.addAttributes(assetCapsule = AssetCapsule)
# spec_currency.addAttributes(currencyCapsule = CurrencyCapsule)
# spec_underlying_state.addAttributes(underlyingStateCapsule = UnderlyingStateCapsule)