import typing, types

import sqlalchemy as sqla
import sqlalchemy.orm as sqla_orm

from europy_db_controllers.entity_capsules import _capsule_base, _generic_capsule_attr, _capsule_utils, \
                                            _capsule_init, _capsule_consistency, _capsule_json


CB = typing.TypeVar("CB", bound=_capsule_base.CapsuleBase)

def setupCapsules(declarativeBase: sqla_orm.DeclarativeBase,
                  capsuleList: typing.List[CB],
                  callingGlobals: typing.Dict[str, typing.Any]):
  for key, table in declarativeBase.metadata.tables.items():
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # a. Capsule class definition:
    capsuleClassName = _capsule_utils.getCapsuleClassName(table)
    capsuleBaseClass = _capsule_base.CapsuleBaseWithName if _capsule_utils.hasName(table) else \
              _capsule_base.CapsuleBase
    capsuleType = types.new_class(capsuleClassName, (capsuleBaseClass,), {})
    callingGlobals[capsuleClassName] = capsuleType
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # b. Setting the attribute of the type of the sqlalchemy table represented by the capsule
    #    ('sqlalchemyTableType')
    sqlalchemyTableTypeName = _capsule_utils.getSqlalchemyTableTypeName(table)
    sqlalchemyTableType = callingGlobals[sqlalchemyTableTypeName]
    setattr(capsuleType, 'sqlalchemyTableType', sqlalchemyTableType)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # c. Log capsule object to capsule list:
    capsuleList.append(callingGlobals[capsuleClassName])

  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # a. __init__ methods
  _capsule_init.addInitMethods(capsuleList = capsuleList,
                              callingGlobals = callingGlobals)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # b. Add getter, setter properties and omit-if-none methods all data columns not 
  #    named 'name' or 'id' (latter with special treatment)
  _generic_capsule_attr.addDataColumnAttributes(capsuleList = capsuleList,
                                                callingGlobals = callingGlobals)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # c. Add getter, setter properties and omit-if-none methods for relationships (for id, name,
  #    and the capsule object) defined by foreign key columns 
  #    (the 'one' part of 'one'-to-'many' relationships - 1-to-1 out of scope!)
  _generic_capsule_attr.addRelationshipAttributes(capsuleList= capsuleList,
                                                  callingGlobals=callingGlobals)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # d. Add getter, setter properties and omit-if-none methods for relationships (for id, name,
  #    and the capsule object) defined by foreign key columns 
  #    (the 'one' part of 'one'-to-'many' relationships - 1-to-1 out of scope!)
  _capsule_consistency.addRelationshipConsistencyChecks(capsuleList = capsuleList,
                                                        callingGlobals = callingGlobals)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # 3. Add attributes related to relationships defined as list of entities (for access only
  #    - no manipulations permitted)
  #    (the 'many' part of 'one'-to-'many' relationships)
  _generic_capsule_attr.addListAttributes(capsuleList = capsuleList,
                                          callingGlobals = callingGlobals)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  _capsule_json.addJsonFunctions(capsuleList = capsuleList,
                                 callingGlobals = callingGlobals)  