import typing, datetime

import sqlalchemy.orm as sqlalchemy_orm

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base

from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import constants, consistency_checks

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# initialize the capsule here (before handling the [possibly] provided 
#     dict definitions of the capsule's relationships
#     Justification: In case of reading a dict without Ids -
#                    If the name of a relationship is provided in the
#                    main capsule's dict, the relationship's capsule must 
#                    be identified. 
#                    Otherwise, the relationship's capsule will go without
#                    id and violate consistency constraints. 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def getInitParameters(session: sqlalchemy_orm.Session,
                      columnsAndAlikeInfo: typing.Dict[str, typing.Tuple[str, bool, bool]],
                      capsuleDict: typing.Dict[str, any]):
  result = {"session": session}
  for column_name, column_info in columnsAndAlikeInfo.items():
    isRelationshipColumn = _capsule_utils.isRelationshipIdColumnName(columnName = column_name)
    # all non-relationship informational items are defined first and the capsule gets 
    # initialized.
    # This leaves out all relationships within the first step of capsule initialization.
    # Please note the following: This might lead to conflicts with possible validations on 
    #                            some of these non-relationship values that recur onto either 
    #                            the existence and/or certain features of such relationships.
    if not isRelationshipColumn:
      columnName = column_name
      if '_timestamp' in column_name:
        value = capsuleDict[columnName]
        if isinstance(value, str):
          value = datetime.datetime.strptime(value, constants.DATETIME_FORMAT)
          result[columnName] = value
      else:
        result[columnName] = capsuleDict[columnName]  
  return result

def getInitializedBasicCapsule(session: sqlalchemy_orm.Session,
                               capsuleType: typing.Type[T],
                               columnsAndAlikeInfo: typing.Dict[str, typing.Tuple[str, bool, bool]],
                               capsuleDict: typing.Dict[str, any],
                               persistentMustHaveId: bool) -> typing.Tuple[T, bool]:
  initParameters = getInitParameters(session = session,
                                     columnsAndAlikeInfo = columnsAndAlikeInfo,
                                     capsuleDict = capsuleDict)
  result = capsuleType(**initParameters)
  # add to session if not already in session
  #   DO NOT invoke addToSession() here but add the sqlalchemyTable to the session
  #   This is done to avoid the runAtAddToSession() to be called here (this it NOT the complete entity that can be flushed)
  is_not_in_session = not result.isInSession
  if is_not_in_session:
    session.add(result.sqlalchemyTable)
  # Test if entity has an id that is not provided by the input dict
  consistency_checks.ensureNoIdProvidedForExistingEntity(entity = result,
                                                         capsuleDict = capsuleDict,
                                                         persistentMustHaveId = persistentMustHaveId)
  return result, is_not_in_session

