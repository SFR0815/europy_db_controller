import typing, datetime, sys, uuid, enum, sqlalchemy, json

from sqlalchemy.ext import declarative as sqlalchemy_decl


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils, _capsule_shared

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
# b. Function ensuring the id of the relationship's entity being consistent between
#       - the relationship entity's id available as and attribute on the capsule's 
#         sqlalchemyTable 
#         (<capsule>.sqlalchemyTable.<relationshipIdAttr>) AND
#       - the relationship entity's id available on the sqlalchemyTable of 
#         the relationship's entity 
#         (<capsule>.sqlalchemyTable.<relationshipName>.id)
def ensureConsistentRelationshipId(capsule: T,
                                     dictAttributeNamingConventions: dict[str, str]):
  relationshipIdAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Raise Exception if the sqlalchemyTable of the capsule does not have
  #    an attribute providing the relationship's id (see above #b)
  _capsule_shared._raiseExceptionIfNoRelationshipIdOnCapsuleSqlaTable(capsule = capsule,
                                                       dictAttributeNamingConventions = dictAttributeNamingConventions) 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify the sqlalchemyTable of the relationship's entity
  
  try:
    with capsule.session.no_autoflush:
      relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName)
  except:
    msg = f"relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName) let to Exception\n" + \
          f" - trying to source it based on name. sqlalchemyTable: {capsule.sqlalchemyTable.__class__.__name__}," + \
          f" relationshipName: {relationshipName}"
    raise Exception(msg)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Exit if the sqlalchemyTable of the relationship's entity is not defined yet
  if relationshipSqlaTable is None: 
    # do nothing if no property sqlalchemyTable
    return 
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify 
  #    - the relationship entity's id as defined on the capsule's sqlalchemyTable 
  #      (as per <capsule>.sqlalchemyTable.<relationshipIdAttr>) and 
  #    - the relationship entity's id as defined on the relationship's sqlalchemy 
  #      table
  #      (as per <capsule>.sqlalchemyTable.<relationshipName>.id)
  with capsule.session.no_autoflush:
    relationshipIdOnCapsule = getattr(capsule.sqlalchemyTable, relationshipIdAttr)
    relationshipIdStoredSqla = getattr(relationshipSqlaTable, 'id')
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # If the relationship entity's id on the capsule's sqlalchemyTable 
  #    (<capsule>.sqlalchemyTable.<relationshipIdAttr>)
  #    does not yet have a value assigned, set it equal to the  
  #    relationship entity's id defined on the relationship's sqlalchemyTable
  #    (<capsule>.sqlalchemyTable.<relationshipName>.id)
  # Comment: relationship entity's id defined on the relationship's sqlalchemyTable
  #          might be 'None' in such case. 
  
  # if type(capsule).__name__ == "AssetClassCapsule":
  #   print(f"__ensureConsistentRelationshipId on capsule {type(capsule)}")
  #   print(f"    capsule.relationshipName: {relationshipName}                ")
  #   print(f"    capsule.relationshipIdOnCapsule: {relationshipIdOnCapsule}                ")
  #   print(f"    capsule.relationshipSqlaTable: \n{relationshipSqlaTable}                ")
  
  if relationshipIdOnCapsule is None:
    # If no relationshipIdAttr defined yet -> assign value
    capsule._setAttributeOnSqlalchemyTable(attributeName = relationshipIdAttr,
                                             value = relationshipIdStoredSqla)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Raise Exception the relationship entity's id on the capsule's sqlalchemyTable
  #    is not the same
  #    as the relationship entity's id defined on the relationship's sqlalchemy 
  #    table
  else:
      # if defied already and different -> raise error
    errMsg = f"Inconsistent definition of '{relationshipIdAttr}' and " + \
              f"'id' of '{relationshipName}' on object of type '{type(capsule)}'.\n" + \
              f"Value of '{relationshipIdAttr}': {relationshipIdOnCapsule}\n" + \
              f"Value of 'id' of {relationshipName}: {relationshipIdStoredSqla}\n"
    if relationshipIdStoredSqla is None:
      capsule._raiseException(errMsg)
    if relationshipIdOnCapsule != relationshipIdStoredSqla:
      # print("\n\ntype(relationshipIdOnCapsule): ", type(relationshipIdOnCapsule), " -  type(relationshipIdStoredSqla): ", type(relationshipIdStoredSqla))
      capsule._raiseException(errMsg)