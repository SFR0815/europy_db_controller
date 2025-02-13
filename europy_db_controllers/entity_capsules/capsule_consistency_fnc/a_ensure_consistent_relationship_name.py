import typing

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

# a. Function ensuring the name of the relationship's entity being consistent between
#       - the relationship entity's name stored as attribute on the capsule 
#         (<capsule>.<relationshipNameCapsuleInternalAttr>) AND
#       - the relationship entity's name available on the sqlalchemyTable of 
#         the relationship's entity 
#         (<capsule>.sqlalchemyTable.<relationshipName>.name)
def ensureConsistentRelationshipName(capsule: T,
                                       dictAttributeNamingConventions: dict[str, str]):
  relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Do nothing if the capsule has no attribute for the relationships name
  #    (no <capsule>.<relationshipNameCapsuleInternalAttr>), 
  #    i.e. the sqlalchemyTable of the relationship does not have a 'name'
  #    attribute.
  if hasattr(capsule, relationshipNameCapsuleInternalAttr):

    # if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
    #   print(f"__ensureConsistentRelationshipName on capsule {capsule.__class__.__name__} - relationshipName: {relationshipName}")
    #   print(f"    capsule.relationshipNameCapsuleInternalAttr: {relationshipNameCapsuleInternalAttr}")

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Identify the sqlalchemyTable of the relationship's entity
    with capsule.session.no_autoflush:
      relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Exit if the sqlalchemyTable of the relationship's entity is not defined yet
    if relationshipSqlaTable is None: 
      return 
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Identify 
    #    - the relationship entity's name as defined on the capsule 
    #      (as per <capsule>.<relationshipNameCapsuleInternalAttr>) and 
    #    - the relationship entity's name as defined on the relationship's sqlalchemyTable
    #      (as per <capsule>.sqlalchemyTable.<relationshipName>.name)
    relationshipNameOnCapsule = getattr(capsule, relationshipNameCapsuleInternalAttr)
    relationshipNameStoredSqla = getattr(relationshipSqlaTable, 'name')
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's name on the capsule 
    #    (<capsule>.<relationshipNameCapsuleInternalAttr>)
    #    does not yet have a value assigned, set it equal to the  
    #    relationship entity's name defined on the relationship's sqlalchemyTable
    #    (<capsule>.sqlalchemyTable.<relationshipName>.name)
    # Comment: relationship entity's name defined on the relationship's sqlalchemyTable
    #          might be 'None' in such case. 
    if relationshipNameOnCapsule is None:
      setattr(capsule, relationshipNameCapsuleInternalAttr, relationshipNameStoredSqla)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Raise Exception the relationship entity's name on the capsule is not the same
    #    as the relationship entity's name defined on the relationship's sqlalchemyTable
    else:
      errMsg = f"Inconsistent definition of '{relationshipNameCapsuleInternalAttr}' and " + \
                f"'name' of '{relationshipName}' on object of type '{type(capsule)}'.\n" + \
                f"Value of '{relationshipNameCapsuleInternalAttr}': {relationshipNameOnCapsule}\n" + \
                f"Value of 'name' of {relationshipName}: {relationshipNameStoredSqla}\n"
      if relationshipNameStoredSqla is None:
        capsule._raiseException(errMsg)
      if relationshipNameOnCapsule != relationshipNameStoredSqla:
        capsule._raiseException(errMsg)
