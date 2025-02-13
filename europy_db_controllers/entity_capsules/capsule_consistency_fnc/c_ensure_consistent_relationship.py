import typing

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base, _capsule_shared

from europy_db_controllers.entity_capsules.capsule_consistency_fnc import a_ensure_consistent_relationship_name \
             as consistency_rel_name
from europy_db_controllers.entity_capsules.capsule_consistency_fnc import b_ensure_consistent_relationship_id \
             as consistency_rel_id

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)

# c. Function ensuring both 'name' and 'id' consistency:
def ensureConsistentRelationship(capsule: T,
                                   dictAttributeNamingConventions: dict[str, str]):
  relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  consistency_rel_id.ensureConsistentRelationshipId(capsule = capsule,
                                    dictAttributeNamingConventions = dictAttributeNamingConventions)
  if hasattr(capsule, relationshipNameCapsuleInternalAttr):
    consistency_rel_name.ensureConsistentRelationshipName(capsule = capsule,
                                        dictAttributeNamingConventions = dictAttributeNamingConventions)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# d. Function sourcing the relationship entity's sqlalchemyTable from db based
#       upon the relationship entity's id
def sourceRelationshipSqlalchemyTableBasedOnId(capsule: T,
                                                 dictAttributeNamingConventions: dict[str, str],
                                                 relationshipType: type[U]):
  relationshipIdAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  # NOT REQUIRED, SEE BELOW:
  # relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Raise Exception if the sqlalchemyTable of the capsule does not have
  #    an attribute providing the relationship's id (see above #b)
  _capsule_shared._raiseExceptionIfNoRelationshipIdOnCapsuleSqlaTable(capsule = capsule,
                                                       dictAttributeNamingConventions = dictAttributeNamingConventions)
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify the relationship entity's id as defined on the capsule's sqlalchemyTable 
  #      (as per <capsule>.sqlalchemyTable.<relationshipIdAttr>)
  relationshipIdOnCapsule = getattr(capsule.sqlalchemyTable, relationshipIdAttr)
  if relationshipIdOnCapsule is not None:      
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's id as defined on the capsule's sqlalchemyTable 
    #      (as per <capsule>.sqlalchemyTable.<relationshipIdAttr>) has some
    #      value assigned, source it from the db
    # Comment: If no such relationship entity is found on the db, this raises an 
    #          Exception (see module _capsule_base)
    sqlalchemyTable = relationshipType._queryTableById(
                                session = capsule.session, 
                                id = relationshipIdOnCapsule)
    # Set the attribute <relationshipName> of the capsule's sqlalchemyTable
    #    equal to the relationship entity's sqlalchemyTable sourced
    capsule._setAttributeOnSqlalchemyTable(attributeName = relationshipName,
                                             value = sqlalchemyTable)
              # CAN'T BE THE CASE. WHEN UPDATING AND ID OR SQLALCHEMY TABLE
              # ON THE CAPSULE, ALL NON UPDATED ATTRIBUTES OF THE RELATIONSHIP
              # ARE SET TO 'NONE' AND ARE SUBSEQUENTLY UPDATED VIA THE CONSISTENCY
              # METHODS ABOVE. 
              # # Update the relationship entity's name on the capsule
              # # Comment: this effectively overwrites the capsule's internal relationship
              # #          name with the source relationship entity's name.
              # #          This ensures consistency in case of 'replacing' the relationship
              # #          entity by some other. 
              # if hasattr(sqlalchemyTable, 'name'): 
              #   relationshipNameStoredSqla = getattr(sqlalchemyTable, 'name')
    # Ensure name consistency between the 
    #       - the relationship entity's name stored as attribute on the capsule 
    #         (<capsule>.<relationshipNameCapsuleInternalAttr>) AND
    #       - the relationship entity's name available on the sqlalchemyTable of 
    #         the relationship's entity 
    #         (<capsule>.sqlalchemyTable.<relationshipName>.name)
    # see above #c
    consistency_rel_name.ensureConsistentRelationshipName(capsule = capsule,
                                       dictAttributeNamingConventions = dictAttributeNamingConventions)    
