import typing, datetime, sys, uuid, enum, sqlalchemy, json

from sqlalchemy.ext import declarative as sqlalchemy_decl


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils, _capsule_shared

from europy_db_controllers.entity_capsules.capsule_consistency_fnc import b_ensure_consistent_relationship_id \
             as consistency_rel_id

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)   

# d. Function sourcing the relationship entity's sqlalchemyTable from db based
#       upon the relationship entity's name
def sourceRelationshipSqlalchemyTableBasedOnName(capsule: T,
                                                   dictAttributeNamingConventions: dict[str, str],
                                                   relationshipType: type[U]):
  relationshipNameCapsuleInternalAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_INTERNAL_NAME]
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Do nothing if the capsule has no attribute for the relationships name
  #    (no <capsule>.<relationshipNameCapsuleInternalAttr>), 
  #    i.e. the sqlalchemyTable of the relationship does not have a 'name'
  #    attribute.
  if hasattr(capsule, relationshipNameCapsuleInternalAttr):
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Identify the relationship entity's name as defined on the capsule 
    #      (as per <capsule>.<relationshipNameCapsuleInternalAttr>)
    relationshipNameOnCapsule = getattr(capsule, relationshipNameCapsuleInternalAttr)
    if relationshipNameOnCapsule is not None:      
      # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
      # If the relationship entity's id as defined on the capsule 
      #      (as per <capsule>.<relationshipNameCapsuleInternalAttr>) has some
      #      value assigned, source it from the db
      # Comment: If no such relationship entity is found on the db, no Exception is  
      #          raised (see module _capsule_base)
      sqlalchemyTables = relationshipType._queryTableByName(
                                  session = capsule.session,
                                  name =  relationshipNameOnCapsule)
      # Raise an Exception if no such relationship entity has been identified or 
      #    the name of the relationship's entity provided is not unique
      if len(sqlalchemyTables) == 0:
        capsule._raiseException(f"No entities of '{relationshipName}' on object " + \
                        f"of type '{type(capsule)}' with name: {relationshipNameOnCapsule}")
      elif len(sqlalchemyTables) > 1:
        capsule._raiseException(f"Multiple entities of '{relationshipName}' on object " + \
                        f"of type '{type(capsule)}' with identical name: {relationshipNameOnCapsule}")
      # Set the attribute <relationshipName> of the capsule's sqlalchemyTable
      #    equal to the relationship entity's sqlalchemyTable sourced
      sqlalchemyTable = sqlalchemyTables[0]
      capsule._setAttributeOnSqlalchemyTable(attributeName = relationshipName,
                                             value = sqlalchemyTable)
      # Ensure name consistency between the 
      #       - the relationship entity's id available on the capsule's sqlalchemyTable 
      #         (<capsule>.sqlalchemyTable.<relationshipIdAttr>) AND
      #       - the relationship entity's id available on the sqlalchemyTable of 
      #         the relationship's entity 
      #         (<capsule>.sqlalchemyTable.<relationshipName>.id)
      # see above #d
      consistency_rel_id.ensureConsistentRelationshipId(capsule = capsule,
                                       dictAttributeNamingConventions = dictAttributeNamingConventions)
