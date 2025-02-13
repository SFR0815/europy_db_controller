import typing, datetime, sys, uuid, enum, sqlalchemy, json

from sqlalchemy.ext import declarative as sqlalchemy_decl


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils, _capsule_shared
from europy_db_controllers.entity_capsules.capsule_consistency_fnc import d_source_relationship_table_on_name



T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)
U = typing.TypeVar("U", bound=_capsule_base.CapsuleBase)

# f. Function ensuring the relationship entity's sqlalchemy is consistent with
#       the capsule's relationship entity's id and name definitions AND
#       present if such relationship entity is defined on db
def ensureConsistentRelationshipSqlalchemyTable(capsule: T,
                                                dictAttributeNamingConventions: dict[str, str],
                                                relationshipType: type[U]):
  relationshipName = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_RELATIONSHIP]
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  # Identify the sqlalchemyTable of the relationship's entity

  # if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
  #   print(f"\n[_capsule_consistency.ensureConsistentRelationshipSqlalchemyTable] on capsule {capsule.__class__.__name__} - relationshipName: {relationshipName}")
      
  relationshipSqlaTable = getattr(capsule.sqlalchemyTable, relationshipName)
  if relationshipSqlaTable is None:

    # if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
    #   print(f"[_capsule_consistency.ensureConsistentRelationshipSqlalchemyTable]    relationshipSqlaTable is None - trying to source it based on id")
      
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's sqlalchemyTable is not defined yet, 
    #    try to identify via the capsule's relationship entity id (see above #e)
    # Comment: This does nothing if the capsule sqlalchemyTable's relationship id  
    #          (<capsule>.sqlalchemyTable.<relationshipIdAttr>) is 'None'
    d_source_relationship_table_on_name.sourceRelationshipSqlalchemyTableBasedOnName(capsule = capsule,
                                                 dictAttributeNamingConventions = dictAttributeNamingConventions,
                                                 relationshipType = relationshipType)
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # If the relationship entity's sqlalchemyTable has not been identified
    #    based on id (see above), 
    #    try to identify via the capsule's relationship entity name (see above #f)
    # Comment: This does nothing if the capsule's relationship name  
    #          (<capsule>.<relationshipNameCapsuleInternalAttr>) is 'None'
    if getattr(capsule.sqlalchemyTable, relationshipName) is None:

      # if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
      #   print(f"[_capsule_consistency.ensureConsistentRelationshipSqlalchemyTable]    relationshipSqlaTable is None - trying to source it based on name")
      
      d_source_relationship_table_on_name.sourceRelationshipSqlalchemyTableBasedOnName(capsule = capsule,
                                                     dictAttributeNamingConventions = dictAttributeNamingConventions,
                                                     relationshipType = relationshipType)

    # if type(capsule).__name__ == DEBUG_CAPSULE_TYPE and relationshipName == DEBUG_RELATIONSHIP_NAME:
    #   print(f"[_capsule_consistency.ensureConsistentRelationshipSqlalchemyTable]    relationshipSqlaTable is None? {getattr(capsule.sqlalchemyTable, relationshipName) is None} - after sourcing based on name")
      
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Ensure name and id consistency between definitions on the capsule and the 
    #    relationship entity's sqlalchemyTable (if the latter is defined),
    #    see above #c & #b
    # Comment: This does nothing if the sqlalchemyTable of the relationship's entity
    #          (<capsule>.sqlalchemyTable.<relationshipName>) is 'None'
    nameOfConsistencyFnc = _capsule_utils.getConsistencyCheckFncName(
                                    relationshipName = relationshipName)
    getattr(capsule, nameOfConsistencyFnc)()