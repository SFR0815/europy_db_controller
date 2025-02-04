import typing, sys


from europy_db_controllers.entity_capsules import _capsule_base, _capsule_utils

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Raise error if <relationshipIdAttr> is not available on the capsule's 
#    sqlalchemy table
def _raiseExceptionIfNoRelationshipIdOnCapsuleSqlaTable(capsule: T,
                                                        dictAttributeNamingConventions: dict[str, str]):
  relationshipIdAttr = dictAttributeNamingConventions[_capsule_utils.REL_ATTR_DICT_KEY_ID]
  # Get all ORM descriptors including hybrid properties
  all_descriptors = capsule.sqlalchemyTable.__mapper__.all_orm_descriptors.keys()
  
  if relationshipIdAttr not in dir(capsule.sqlalchemyTable) and relationshipIdAttr not in all_descriptors:
    errMsg = f"Could not identify the required attribute {relationshipIdAttr} on " + \
             f"the sqlalchemy table of object {type(capsule).__name__}."
    errMsg += "\nAvailable attributes:\n"
    errMsg += "Regular attributes:\n"
    for attr in dir(capsule.sqlalchemyTable):
        if not attr.startswith('_'):
            errMsg += f"  - {attr}\n"
    errMsg += "\nORM descriptors (including hybrid properties):\n"
    for attr in all_descriptors:
        errMsg += f"  - {attr}\n"
    capsule._raiseException(errMsg)
