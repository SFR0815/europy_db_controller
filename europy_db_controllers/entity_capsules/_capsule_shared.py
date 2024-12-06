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
  if not hasattr(capsule.sqlalchemyTable, relationshipIdAttr):
    capsule._raiseException(f"Could not identify the required attribute {relationshipIdAttr} on " + \
                    f"the sqlalchemy table of object {type(capsule).__name__}.")
