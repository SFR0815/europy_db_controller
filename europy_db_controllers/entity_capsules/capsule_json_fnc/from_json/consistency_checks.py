import json, typing

from europy_db_controllers.entity_capsules import _capsule_base

from europy_db_controllers.entity_capsules.capsule_json_fnc.from_json import constants
from europy_db_controllers.entity_capsules.capsule_json_fnc.utils import uuid_encoder

T = typing.TypeVar("T", bound=_capsule_base.CapsuleBase)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Raising exception if something is inconsistent
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def ensureKeyInDict(key: str,
                    capsuleDict: dict[str, any],
                    className: str):
  if not key in capsuleDict:
    raise Exception(f"Missing key in dictionary provided to {constants.NAME_OF_DICT_FNC} of " + \
                    f"class {className}. \n" + \
                    f"Key missing: {key}.\n" + \
                    "Dictionary:\n" + str(capsuleDict))
def ensureNoIdProvidedForExistingEntity(entity: T,
                                        capsuleDict: dict[str, any],
                                        persistentMustHaveId: bool = False):
  if persistentMustHaveId:
    resultId = entity.id
    capsuleDictId = capsuleDict['id']
    if not resultId is None and capsuleDictId is None:
      jsonSpec = json.dumps(capsuleDict, cls=uuid_encoder.UUIDEncoder, indent = 4)
      raise Exception(f"Trying to upload an existing entity to the database: {entity.name}\n" + \
                      f"Id found in the database:  {resultId.hex}\n" + \
                      f"json spec: \n" + \
                      jsonSpec)