import typing

from europy_db_controllers.entity_capsules import _capsule_base

CT = typing.TypeVar('CT', bound=_capsule_base.CapsuleBase)

DEBUG_CAPSULE_NAME = "AssetClassCapsule_x"
DO_DEBUG = False

def isDebugCapsule(capsuleType: typing.Type[CT]) -> bool:
  return capsuleType.__name__ == DEBUG_CAPSULE_NAME

def setDoDebug(capsuleType: typing.Type[CT]) -> None:
  global DO_DEBUG
  DO_DEBUG = isDebugCapsule(capsuleType)

def debugPrint(msg: str,
               fncName: str,
               ) -> None:
  if DO_DEBUG:
    print(f"[{DEBUG_CAPSULE_NAME}.{fncName}] {msg}")

