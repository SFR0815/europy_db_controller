import typing

from europy_db_controllers.xl.sheet.col_control_fnc import base_class

from europy_db_controllers.entity_capsules import _capsule_base

CT = typing.TypeVar('CT', bound=_capsule_base.CapsuleBase)

def addSubColControlFnc(self,
                        capsuleType: type[CT],
                        isList: bool = False,
                        relationshipKey: str = None
                        ) -> None:  
  subColControl = base_class.ColControl(subControllerKey = self.subControllerKey,
                              controllerKeyEnum = self.controllerKeyEnum,
                              capsuleType = capsuleType,
                              capsuleList = self._capsuleList,
                              rowControl = self._rowControl,
                              validations = self.validations,
                              firstCol = self.lastCol + 1,
                              parentColControl = self,
                              isList = isList,
                              relationshipKey=relationshipKey)
  self.subColControls[subColControl.label] = subColControl