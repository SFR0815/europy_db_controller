from __future__ import annotations

import typing


from europy_db_controllers.entity_capsules import _capsule_base
from europy_db_controllers import _controller_base, _controller_attr, \
                            _controller_obj_setup, _controller_json

######################################################################################
######################################################################################
# ATTENTION: do not forget to update _controller_base.ControllerKeyEnum
#            when adding sub-controllers to the main controller 
######################################################################################
######################################################################################


C = typing.TypeVar("C", bound=_controller_base.ControllerBase)
CT = typing.TypeVar("CT", bound=_capsule_base.CapsuleBase)
# CONTR_ENUM_STR = typing.TypeVar("CONTR_ENUM_STR", str, _controller_base.ControllerKeyEnum)



CE = typing.TypeVar('CE', bound = _controller_base.BaseControllerKeyEnum)

#FIXME: please think of a better name for this fnc :) 
def setupControllerClass(
            callingGlobals: typing.Dict[str, any],
            controllerTypeNames: typing.List[str],
            controllerTypeEnumType: typing.Type,
            ) -> typing.Type[C]:
    # Merge callingGlobals with the current file's globals
    merged_globals = globals().copy()
    merged_globals.update(callingGlobals)
    
    # Configure attributes
    _controller_attr.addAttributes(controllerTypeNames=controllerTypeNames,
                                   callingGlobals=merged_globals)

    # Add setup methods
    _controller_obj_setup.addSetupMethods(controllerTypeNames=controllerTypeNames,
                                          callingGlobals=merged_globals)

    # Add dictionary functions
    _controller_json.addDictFunctions(controllerTypeNames=controllerTypeNames,
                                      controllerKeyEnumType = controllerTypeEnumType,
                                      callingGlobals=merged_globals)

    # Return the configured Controller class
    return merged_globals['Controller']



# project_input.addAttributes(projectInputSubcontroller = ProjectInput)
# fifo_data.addAttributes(fifoDataController = FifoData)
# asset_classification.addAttributes(assetClassificationController = AssetClassification)