# import sys, os, importlib
# from src.utils.os_utils import os_utils

# sys.path.insert(0, '..\..')

# _controller_dir = os.path.dirname(os.path.abspath(__file__))
# _OBJECTS_SUBDIR_NAME = "entity_capsules"
# _REL_PROP_SUBDIR_NAME = "relationship_attr"
# _OBJECTS_SUBDIR = os.path.join(_controller_dir , _OBJECTS_SUBDIR_NAME)
# _REL_PROP_SUBDIR = os.path.join(_controller_dir , _REL_PROP_SUBDIR_NAME)

# __all__ = []

# # entity_capsules modules:
# _module_names = [f[:-3] for f in os.listdir(_OBJECTS_SUBDIR) if \
#                       f.endswith(".py") and \
#                       not f.startswith("__")]
# for module_name in _module_names:
#   module = importlib.import_module(f'.{_OBJECTS_SUBDIR_NAME}.{module_name}', package=__package__)
#   globals()[module_name] = module
#   __all__.append(module_name)

# # relationship attribute modules:
# _module_names = [f[:-3] for f in os.listdir(_REL_PROP_SUBDIR) if \
#                       f.endswith(".py") and \
#                       not f.startswith("__")]
# for module_name in _module_names:
#   ref_module_name = f"_rel_attr_{module_name}"
#   module = importlib.import_module(f'.{_REL_PROP_SUBDIR_NAME}.{module_name}', package=__package__)
#   globals()[ref_module_name] = module
#   __all__.append(ref_module_name)


