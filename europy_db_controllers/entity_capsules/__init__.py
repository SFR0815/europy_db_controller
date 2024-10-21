# import sys, os, importlib


# _this_dir = os.path.dirname(os.path.abspath(__file__))

# __all__ = []

# _module_names = [f[:-3] for f in os.listdir(_this_dir) if \
#                       f.endswith(".py") and \
#                       not f.startswith("__")]
# for module_name in _module_names:
#   module = importlib.import_module(f'.{module_name}', package=__package__)
#   globals()[module_name] = module
#   __all__.append(module_name)
