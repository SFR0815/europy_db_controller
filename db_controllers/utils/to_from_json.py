from __future__ import annotations

import typing, sys, uuid, json, datetime


sys.path.insert(0, '..')

from .. import controller_base, transaction, asset, \
                asset_class


literal_types = (str, float, int, bool, datetime.datetime, uuid.UUID)

def toDict(self):
  def isJsonList(list: typing.List) -> bool:
    listType: any
    for item in list:
      if item is None: 
        continue
      elif listType is None:
        listType = type(item)
        continue
      elif listType == type(item):
        continue
      else:
        return False
    return True     
  result = {}
  print("dir(self): ")
  for dictAttrName in dir(self):
    # exclude attributes that are marked as to be excluded:
    if not (dictAttrName.startswith("_") or dictAttrName in self._nonJsonProperties):
      attr = getattr(self, dictAttrName)
      if not (callable(getattr(self, dictAttrName)) or hasattr(attr, '_capsule_object')): 
        if isinstance(attr, literal_types):
          result[dictAttrName] = getattr(self, dictAttrName)
          print("    Literal          : ", dictAttrName, "IsTypeOf: ", type(getattr(self, dictAttrName)))
        else:
          self._raiseException(f"toDict trying to handle unspecified literal value of type {type(attr)} for attribute {dictAttrName}. \n" + \
                          f"Type of object converted into a dictionary: {type(self)}.")
      elif hasattr(attr, '_capsule_object'): 
        result[dictAttrName] = attr.toDict()
        print("    _capsule_object: ", dictAttrName, "IsTypeOf: ", type(getattr(self, dictAttrName)))
      elif attr is None:
        result[dictAttrName] = attr
      elif type(attr) is typing.List:
        if len(attr) == 0:
          result[dictAttrName] = attr
        else:
          attrList = []
          if isJsonList(attr):
            for item in attr:
              if item is None:
                attrList.append(item)
              elif isinstance(item, literal_types):
                attrList.append(item)
              elif hasattr(item, '_capsule_object'):
                attrList.append(item.toDict())
              else:
                self._raiseException(f"toDict trying to handle unspecified type {type(item)} in list of attribute {dictAttrName}. \n" + \
                                f"Type of object converted into a dictionary: {type(self)}.")
          else:
            self._raiseException("Non homogeneous type of list items.") 
          result[dictAttrName] = attrList
  return result 

def isAlreadyDefined(self, dictionary:typing.Dict): 
  # test if an object with given value is already defined on the system
  pass
  
def fromDict(self, 
             dictionary: typing.Dict,
             force: bool = False):
  # force -> create new object regardless of object with same values already exists on db 
  print("Look how nice!")
  return

def toJson(self):
    entityDict = self.toDict()
    return json.dumps(entityDict)

setattr(controller_base.CapsuleBase, "toDict", toDict)
setattr(controller_base.CapsuleBase, "fromDict", classmethod(fromDict))
setattr(controller_base.CapsuleBase, "toJson", toJson)