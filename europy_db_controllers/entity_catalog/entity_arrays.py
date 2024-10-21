from __future__ import annotations

import sys, uuid, typing, datetime, \
       types
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy 

sys.path.insert(0, '..\..')

from europy_db_controllers.entity_capsules import _capsule_base, capsules

class entityArrayFnc():
  _controller_array = True
  _with_name = False

  def getDuplicate(self, controllerCapsule: _capsule_base.CapsuleBase):
    if controllerCapsule is None: return None
    for thisControllerCapsule in self:
      if controllerCapsule is thisControllerCapsule: 
        return None # Not a duplicate, it's the same object
      if controllerCapsule.sqlalchemyTable is thisControllerCapsule.sqlalchemyTable:
        return thisControllerCapsule # This is a real duplicate
    return None
  def hasDuplicate(self, controllerCapsule: _capsule_base.CapsuleBase):
    return self.hasDuplicate(controllerCapsule=controllerCapsule) is not None
  def hasAlready(self, controllerCapsule: _capsule_base.CapsuleBase):
    if controllerCapsule is None: return False
    for thisControllerCapsule in self:
      if controllerCapsule is thisControllerCapsule:
        return True
    return False
    
  def remove(self, controllerCapsule: _capsule_base.CapsuleBase):
    controllerCapsule.expire()
    self.remove(controllerCapsule)
    controllerCapsule = None
  def addOrRefresh(self, controllerCapsule: _capsule_base.CapsuleBase):
    if controllerCapsule is None: return
    if controllerCapsule.isExpired(): return
    for thisControllerCapsule in self:
      if thisControllerCapsule.sqlalchemyTable is controllerCapsule.sqlalchemyTable:
        if not(thisControllerCapsule is controllerCapsule): 
          self.remove(thisControllerCapsule)
          self.append(controllerCapsule)
          return
        else:
          return # controllerCapsule exists already
    self.append(controllerCapsule)
     
  def getObjectOfId(self, id: uuid.UUID) -> _capsule_base.CapsuleBase:
    for controllerCapsule in self:
      if controllerCapsule.id == id: 
        return controllerCapsule
    return None
  def hasObjectOfId(self, id: uuid.UUID) -> bool:
    return self.getObjectOfId(id = id) is not None

class entityArrayFncWithName(entityArrayFnc):
  _controller_array = True
  _with_name = True
  def getObjectOfName(self, name: str) -> _capsule_base.ControllerBaseWithName:
    for controllerCapsule in self.items:
      if controllerCapsule.name == name: 
        return controllerCapsule
    return None
  def hasObjectOfName(self, name: str) -> bool:
    return self.getObjectOfName(name = name) is not None
  

TransactionTypeCapsuleArray = types.new_class('TransactionTypeCapsuleArray',
                                              (typing.List[capsules.TransactionTypeCapsule], \
                                              entityArrayFncWithName))