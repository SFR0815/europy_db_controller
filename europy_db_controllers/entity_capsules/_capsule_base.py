from __future__ import annotations

import sys, uuid, typing, datetime
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy 
from sqlalchemy.ext import declarative as sqlalchemy_decl


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Decorator ensuring the session to be properly closed in case of an Exception
def cleanAndCloseSession(func):
  def wrapper(*args, **kwargs):
    session: sqlalchemy_orm.Session = None
    if 'session' in kwargs: # session passed as parameter
      session = kwargs['session']
    else: 
      session = args[0].session
    try:
      result = func(*args, **kwargs)
    except Exception as e:
      session.expunge_all()
      session.close()
      raise e
    return result
  return wrapper 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# basic capsule
class CapsuleBase():
  _capsule_object = True

  _nonJsonProperties = ["sqlAState", "isTransient", "isPending", \
                         "isDetached", "isPersistent", "isModified", \
                         "hasValueInput", "isInSession", \
                         "sqlalchemyTable", "sqlalchemyTableType", "session", \
                         "controllerData", "controllersLinkingTo", "controllersLinkedTo"]
  _jsonOnlyIfNoneId = []
  _base_columns = ['id']
  # the list of related capsules that are part of the current capsule's json and 
  #   are referred to by 'name' within the json of the current capsule
  #   Required for the validation of inputs (lists to select from)
  _referred_by_name_capsules = []  

  sqlalchemyTableType: any

  def _raiseException(self, errMsg: str): 
    self.session.expunge_all()
    self.session.close()
    raise Exception(errMsg)

  @cleanAndCloseSession
  def __init__(self,
               session: sqlalchemy_orm.Session,
               enforceNotNewOrDirty: bool = False) -> None:
    # enforceNotNewOrDirty is not applicable to objects without name
    self.session = session
    self.sqlalchemyTable = self.sqlalchemyTableType()
    self.controllersLinkingTo: typing.List[CapsuleBase] = []
    self.controllersLinkedTo: typing.List[CapsuleBase] = []
    self._hasValueInput = False # controls for any values set

  # def __del__(self):
  #   self.session.expire(self.sqlalchemyTable)
  
  def hasControllersLinkedTo(self) -> bool:
    return len(self.controllersLinkedTo) == 0   
  def hasControllersLinkingTo(self) -> bool:
    return len(self.controllersLinkingTo) == 0   
  def isUntied(self):
    return not (self.hasControllersLinkedTo() or \
                self.hasControllersLinkingTo)
  @cleanAndCloseSession
  def expire(self):
    if self.hasControllersLinkingTo(): 
      for linkingController in self.controllersLinkingTo:
        linkingController.controllersLinkedTo.remove(self)
      self.controllersLinkingTo = []
    if self.hasControllersLinkedTo(): 
      for linkedController in self.controllersLinkedTo:
        linkedController.controllersLinkingTo.remove(self)
      self.controllersLinkedTo = []
    self.session.expire(self.sqlalchemyTable)
    self.sqlalchemyTable = None
    self._hasValueInput = False
  def isEmpty(self):
    return self.sqlalchemyTable is None and \
           self._hasValueInput == False
  
  @classmethod
  def _key(self):
    return self.sqlalchemyTableType.__table__.name

  @classmethod
  @cleanAndCloseSession
  def defineBySqlalchemyTable(self, 
                              session: sqlalchemy_orm.Session,
                              sqlalchemyTableEntity):
    outType = self
    try:
      out = outType(session = session)
    except Exception as e:
      print(f"\n[defineBySqlalchemyTable]i outType: {outType.__name__} - sqlalchemyTableEntity: {type(sqlalchemyTableEntity).__name__}")
      raise e
    out.sqlalchemyTable = sqlalchemyTableEntity
    out._ensureConsistency()
    return out
  
  @classmethod
  @cleanAndCloseSession
  def _queryTableById(self,
                      session: sqlalchemy_orm.Session,
                      id: uuid.UUID):
    query = sqlalchemy.select(self.sqlalchemyTableType).where(self.sqlalchemyTableType.id == id)
    with session.no_autoflush:
      tableEntity = session.execute(query).scalar()
    if tableEntity is None:
      errMsg = f"Could not find any db entry with id '{id}' on table {str(self.sqlalchemyTableType.__table__)}."
      session.expunge_all()
      session.close()
      raise Exception(errMsg)
    return tableEntity
  def queryById(self,
                session: sqlalchemy_orm.Session,
                id: uuid.UUID):
    sqlalchemyTableEntity = self._queryTableById(session=session, id=id)
    output = self.defineBySqlalchemyTable(session=session, 
                                        sqlalchemyTableEntity = sqlalchemyTableEntity)
    output._hasValueInput = True
    return output

  @classmethod
  @cleanAndCloseSession
  def _queryTablesAll(self,
                session: sqlalchemy_orm.Session) -> typing.List[sqlalchemy_decl.DeclarativeMeta]: 
    query = sqlalchemy.select(self.sqlalchemyTableType).where(self.sqlalchemyTableType.id == id)
    result: typing.List[sqlalchemy_decl.DeclarativeMeta] = []
    for newObject in session.new:
      if isinstance(newObject, self.sqlalchemyTableType):
        result.append(newObject)
    for modifiedObject in session.dirty:
      if isinstance(modifiedObject, self.sqlalchemyTableType):
        result.append(modifiedObject)
    query = sqlalchemy.select(self.sqlalchemyTableType)
    with session.no_autoflush:
      return result + session.scalars(query).unique().all()
  @classmethod
  @cleanAndCloseSession
  def queryAll(self,
               session: sqlalchemy_orm.Session): 
        sqlalchemyTableEntities = self._queryTablesAll(session = session)
        result = []
        for sqlalchemyTableEntity in sqlalchemyTableEntities:
          result.append(self.defineBySqlalchemyTable(session = session, 
                                                     sqlalchemyTableEntity = sqlalchemyTableEntity))
        return result
  
  @property
  @cleanAndCloseSession
  def id(self):
    return self.sqlalchemyTable.id
  @id.setter
  @cleanAndCloseSession
  def id(self, id: uuid.UUID):
    if id is None: return
    if self.sqlalchemyTable.id is None:
      if self._hasValueInput:
        errMsg = f'Trying to provide an id of an entity of type {str(type(self))}.\n' + \
                 f'The object already carries values not being the default.\n' + \
                 f'Id of entity: {str(self.sqlalchemyTable.id)}.\n' + \
                 f'Id provided : {str(id)}.'
        self._raiseException(errMsg)
      else:
        self.sqlalchemyTable = self._queryTableById(session = self.session, 
                                                    id = id)
        self._hasValueInput = True
        self._ensureConsistency()
    else:
      if self.sqlalchemyTable.id != id: 
        # Changing the id is not permissible
        errMsg = f'Trying to change the id of an entity of type {str(type(self))}.\n' + \
                        f'Id of entity: {str(self.sqlalchemyTable.id)}.\n' + \
                        f'Id provided : {str(id)}.'
        self._raiseException(errMsg)
      else:
        # Nothing to change
        pass
  def _omit_none_id(self, id: uuid.UUID):
    if id is None: return
    self.id = id

  def addToSession(self):
    if not self.sqlalchemyTable in self.session:
      if not self.isInSession:
        self.session.add(self.sqlalchemyTable)
  def deleteFromDb(self):
    if self.sqlalchemyTable in self.session: 
      ready_for_delete = True
      if hasattr(self.sqlalchemyTable, 'readyForDelete'):
        ready_for_delete, msg = self.sqlalchemyTable.readyForDelete
      if ready_for_delete:
        self.session.delete(self.sqlalchemyTable)
      else: 
        raise ValueError(msg)
  def refresh(self):
    # refresh the data of the object
    self.session.refresh(self.sqlalchemyTable)

  @property
  def sqlAState(self):
    if self.sqlalchemyTable is None: return None
    return sqlalchemy.inspect(self.sqlalchemyTable)
  @property
  def isTransient(self):
    state = self.sqlAState
    if state is None: return False
    return state.transient
  @property
  def isPending(self):
    state = self.sqlAState
    if state is None: return False
    return state.pending
  @property
  def isDetached(self):
    state = self.sqlAState
    if state is None: return False
    return state.detached
  @property
  def isPersistent(self):
    state = self.sqlAState
    if state is None: return False
    return state.persistent
  @property
  def isInSession(self):
    return self.sqlalchemyTable in self.session
  
  @property
  def isModified(self):
    if self.sqlalchemyTable is None: return False
    return self.session.is_modified(self.sqlalchemyTable)
  
  @property
  def hasValueInput(self) -> bool:
    return self._hasValueInput
  


class CapsuleBaseWithName(CapsuleBase):
  @cleanAndCloseSession
  def __init__(self,
               session: sqlalchemy_orm.Session,
               enforceNotNewOrDirty: bool = False) -> None:
    super().__init__(session = session,
                     enforceNotNewOrDirty = enforceNotNewOrDirty)
    self.enforceNotNewOrDirty = enforceNotNewOrDirty

  _base_columns = CapsuleBase._base_columns + ['name'] 

  @classmethod
  @cleanAndCloseSession
  def _queryTablesByNamePrefix(self,
                              session: sqlalchemy_orm.Session,
                              namePrefix: str) -> typing.List[sqlalchemy_decl.DeclarativeMeta]: 
    result: typing.List[sqlalchemy_decl.DeclarativeMeta] = []
    for newObject in session.new:
      if isinstance(newObject, self.sqlalchemyTableType):
        if newObject.name.startswith(namePrefix):
          result.append(newObject)
    for modifiedObject in session.dirty:
      if isinstance(modifiedObject, self.sqlalchemyTableType):
        if modifiedObject.name.startswith(namePrefix):
          result.append(modifiedObject)
    query = sqlalchemy.select(self.sqlalchemyTableType).where(self.sqlalchemyTableType.name.startswith(namePrefix))
    with session.no_autoflush:
      return result + session.scalars(query).unique().all()
  @classmethod
  @cleanAndCloseSession
  def queryByNamePrefix(self,
                        session: sqlalchemy_orm.Session,
                        namePrefix: str): 
        sqlalchemyTableEntities = self._queryTablesByNamePrefix(
                        session = session,
                        namePrefix = namePrefix)
        result = []
        for sqlalchemyTableEntity in sqlalchemyTableEntities:
          result.append(self.defineBySqlalchemyTable(session = session, 
                                                     sqlalchemyTableEntity = sqlalchemyTableEntity))
        return result

  @classmethod
  def getNewSqlalchemyTablesOfName(self,
                                   session: sqlalchemy_orm.Session,
                                   name: str) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
    result = []
    for newObject in session.new:
      if isinstance(newObject, self.sqlalchemyTableType):
        if newObject.name == name:
          result.append(newObject)
    return result
  @classmethod
  def hasNewSqlalchemyTablesOfName(self,
                                   session: sqlalchemy_orm.Session,
                                   name: str) -> bool:
    return len(self.getNewSqlalchemyTablesOfName(session, name)) > 0
  @classmethod
  def getDirtySqlalchemyTablesOfName(self,
                                     session: sqlalchemy_orm.Session,
                                     name: str) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
    result = []
    for modifiedObject in session.dirty:
      if isinstance(modifiedObject, self.sqlalchemyTableType):
        if modifiedObject.name == name:
          result.append(modifiedObject)
    return result
  @classmethod
  def hasDirtySqlalchemyTablesOfName(self,
                                   session: sqlalchemy_orm.Session,
                                   name: str) -> bool:
    return len(self.getDirtySqlalchemyTablesOfName(session, name)) > 0
  @classmethod
  def getNewOrDirtySqlalchemyTablesOfName(self,
                                          session: sqlalchemy_orm.Session,
                                          name: str) -> typing.List[sqlalchemy_decl.DeclarativeMeta]: 
    newSqlalchemyTables = self.getNewSqlalchemyTablesOfName(session, name)
    dirtySqlalchemyTables = self.getDirtySqlalchemyTablesOfName(session, name)
    return newSqlalchemyTables + dirtySqlalchemyTables
  @classmethod
  def hasNewOrDirtySqlalchemyTablesOfName(self,
                                          session: sqlalchemy_orm.Session,
                                          name: str):  
    return len(self.getNewOrDirtySqlalchemyTablesOfName(session, name)) > 0
    
  @classmethod
  @cleanAndCloseSession
  def _queryTableByName(self,
                        session: sqlalchemy_orm.Session,
                        name: str) -> typing.List[sqlalchemy_decl.DeclarativeMeta]:
    # first check if object exists in new or modified objects:
    newOrDirty = self.getNewOrDirtySqlalchemyTablesOfName(session, name)
    if len(newOrDirty) > 0:
      return newOrDirty
    # if not in new objects search database
    query = sqlalchemy.select(self.sqlalchemyTableType).where(self.sqlalchemyTableType.name == name)
    with session.no_autoflush:
      return session.scalars(query).unique().all()
  @classmethod
  @cleanAndCloseSession
  def nameExists(self,
                           session: sqlalchemy_orm.Session,
                           name: str) -> bool:
    sqlalchemyTables = self._queryTableByName(session=session, name=name)
    return len(sqlalchemyTables) > 0


  @classmethod
  @cleanAndCloseSession
  def queryByName(self,
                  session: sqlalchemy_orm.Session,
                  name: str):
    sqlalchemyTableEntities = self._queryTableByName(session = session,
                                                     name = name)
    result = []
    for sqlalchemyTableEntity in sqlalchemyTableEntities:
      result.append(self.defineBySqlalchemyTable(session = session, 
                                                  sqlalchemyTableEntity = sqlalchemyTableEntity))
    return result
  
  @property
  @cleanAndCloseSession
  def name(self):
    return self.sqlalchemyTable.name
  @name.setter
  @cleanAndCloseSession
  def name(self, name: str):
    def raiseDuplicateNameException():
      if self.__class__.hasNewSqlalchemyTablesOfName(self.session, name):
        errMsg = f"[Duplicate name of entity] Newly created entities (exactly '{str(len(sqlalchemyTables))}') " + \
                 f"of type '{type(self)}' with name '{name}' identified on the system."
        self._raiseException(errMsg)
      elif self.__class__.hasDirtySqlalchemyTablesOfName(self.session, name):
        errMsg = f"[Duplicate name of entity] Some modified entities (exactly '{str(len(sqlalchemyTables))}') " + \
                 f"of type '{type(self)}' had their name changed to '{name}' during session."
        self._raiseException(errMsg)
      else:
        errMsg = f"[Duplicate name of entity] Other entities on the database (exactly '{str(len(sqlalchemyTables))}') " + \
                 f"of type '{type(self)}' with name '{name}' identified on the system."
        self._raiseException(errMsg)
    if not self._hasValueInput:
      # no input provided yet
      if self.enforceNotNewOrDirty:
        if self.__class__.hasNewSqlalchemyTablesOfName(self.session, name):
          newOfName = self.__class__.getNewSqlalchemyTablesOfName(self.session, name)
          numberOfNewOfName = len(newOfName)
          errMsg = f"[New entity with duplicate name] Trying to setup a {type(self).__name__} using a name that is " + \
                   f"already in use within the newly setup objects of such type " + \
                   f"- while explicitly not allowing for this.\n" + \
                   f"Name causing the issue: {name}\n" + \
                   f"Number of entities using this name: {str(numberOfNewOfName)}\n"
          self._raiseException(errMsg)
        elif self.__class__.hasDirtySqlalchemyTablesOfName(self.session, name):
          newDirtyOfName = self.__class__.getDirtySqlalchemyTablesOfName(self.session, name)
          numberOfDirtyOfName = len(newDirtyOfName)
          errMsg = f"[New entity with duplicate name] Trying to setup a {type(self).__name__} using a name that is " + \
                   f"belongs to an entity whose name got changed during the session. " + \
                   f"- while explicitly not allowing for this.\n" + \
                   f"Name causing the issue: {name}\n" + \
                   f"Number of entities using this name: {str(numberOfDirtyOfName)}\n"
          self._raiseException(errMsg)
      sqlalchemyTables = self._queryTableByName(session = self.session,
                                               name = name)
      if len(sqlalchemyTables) == 1:
        # 1. set sqlalchemyTable if entity with name found on system
        self.sqlalchemyTable = sqlalchemyTables[0]
        if self.isModified and (name != self.name):
          errMsg = f"[Accessing object with changed name by original name] " + \
                   f"Name used for object of type {type(self).__name__} has been changed " + \
                   f"during the session.\n" + \
                   f"Name name provided for entity identification  : {name}\n" + \
                   f"This name has been changed during session into: {self.name}\n"
          self._raiseException(errMsg)
        self._ensureConsistency()
        self._hasValueInput = True
      elif len(sqlalchemyTables) == 0:  
        # 2. Set name on (new) object if not found
        self.sqlalchemyTable.name = name
        self._hasValueInput = True
      else:
        raiseDuplicateNameException()
    else:
      # some input provided already
      # 1. Raise error if new name is different from current one
      #    and name already used for some entity
      if name != self.sqlalchemyTable.name:
        # self.session.expire(self.sqlalchemyTable)
        if self.__class__.hasNewOrDirtySqlalchemyTablesOfName(self.session, name):
          newOrDirtyOfName = self.__class__.getNewOrDirtySqlalchemyTablesOfName(self.session, name)
          numberOfNewOrDirtyOfName = len(newOrDirtyOfName)
          errMsg = f"Trying to modify a {type(self).__name__} using a name that is " + \
                   f"already in use within the newly setup or changed objects of such type.\n" + \
                   f"Name causing the issue: {name}\n" + \
                   f"Number of entities using this name: {str(numberOfNewOrDirtyOfName)}\n"
          self._raiseException(errMsg)
        sqlalchemyTables = self._queryTableByName(session = self.session,
                                                  name = name)
        if len(sqlalchemyTables) > 0:
          raiseDuplicateNameException()
      # 2. Set new name on object
        if self.sqlalchemyTable.id is None:
          if self.sqlalchemyTable.name != name:
            self.sqlalchemyTable.modified_at = datetime.datetime.now()
        self.sqlalchemyTable.name = name

  def _omit_none_name(self, name: str):
    if name is None: return
    self.name = name
