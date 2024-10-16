from __future__ import annotations

import typing, uuid, unittest
import enum

FSD = typing.TypeVar('FSD', bound='FixedStateDict')

class StateEnum(enum.Enum):
  FULL = 1
  BASIC = 2

class SqlalchemyColumnType(enum.Enum):
  OTHER = 0
  RELATIONSHIP_ID = 1
  RELATIONSHIP_NAME_FIELD = 2
  RELATIONSHIP = 3
  PRIMARY_KEY = 4

class StateSwitch():
  def __init__(self, state = StateEnum.FULL) -> None:
    self.state: StateEnum = state

class ValueFlags():
  def __init__(self, 
               isList: bool = False,
               isUnique: bool = False, 
               columnType: SqlalchemyColumnType = SqlalchemyColumnType.OTHER,
               # etc.
               ) -> None:
    self.isList = isList
    self.isUnique = isUnique
    self.columnType = columnType

class StateDictBase():
  def __init__(self, stateSwitch: StateSwitch = None) -> None:
    self._stateSwitch: StateSwitch = stateSwitch
  @property
  def state(self) -> StateEnum:
    return self._stateSwitch.state
  @state.setter
  def state(self, state: StateEnum) -> None:
    self._stateSwitch.state = state

class NumberedDict(StateDictBase):
  def __init__(self, stateSwitch: StateSwitch) -> None:
    super().__init__(stateSwitch = stateSwitch)
    self.data: typing.OrderedDict[int, FixedStateDict] = typing.OrderedDict[int, FixedStateDict]()
  
  def __ensureValidValue(self, value: FixedStateDict, fncName: str) -> bool:
    if not isinstance(value, FixedStateDict): 
      print(f"type(value): {type(value).__name__} - FSD: {FixedStateDict.__name__} - fulfills: {FixedStateDict.__name__}")
      raise Exception(f"[{fncName}] Value not of type 'FixedStateDict' but of type '{type(value).__name__}'.")

  def __ensureValidKey(self, key: int, fncName: str) -> bool:
    if not isinstance(key, int):
      raise Exception(f"[{fncName}] Non integer key '{key}' of type '{type(key).__name__}' provided to NumberedDict.")
    if not(key < self.length): 
      raise Exception(f"[{fncName}] Key provided exceeds the max pos of elements in NumberedDict.\n" + \
                      f"Max pos of elements in NumberedDict: {self.length - 1}\n" + \
                      f"Position requested as per key      : {key}")
    if key < 0: 
      raise Exception(f"[{fncName}] Key provided is negative.\n" + \
                      f"Positions of elements in NumberedDict are greater or equal to '0'.") 
  
  @property
  def length(self):
    return len(self.data)
  
  def hasKey(self, key: int) -> bool:
    return key < self.length and key >= 0
  def append(self, value: FixedStateDict):
    self.__ensureValidValue(value, "append")
    self.data[self.length] = value
  def __getitem__(self, __key: int) -> FixedStateDict:
    self.__ensureValidKey(__key, "__getitem__")  
    return self.data[__key]
  def __setitem__(self, __key: str, __value: FixedStateDict):
    # adding not permitted, replacement only
    self.__ensureValidKey(__key, "__setitem__") 
    self.__ensureValidValue(__value, "__setitem__")
    self.data.__setitem__(__key, __value)
  def __delitem__(self, __key: int) -> None:
    self.__ensureValidKey(__key, "__setitem__") 
    if __key == self.length - 1:
      self.data__delitem__(__key)
    else:
      head = typing.OrderedDict[int, FixedStateDict](**{k: self.data[k] for k in range(0, __key)})
      tail = typing.OrderedDict[int, FixedStateDict](**{k-1: self.data[k] for k in range(__key + 1, self.length)})
      self.data = head | tail
  def reset(self):
    self.data = typing.OrderedDict[int, FixedStateDict]()  

  def values(self):
    return self.data.values()
  def items(self):
    return self.data.items()
  def keys(self):
    return self.data.keys()

  def __repr__(self) -> str: 
    return self.data.__repr__()



class FixedStateDict():
  __fieldsPerState: typing.Dict[StateEnum, typing.List[str]] = {}
  __fullStateFields: typing.List[str] = None
  __currStateFields: typing.List[str] = None 
  __state: StateEnum
  
  def __setFieldsPerState(self, fieldsPerState: typing.Dict[StateEnum, typing.List[str]]):
    self.__fieldsPerState = fieldsPerState
    if not StateEnum.FULL in fieldsPerState:
      if not stateEnum in fieldsPerState:
        raise Exception(f"The main (complete) list of dictionary fields is not defined.")
    self.__fullStateFields = self.__fieldsPerState[StateEnum.FULL]
    for stateEnum in StateEnum:
      if not stateEnum in fieldsPerState:
        raise Exception(f"Missing state in dictionary of FixedStateDict fields. Missing: {stateEnum.name}")
      if stateEnum != StateEnum.FULL:
        stateFields = fieldsPerState[stateEnum]
        for stateField in stateFields:
          if not stateField in self.__fullStateFields:
            raise Exception(f"A field of subset {stateEnum.name} is not available in complete list of fields.\n" + \
                            f"Field name causing the issue: {stateField}" + \
                            f"Fields of each state must be a subset of the complete list of fields.")
  def __setupFields(self):
    self.data: typing.Dict[str, any] = {}
    for fullStateField in self.__fullStateFields:
      self.data.__setitem__(fullStateField, None)
  def __setState(self, state: StateEnum.FULL): 
    self.__state = state

  @property
  def state(self) -> StateEnum:
    return self.__state
  @state.setter
  def state(self, state: StateEnum):
    self.__setState(state)
    self.__currStateFields = self.__fieldsPerState[self.state]
    if self.state == StateEnum.FULL:
      self.stateData = None
    else:
      self.stateData = {k: self.data[k] for k in self.__currStateFields}
    
  def __init__(self, fieldsPerState: typing.Dict[StateEnum, typing.List[str]]):
    self.__setFieldsPerState(fieldsPerState = fieldsPerState)
    self.__setupFields()
    self.state = StateEnum.FULL
    self.stateData: typing.Dict[str, any] = None

  def hasKey(self, key: str) -> bool:
    if self.stateData is None: return key in self.data
    else: return key in self.stateData
  def __setitem__(self, __key: str, __value: any):
    if not __key in self.__currStateFields: 
      raise Exception(f"FixedDict KeyError __setitem__: no such key specified in fields: {__key}")
    if isinstance(self.data[__key], typing.Dict):
      raise Exception(f"FixedDict KeyError __setitem__: trying to set value to {__key} containing a dictionary")
    self.data.__setitem__(__key, __value)
    if not self.stateData is None: self.stateData.__setitem__(__key, __value)
  def __getitem__(self, __key: str) -> any:
    if not __key in self.__currStateFields: 
      raise Exception(f"FixedDict KeyError __getitem__: no such key specified in fields: {__key}")
    else:
      return self.data.__getitem__(__key)
  def __delitem__(self, __key: str) -> None:
    raise Exception(f"FixedDict DelError: non-permissible delete. Fields are predefined.")
  def reset(self, __key: str) -> None:
    self.data.__setitem__(__key, None)
    if not self.stateData is None: self.stateData.__setitem__(__key, None)
  def resetAll(self) -> None:
    for key in self.data.keys(): self.data[key] = None
    if not self.stateData is None: 
      for key in self.stateData.keys(): self.stateData[key] = None

  def values(self):
    if self.stateData is None: return self.data.values()
    else: return self.stateData.values()
  def items(self):
    if self.stateData is None: return self.data.items()
    else: return self.stateData.items()
  def keys(self):
    if self.stateData is None: return self.data.keys()
    else: return self.stateData.keys()

  def __repr__(self) -> str: 
    if self.stateData is None: return self.data.__repr__()
    else: return self.stateData.__repr__()




class TestFixedStateDict(unittest.TestCase):
  @classmethod
  def setUpClass(self):
    self.fullFields = ['id', 'name', 'feature', 'schoolClass', 'isin']
    self.basicFields = ['id', 'name']
    self.fieldsPerState = {StateEnum.FULL: self.fullFields, 
                           StateEnum.BASIC: self.basicFields}
    class ThisFixedStateDict(FixedStateDict):
      def __init__(cls):
        super().__init__(fieldsPerState = self.fieldsPerState)
    self.testFixedStateDictClass = ThisFixedStateDict
    self.testFixedStateDict = self.testFixedStateDictClass()
    self.id = uuid.uuid4()
    self.name = 'otto'
    self.feature = 'special'
    self.schoolClass = 4
    self.isin = 47.11 
    self.id_2 = uuid.uuid4()
    self.name_2 = 'tom'
    self.feature_2 = 'ugly'
    self.schoolClass_2 = 8
    self.isin_2 = 15.08 
  @classmethod
  def tearDownClass(self):
    pass
  def test_01_full_state_initialized(self):
    self.assertEqual(self.testFixedStateDict.state, StateEnum.FULL)
    for key in self.fullFields:
      self.assertTrue(self.testFixedStateDict.hasKey(key)) 
      self.assertIsNone(self.testFixedStateDict[key]) 
  def test_02_basic_state(self):
    self.testFixedStateDict.state = StateEnum.BASIC
    self.assertEqual(self.testFixedStateDict.state, StateEnum.BASIC)
    for key in self.fullFields:
      if key in self.basicFields:
        self.assertTrue(self.testFixedStateDict.hasKey(key)) 
        self.assertIsNone(self.testFixedStateDict[key]) 
      else:
        self.assertFalse(self.testFixedStateDict.hasKey(key))
  def test_03_full_state_set_values(self):
    self.testFixedStateDict.state = StateEnum.FULL
    self.testFixedStateDict['id'] = self.id
    self.testFixedStateDict['name'] = self.name
    self.testFixedStateDict['feature'] = self.feature
    self.testFixedStateDict['schoolClass'] = self.schoolClass
    self.testFixedStateDict['isin'] = self.isin
    with self.assertRaises(Exception):
      self.testFixedStateDict['nonsense'] = 'nonsense'
    self.assertEqual(self.testFixedStateDict['id'], self.id)    
    self.assertEqual(self.testFixedStateDict['name'], self.name)    
    self.assertEqual(self.testFixedStateDict['feature'], self.feature)    
    self.assertEqual(self.testFixedStateDict['schoolClass'], self.schoolClass)    
    self.assertEqual(self.testFixedStateDict['isin'], self.isin)    
    self.assertEqual(str(self.testFixedStateDict), str(self.testFixedStateDict.data))    
  def test_04_basic_state_set_values(self):
    self.testFixedStateDict.state = StateEnum.BASIC
    self.testFixedStateDict['id'] = self.id_2
    self.testFixedStateDict['name'] = self.name_2
    with self.assertRaises(Exception):
      self.testFixedStateDict['feature'] = self.feature_2
      self.testFixedStateDict['schoolClass'] = self.schoolClass_2
      self.testFixedStateDict['isin'] = self.isin_2
      self.testFixedStateDict['nonsense'] = 'nonsense'
    self.assertEqual(self.testFixedStateDict['id'], self.id_2)    
    self.assertEqual(self.testFixedStateDict['name'], self.name_2)    
    self.assertEqual(str(self.testFixedStateDict), str(self.testFixedStateDict.stateData))    
  def test_05_full_state_delete_values(self):
    self.testFixedStateDict.state = StateEnum.FULL
    with self.assertRaises(Exception):
      del self.testFixedStateDict['id']
      del self.testFixedStateDict['name'] 
      del self.testFixedStateDict['feature'] 
      del self.testFixedStateDict['schoolClass']
      del self.testFixedStateDict['isin'] 
  def test_06_basic_state_delete_values(self):
    self.testFixedStateDict.state = StateEnum.BASIC
    with self.assertRaises(Exception):
      del self.testFixedStateDict['id']
      del self.testFixedStateDict['name'] 
      del self.testFixedStateDict['feature'] 
      del self.testFixedStateDict['schoolClass']
      del self.testFixedStateDict['isin'] 
  def test_07_full_state_reset_values(self):
    self.testFixedStateDict.state = StateEnum.FULL
    self.testFixedStateDict.reset('id')
    self.assertIsNone(self.testFixedStateDict['id'])
    self.assertIsNotNone(self.testFixedStateDict['name'])
    self.assertIsNotNone(self.testFixedStateDict['feature'])
    self.assertIsNotNone(self.testFixedStateDict['schoolClass'])
    self.assertIsNotNone(self.testFixedStateDict['isin'])
    self.testFixedStateDict.resetAll()
    self.assertIsNone(self.testFixedStateDict['id'])
    self.assertIsNone(self.testFixedStateDict['name'])
    self.assertIsNone(self.testFixedStateDict['feature'])
    self.assertIsNone(self.testFixedStateDict['schoolClass'])
    self.assertIsNone(self.testFixedStateDict['isin'])
  def test_08_basic_state_reset_values(self):
    self.testFixedStateDict.state = StateEnum.BASIC
    self.testFixedStateDict['id'] = self.id_2
    self.testFixedStateDict['name'] = self.name_2
    self.testFixedStateDict.reset('id')
    self.assertIsNone(self.testFixedStateDict['id'])
    self.assertIsNotNone(self.testFixedStateDict['name'])
    self.testFixedStateDict.resetAll()
    print()
    print(self.testFixedStateDict.data)
    print(self.testFixedStateDict.stateData)
    self.assertIsNone(self.testFixedStateDict['id'])
    self.assertIsNone(self.testFixedStateDict['name'])
    self.testFixedStateDict.state = StateEnum.FULL
    self.assertIsNone(self.testFixedStateDict['feature'])
    self.assertIsNone(self.testFixedStateDict['schoolClass'])
    self.assertIsNone(self.testFixedStateDict['isin'])


class ThisFixedStateDict(FixedStateDict):
  def __init__(cls):
    super().__init__(fieldsPerState = {StateEnum.FULL: ['id', 'name'], 
                                        StateEnum.BASIC:['id']})
def getStateDict(name: str) -> ThisFixedStateDict:
  result = ThisFixedStateDict()
  result['id'] = uuid.uuid4()
  result['name'] = name
  return result


class TestNumberedDict(unittest.TestCase):
  @classmethod
  def setUpClass(self):
    self.stateSwitch: StateSwitch = StateSwitch()
    self.fullFields = ['id', 'name']
    self.basicFields = ['id']
    self.fieldsPerState = {StateEnum.FULL: self.fullFields, 
                           StateEnum.BASIC: self.basicFields}
    self.numberedDict: NumberedDict = NumberedDict(stateSwitch = self.stateSwitch)
  @classmethod
  def tearDownClass(self):
    pass
  def test_01_adding_values(self):
    
    self.numberedDict.append(value = getStateDict('otto'))
    self.numberedDict.append(value = getStateDict('peter'))
    self.numberedDict.append(value = getStateDict('tom'))
    self.numberedDict.append(value = getStateDict('tim'))
    self.numberedDict.append(value = getStateDict('hugo'))
    count: int = 0
    for k in self.numberedDict.keys():
      self.assertEqual(k, count)
      count += 1  
  def test_02_accessing_values(self):
    self.assertEqual(self.numberedDict[0]['name'], 'otto')
    self.assertEqual(self.numberedDict[1]['name'], 'peter')
    self.assertEqual(self.numberedDict[2]['name'], 'tom')
    self.assertEqual(self.numberedDict[3]['name'], 'tim')
    self.assertEqual(self.numberedDict[4]['name'], 'hugo')
    with self.assertRaises(Exception):
      _ = self.numberedDict[-1] 
    with self.assertRaises(Exception):
      length = self.numberedDict.length
      _ = self.numberedDict[length] 
  def test_03_replacing_a_value(self):
    self.numberedDict[2] = getStateDict('jim')
    count: int = 0
    for k, v in self.numberedDict.items():
      self.assertEqual(k, count)
      count += 1  
    with self.assertRaises(Exception):
      self.numberedDict[-1] = getStateDict('not valid')
    with self.assertRaises(Exception):
      length = self.numberedDict.length
      self.numberedDict[length] = getStateDict('not valid')
  def test_04_deleting_a_value(self):
    del self.numberedDict[2] 
    self.assertEqual(self.numberedDict[0]['name'], 'otto')
    self.assertEqual(self.numberedDict[1]['name'], 'peter')
    self.assertEqual(self.numberedDict[2]['name'], 'tim')
    self.assertEqual(self.numberedDict[3]['name'], 'hugo')
    count: int = 0
    for k, v in self.numberedDict.items():
      self.assertEqual(k, count)
      count += 1  
    with self.assertRaises(Exception):
      _ = self.numberedDict[-1] 
    with self.assertRaises(Exception):
      length = self.numberedDict.length
      _ = self.numberedDict[length] 
  def test_05_non_numeric_key(self):
    with self.assertRaises(Exception):
      _ = self.numberedDict['a']
  def test_05_non_fixed_state_dict_value(self):
    with self.assertRaises(Exception):
      self.numberedDict.append("hallo")
    with self.assertRaises(Exception):
      self.numberedDict[2] = "hallo"

