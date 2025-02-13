from sqlalchemy.ext import hybrid as sqlalchemy_hyb # type: ignore
from europy_db_controllers.table_decorators.utils import object_identification


def hybrid_typed_entity_property(source_attribute: str, 
                                 schema_name: str = None):
  def decorator(func):
    function_name = func.__name__ # name of function will always be the the __tablename__
    table_name = function_name
    table_path = f"{schema_name}.{function_name}"
    return_type_property_name = f"_hyb_prop_{func.__name__}_return_type" 
    
    @sqlalchemy_hyb.hybrid_property
    def getter(self):
      class_type = object_identification.getApplicableClassDefinition(decoratorObject=self)
      if not hasattr(class_type, return_type_property_name):
        return_type = object_identification.identifyTableTypeOfName(decoratorObjectClass = class_type,
                                                                    table_name = table_name,
                                                                    schema_name = schema_name)
        setattr(class_type, return_type_property_name, return_type)
        getattr(class_type, func.__name__).fget.__annotations__['return'] = return_type
      if not object_identification.isInitializedTableEntity(testObject = self):
        return None
      try:
        source_attr = getattr(self, source_attribute)
      except AttributeError:
        raise Exception(f"Attribute '{source_attribute}' not found on class '{self.__class__.__name__}'")
      if source_attr is None:
        return None
      try:
        result = getattr(source_attr, function_name)
      except AttributeError:
        raise Exception(f"Attribute '{function_name}' not found on class '{source_attr.__class__.__name__}'")
      return result
    @getter.setter
    def setter(self, value):
      class_type = object_identification.getApplicableClassDefinition(decoratorObject=self)
      if not hasattr(class_type, return_type_property_name):
        return_type = object_identification.identifyTableTypeOfName(decoratorObjectClass = class_type,
                                                                    table_name = table_name,
                                                                    schema_name = schema_name)
        setattr(class_type, return_type_property_name, return_type)
        getattr(class_type, func.__name__).fget.__annotations__['return'] = return_type
      if not object_identification.isInitializedTableEntity(testObject = self):
        raise Exception(f"A setter for a hybrid typed entity property can only be set on a table entity.\n" + \
                        f"  Current instance: {self}, Class type: {self.__class__}.")
      try:
        source_attr = getattr(self, source_attribute)
      except AttributeError:
        raise Exception(f"Attribute '{source_attribute}' not found on class '{self.__class__.__name__}'")
      if source_attr is None:
        raise Exception(f"Setter of {function_name} requires the {source_attribute} " + \
                        f"of an entity of type {self.__class__.__name__} to be set to a non-None value.\n")
      original_value = getattr(source_attr, function_name)
      if not original_value is None:
        raise Exception(f"Setter of {function_name} requires a None value on a table entity's current value.\n" + \
                        f"  Current instance: {self}, Class type: {self.__class__}.")
      try:
        setattr(source_attr, function_name, value)
      except AttributeError:
        raise Exception(f"Setter of '{function_name}' not found on class '{self.__class__.__name__}'")
      
    getter.__name__ = func.__name__
    getter.__doc__ = func.__doc__
    return getter
  return decorator
