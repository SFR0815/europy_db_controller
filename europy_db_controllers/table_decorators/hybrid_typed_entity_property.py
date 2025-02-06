from sqlalchemy.ext import hybrid as sqlalchemy_hyb # type: ignore
from europy_db_controllers.table_decorators.utils import object_identification


def hybrid_typed_entity_property(schema_name: str = None):
    def decorator(func):
      function_name = func.__name__ # name of function will always be the the __tablename__
      table_name = function_name
      table_path = f"{schema_name}.{function_name}"
      return_type_property_name = f"_hyb_prop_{func.__name__}_return_type" 

      print(f"[typed_hybrid_property] - applying decorator with table  path: {schema_name}.{function_name}")

      print(f"[typed_hybrid_property] does something; schema_name: {schema_name}, table_name: {table_name}")
    
      def wrapper(self, *args, **kwargs):
        print(f"[typed_hybrid_property.wrapper] called ")
        print(f"[typed_hybrid_property.wrapper] does something; schema_name: {schema_name}, table_name: {table_name}")

        class_type = object_identification.getApplicableClassDefinition(decoratorObject=self)
        if not hasattr(class_type, return_type_property_name):
          return_type = object_identification.identifyTableTypeOfName(decoratorObjectClass = class_type,
                                                                      table_name = table_name,
                                                                      schema_name = schema_name)
          print(f"[typed_hybrid_property] return_type: {return_type.__name__} ")
          setattr(class_type, return_type_property_name, return_type)
          getattr(class_type, func.__name__).fget.__annotations__['return'] = return_type
        if object_identification.isInitializedTableEntity(testObject = self):
          return func(self, *args, **kwargs)
        return None
      wrapper.__name__ = func.__name__
      wrapper.__doc__ = func.__doc__
      return sqlalchemy_hyb.hybrid_property(wrapper)
    return decorator
