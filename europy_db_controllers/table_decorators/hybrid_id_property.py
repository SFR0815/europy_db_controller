import uuid
from sqlalchemy.ext import hybrid as sqlalchemy_hyb # type: ignore
from europy_db_controllers.table_decorators.utils import object_identification

def hybrid_id_property(source_attribute: str, 
                       is_id_of_source: bool = False):
    """Decorator for creating standardized hybrid properties linked to other table entities
    
    Args:
        attribute_path: String representing the path to the desired attribute (e.g. 'client_entity_asset.asset_id')
    """
    def decorator(func):
      function_name = func.__name__ # name of function will always be the the __tablename__ plus "_id" suffix
      if not function_name.endswith("_id"):
        err_msg = f"Function name '{function_name}' must end with '_id' suffix to be used with hybrid_id_property decorator\n"
        err_msg += f"Source attribute: '{source_attribute}'\n"
        err_msg += f"Error occurred in hybrid_id_property decorator"
        raise ValueError(err_msg)

      @sqlalchemy_hyb.hybrid_property
      def getter(self) -> uuid.UUID:
        if not object_identification.isInitializedTableEntity(testObject = self):
            print(f"[DEBUG]  returning none")
            return None
        print(f"[DEBUG] Current instance: {self}, Class type: {self.__class__}")
        # Navigate through the attribute path
        try:
            source_attr = getattr(self, source_attribute)
        except AttributeError:
            raise Exception(f"Attribute '{source_attribute}' not found on class '{self.__class__.__name__}'")
        if source_attr is None:
            return None
        source_property_name = "id" if is_id_of_source else function_name
        try:
            source_attr_id = getattr(source_attr, source_property_name)
        except AttributeError:
            raise Exception(f"Attribute '{source_property_name}' not found on source attribute '{source_attribute}' of class '{self.__class__.__name__}'")
        return source_attr_id
      @getter.setter
      def setter(self, value):
        raise Exception(f"setter of {function_name} must be implemented on capsule of {source_attribute} (or below) as specific attribute")
      getter.__name__ = function_name 
      setter.__name__ = function_name 
      return getter 
    return decorator
