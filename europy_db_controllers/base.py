from __future__ import annotations
import datetime, typing, uuid
import sqlalchemy
from sqlalchemy import orm as sqlalchemy_orm
from sqlalchemy.ext import declarative as sqlalchemy_decl
from sqlalchemy.ext import hybrid as sqlalchemy_hyb

PARENT_ATTRIBUTE_TO_DELETE_IF_NO_CHILD = "parent_attribute"
CHILDREN_LIST_ATTRIBUTE_OF_PARENT_TO_DELETE_IF_NO_CHILD = "children_list_attribute"


class Base(sqlalchemy_orm.DeclarativeBase):
  # source: https://medium.com/@alanhamlett/part-1-sqlalchemy-models-to-json-de398bc2ef47
  __abstract__ = True
  _exclude_from_json: typing.List[str] = []
  _display_lists: typing.List[str] = []
  _is_part_of_list_of: typing.List[str] = [] 
  _show_id_of: typing.List[str] = []
  _sorted_by: typing.List[str] = None 
  # pls provide description
  _hyb_props_replacing_columns: typing.Dict[str, str] = {}
  # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
  # Dictionary of attributes of parent entities that will be deleted upon deletion of 
  #       the entity if the entity is the parent's last child
  #       E.g.: A parent has a list of children. There is just one child present.
  #             If this only child is deleted, the parent is deleted as well.
  #       Usage: (a) In case of a lot - if there is only one (opening) transaction is defined,
  #              the deletion of such transaction makes the lot itself obsolete.
  #              (b) There are assets defined and projects using these assets. The usage of 
  #              the assets by a project is documented in a separate project asset table.
  #              There may be 'n' transactions in a project on some action. If there is only
  #              one transaction of some asset on a project, the deletion of such transaction 
  #              results in the project asset being obsolete as the project doesn't use it
  #              any longer.
  #       Definition standard: 
  #              keys: consecutive integer numbers (first missing integer in line interrupts the application)
  #                    The first key is '0' by standard.
  #              values: Dictionary of two string values - 
  #                         [PARENT_ATTRIBUTE_TO_DELETE_IF_NO_CHILD] the name of the attribute of the parent 
  #                                                                  within the class of the children
  #                         [CHILDREN_LIST_ATTRIBUTE_OF_PARENT_TO_DELETE_IF_NO_CHILD] the name of the attribute
  #                                                                  of the list of children within the class
  #                                                                  of the parents
  #              comments: - The defined parent/children relationships are worked off by the position of the 
  #                          integer key of their entry. This ensures that those relationships are removed first,
  #                          that could cause integrity issues when deleting others.
  #      Example:
  #             _parents_to_delete_if_no_child = {0: {base.PARENT_ATTRIBUTE_TO_DELETE_IF_NO_CHILD: "project_asset_position",
  #                                                   base.CHILDREN_LIST_ATTRIBUTE_OF_PARENT_TO_DELETE_IF_NO_CHILD: "market_transactions"},
  #                                               1: {base.PARENT_ATTRIBUTE_TO_DELETE_IF_NO_CHILD: "project_asset",
  #                                                   base.CHILDREN_LIST_ATTRIBUTE_OF_PARENT_TO_DELETE_IF_NO_CHILD: "market_transactions"}}
  #      Relevant code: see method 'deleteFromDb' as specified on class 'CapsuleBase' in module 'entity_capsules._capsule_base'.
  _parents_to_delete_if_no_child: typing.Dict[int, typing.Dict[str, str]] = {}


  modified_at: sqlalchemy_orm.Mapped[datetime.date] = sqlalchemy_orm.mapped_column(
                                          type_ = sqlalchemy.TIMESTAMP,
                                          nullable = False,
                                          default = datetime.datetime.now)
  created_at: sqlalchemy_orm.Mapped[datetime.datetime] = sqlalchemy_orm.mapped_column(
                                          type_ = sqlalchemy.TIMESTAMP,
                                          nullable = False,
                                          default = datetime.datetime.now)
  _changeTrackFields = ['modified_at', 'created_at'] 



# def typed_hybrid_property(table_name: str):
#     def decorator(func):
#         RETURN_TYPE_PROPERTY_NAME = f"_hyb_prop_{func.__name__}_return_type"
        
#         def wrapper(self, *args, **kwargs):
#             # Get the actual class, whether we're called on instance or class
#             class_type = self if isinstance(self, type) else self.__class__
            
#             if not hasattr(class_type, RETURN_TYPE_PROPERTY_NAME):
#                 # Get the registry and find the return type class
#                 registry = class_type.registry
#                 return_type = registry.metadata.tables[table_name].class_
#                 # Store the type on the class
#                 setattr(class_type, RETURN_TYPE_PROPERTY_NAME, return_type)
#                 # Update the function's return type annotation
#                 func.__annotations__['return'] = return_type
#             return func(self, *args, **kwargs)
        
#         # Preserve the original function's metadata
#         wrapper.__name__ = func.__name__
#         wrapper.__doc__ = func.__doc__
        
#         # Apply the hybrid_property decorator
#         return sqlalchemy_hyb.hybrid_property(wrapper)
#     return decorator

def typed_hybrid_property(table_path: str):
    def decorator(func):
        print(f"[typed_hybrid_property] - applying decorator with table  path: {table_path}")
        RETURN_TYPE_PROPERTY_NAME = f"_hyb_prop_{func.__name__}_return_type"
        parts = table_path.split('.')
        if len(parts) == 2:
            schema_name, table_name = parts[0], parts[1]
        else:
            schema_name, table_name = 'default', table_path

        print(f"[typed_hybrid_property] does something; schema_name: {schema_name}, table_name: {table_name}")
     
        def wrapper(self, *args, **kwargs):
            print(f"[typed_hybrid_property.wrapper] calle ")
            print(f"[typed_hybrid_property.wrapper] does something; schema_name: {schema_name}, table_name: {table_name}")

            class_type = self if isinstance(self, type) else self.__class__
            if not hasattr(class_type, RETURN_TYPE_PROPERTY_NAME):
                print(f"[typed_hybrid_property] setting return_type: {class_type.__name__}.{func.__name__}")
                registry = class_type.registry
                if table_path not in registry.metadata.tables:
                    err_msg = f"Table '{table_path}' not found for class {class_type.__name__}\n"
                    err_msg += "Available tables:\n"
                    for t in registry.metadata.tables:
                        err_msg += f"  - {t}" + "\n"
                    raise KeyError(err_msg)
                return_type = None
                for mapper in registry.mappers:
                    table_args = mapper.class_.__table_args__ if hasattr(mapper.class_, '__table_args__') else ()
                    table_args_dict = next((arg for arg in table_args if isinstance(arg, dict)), {}) \
                      if isinstance(table_args, tuple) else table_args if isinstance(table_args, dict) else {}
                    mapper_schema = table_args_dict.get('schema', 'default')
                    print(f"[typed_hybrid_property] mapper: {mapper.class_.__tablename__} " + \
                          f"- {mapper.class_.__name__} (schema: {mapper_schema})")
                    if mapper_schema == schema_name and mapper.class_.__tablename__ == table_name: 
                      return_type = mapper.class_
                      break 
                if return_type is None:
                    err_msg = f"Could not find table '{table_path}' in registry.\n"
                    err_msg += "Available tables by schema:\n"
                    tables_by_schema = {}
                    for mapper in registry.mappers:
                        table_args = mapper.class_.__table_args__ if hasattr(mapper.class_, '__table_args__') else ()
                        table_args_dict = next((arg for arg in table_args if isinstance(arg, dict)), {}) \
                          if isinstance(table_args, tuple) else table_args if isinstance(table_args, dict) else {}
                        schema = table_args_dict.get('schema', 'default')
                        if schema not in tables_by_schema:
                            tables_by_schema[schema] = []
                        tables_by_schema[schema].append(mapper.class_.__tablename__)
                    
                    for schema in sorted(tables_by_schema.keys()):
                        err_msg += f"  {schema}:\n"
                        for table in sorted(tables_by_schema[schema]):
                            err_msg += f"    - {table}\n"
                    raise KeyError(err_msg)
                print(f"[typed_hybrid_property] return_type: {return_type.__name__} ")
                setattr(class_type, RETURN_TYPE_PROPERTY_NAME, return_type)
                getattr(class_type, func.__name__).fget.__annotations__['return'] = return_type

            if hasattr(self, '__class__') and isinstance(self, class_type):
                return func(self, *args, **kwargs)
            return None
        
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        
        return sqlalchemy_hyb.hybrid_property(wrapper)
    return decorator

def hybrid_id_property(source_attribute: str, 
                             source_schema: str = None, 
                             is_id_of_source: bool = False):
    """Decorator for creating standardized hybrid properties linked to other table entities
    
    Args:
        attribute_path: String representing the path to the desired attribute (e.g. 'client_entity_asset.asset_id')
    """
    def decorator(func):
        function_name = func.__name__

        @sqlalchemy_hyb.hybrid_property
        def getter(self) -> uuid.UUID:
            # if not isinstance(self, self.__class__):
            #     return None
            if isinstance(self, sqlalchemy.orm.decl_api.DeclarativeAttributeIntercept):
                print(f"[DEBUG]  returning none")
                return None
            print(f"[DEBUG] Current instance: {self}, Class type: {self.__class__}")    
            # Navigate through the attribute path
            try:
                source_attr = getattr(self, source_attribute)
            except AttributeError:
                raise Exception(f"Attribute '{source_attribute}' not found on class '{self.__class__.__name__}'")

            #  DEBUG
            # ************************************************************************
            print(f"[DEBUG] Retrieved source_attr: {source_attr}")
              # Check if source_attr is a DeclarativeAttributeIntercept
            if isinstance(source_attr, sqlalchemy.orm.attributes.InstrumentedAttribute):
                # Debugging: Print the state of the source_attr
                print(f"[DEBUG] source_attr is an InstrumentedAttribute: {source_attr}")
                if hasattr(source_attr, 'impl') and source_attr.impl is not None:
                    print(f"[DEBUG] getting source_attr as an DeclarativeAttributeIntercept: {source_attr}")
                else:
                    print("[DEBUG] source_attr.impl is None or does not exist.")
            else:
                print(f"[DEBUG] source_attr is not an InstrumentedAttribute: {source_attr}")
            # ************************************************************************
            

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

        # Set the name of the property to match the original function
        getter.__name__ = function_name #+ "_id"
        setter.__name__ = function_name #+ "_id"

        # @typed_hybrid_property(table_path = source_schema + "." + function_name)
        # def entity_getter(self):
        #     source_attr = getattr(self, source_attribute)
        #     if source_attr is None:
        #         return None
        #     return getattr(source_attr, function_name)
        # entity_getter.__name__ = function_name

        # Return both getter and entity_getter as a tuple
        return getter #, entity_getter
    return decorator

def add_typed_hybrid_property_with_id(sqlalchemyTableType: sqlalchemy_decl.DeclarativeMeta,
                                      property_name: str,
                                      source_attribute: str, 
                                      source_schema: str, 
                                      is_id_of_source: bool = False):
  print(f"[add_typed_hybrid_property_with_id] #1: {property_name}")  
  # Create the ID property using the actual getter function
  def id_getter(self):
      pass  # The actual implementation is provided by the decorator
  id_getter.__name__ = f"{property_name}_id"
  
  print(f"[add_typed_hybrid_property_with_id] #2: {property_name}")  
  hyb_id_getter = hybrid_id_property(source_attribute=source_attribute,
                    source_schema=source_schema,
                    is_id_of_source=is_id_of_source)(id_getter)
  setattr(sqlalchemyTableType, f"{property_name}_id", hyb_id_getter)
  
  print(f"[add_typed_hybrid_property_with_id] #3: {property_name}")  
  # Create the table property with proper type information
  def entity_getter(self):  
      try:
        source = getattr(self, source_attribute)
      except AttributeError:
        raise Exception(f"Attribute '{source_attribute}' not found on class '{self.__class__.__name__}'")
      if source is None:
          return None
      try:
        result = getattr(source, property_name)
      except AttributeError:
        raise Exception(f"Attribute '{property_name}' not found on source attribute '{source_attribute}' of class '{self.__class__.__name__}'")
      return result
  entity_getter.__name__ = property_name
  hyb_entity_getter = typed_hybrid_property(table_path=f"{source_schema}.{property_name}")(entity_getter)   
  setattr(sqlalchemyTableType, property_name, hyb_entity_getter)

  print(f"[add_typed_hybrid_property_with_id] #4: {property_name}")  
  # Print verification of property creation and return types
  id_property = getattr(sqlalchemyTableType, f'{property_name}_id')
  table_property = getattr(sqlalchemyTableType, property_name)
  
  print(f"Created hybrid properties for {property_name}:")
  print(f"  - {property_name}_id: {id_property.__annotations__.get('return', 'Unknown return type')} (ID property)")
  print(f"  - {property_name}: {table_property.__annotations__.get('return', 'Unknown return type')} (Table property)")

