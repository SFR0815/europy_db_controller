import sqlalchemy # type: ignore
from sqlalchemy import orm as sqlalchemy_orm # type: ignore

def isDeclarativeAttributeIntercept(testObject: any) -> bool:
  declarativeInterceptType = sqlalchemy_orm.decl_api.DeclarativeAttributeIntercept
  if type(testObject) is declarativeInterceptType:
    return True
  return False

def isSqlalchemyTable(testObject: any) -> bool:
  tableType = sqlalchemy.Table
  if type(testObject) is tableType:
    return True
  return False

def isInitializedTableEntity(testObject: any) -> bool:
  if isDeclarativeAttributeIntercept(testObject): return False
  if isSqlalchemyTable(testObject): return False
  declarativeBaseType = sqlalchemy_orm.DeclarativeBase
  return isinstance(testObject, declarativeBaseType)

def getTableClassType(testObject: any) -> type:
  tableType = sqlalchemy.Table
  testObjectType = type(testObject)

def getApplicableClassDefinition(decoratorObject: any) -> type:
  if isInitializedTableEntity(decoratorObject):
    return decoratorObject.__class__
  return decoratorObject

def identifyTableTypeOfName(decoratorObjectClass: type,
                            table_name: str,
                            schema_name: str) -> sqlalchemy_orm.decl_api.DeclarativeAttributeIntercept:
  registry = decoratorObjectClass.registry
  result = None
  for mapper in registry.mappers:
      table_args = mapper.class_.__table_args__ if hasattr(mapper.class_, '__table_args__') else ()
      table_args_dict = next((arg for arg in table_args if isinstance(arg, dict)), {}) \
        if isinstance(table_args, tuple) else table_args if isinstance(table_args, dict) else {}
      mapper_schema = table_args_dict.get('schema', 'default')
      # print(f"[typed_hybrid_property] mapper: {mapper.class_.__tablename__} " + \
      #       f"- {mapper.class_.__name__} (schema: {mapper_schema})")
      if mapper_schema == schema_name and mapper.class_.__tablename__ == table_name: 
        result = mapper.class_
        break 
  if result is None:
      err_msg = f"Could not find table '{schema_name}.{table_name}' in registry.\n"
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
      raise Exception(err_msg)
  return result

