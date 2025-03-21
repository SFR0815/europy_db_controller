import typing

from sqlalchemy import orm as sqlalchemy_orm
from sqlalchemy import schema as sqlalchemy_schema
from sqlalchemy.ext import declarative as sqlalchemy_decl

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base  

CT = typing.TypeVar('CT', bound=_capsule_base.CapsuleBase)

def internalGetRelationshipDefinitionsOfColumnFnc(self,
                                          column_name: sqlalchemy_schema.Column
                                          ) -> typing.Tuple[str, 
                                                            sqlalchemy_orm.Relationship, 
                                                            sqlalchemy_decl.DeclarativeMeta,
                                                            sqlalchemy_schema.Table, 
                                                            type[CT]]:
  rel_name: str = _capsule_utils.getColumnToRelationshipName(columnName=column_name)
  rel: sqlalchemy_orm.Relationship = self._relationships[rel_name]
  rel_name, rel_decl, rel_decl_table, rel_capsule_type = \
          self.__getRelationshipDefinitions(relationship=rel)
  return (rel_name, rel, rel_decl, rel_decl_table, rel_capsule_type)