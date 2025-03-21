import typing
from sqlalchemy import orm as sqlalchemy_orm
from sqlalchemy import schema as sqlalchemy_schema
from sqlalchemy.ext import declarative as sqlalchemy_decl

from europy_db_controllers.entity_capsules import _capsule_utils, _capsule_base

CT = typing.TypeVar('CT', bound=_capsule_base.CapsuleBase)

def internalGetRelationshipDefinitionsFnc(self,
                                  relationship: sqlalchemy_orm.Relationship
                                  ) -> typing.Tuple[str, 
                                                    sqlalchemy_decl.DeclarativeMeta,
                                                    sqlalchemy_schema.Table, 
                                                    type[CT]]:
  relName: str = relationship.key
  relDecl: sqlalchemy_decl.DeclarativeMeta = relationship.mapper.class_
  relDeclTable = relDecl.__table__
  relCapsuleTypeName = _capsule_utils.getCapsuleClassName(sqlalchemyTableType = relDecl)
  for capsuleType in self._capsuleList:
    capsuleTypeName = capsuleType.__name__
    if capsuleTypeName == relCapsuleTypeName:
      # relCapsuleType = getattr(self._capsuleTypes, relCapsuleTypeName)
      relCapsuleType = capsuleType
      return (relName, relDecl, relDeclTable, relCapsuleType)