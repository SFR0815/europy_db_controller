import typing

import sqlalchemy
from sqlalchemy.ext import declarative as sqlalchemy_decl

from europy_db_controllers.entity_capsules import _capsule_utils

def parentPartOfListOfChildDefsFnc(self,
                                   relationshipSqlaTable: sqlalchemy_decl.DeclarativeMeta,
                                   relationshipTable: sqlalchemy.Table) -> typing.Tuple[bool, str]:
  for column in relationshipTable.columns:
    if _capsule_utils.isRelationshipIdColumn(column=column):
      parentRelationshipName: str = _capsule_utils.getRelationshipNameOfColumn(column=column)
      parentRelationship = relationshipSqlaTable.__mapper__.relationships[parentRelationshipName]
      parentRelationshipSqlaTable = parentRelationship.mapper.class_
      parentRelationshipTable = parentRelationshipSqlaTable.__table__
      isPartOfListOf = False
      if hasattr(relationshipSqlaTable, '_is_part_of_list_of'):
        if parentRelationship.key in relationshipSqlaTable._is_part_of_list_of:
          isPartOfListOf = True and hasattr(parentRelationshipSqlaTable, 'name')
      parentNameAttributeField = _capsule_utils.getRelationshipNameFieldOfColumn(column = column)
      if isPartOfListOf and parentRelationshipTable.name == self._table.name: return (True, parentNameAttributeField)
  return (False, None)