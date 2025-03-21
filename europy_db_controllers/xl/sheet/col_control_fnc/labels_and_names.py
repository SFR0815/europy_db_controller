
def tableNameFnc(self) -> str:
  return str(self._capsuleKey)

def labelFnc(self) -> str:
  if self._parentColControl is None: 
    return self.tableName
  else:
    return f"{self._parentColControl.label}_x_{self._relationshipKey}"
 
def subControlNameOfRelationshipKeyFnc(self,
                                      relationshipKey: str) -> str:
  for key in self.subColControls:
    if self.subColControls[key]._relationshipKey == relationshipKey:
      return key
  return None
