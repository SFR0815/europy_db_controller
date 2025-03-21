def internalGetHeadDepthFnc(self) -> int:
  if self.__isMain(): return 1
  else: return self._parentColControl.__getHeadDepth() + 1