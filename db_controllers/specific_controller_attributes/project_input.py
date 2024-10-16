from __future__ import annotations

import sys, typing, operator

from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy

sys.path.insert(0, '..\..\..')

from db_controllers.entity_capsules import capsules, _capsule_utils 
from db_controllers.xl import io_wkb 
from db_controllers import _controller_base, _controller_attr, controller_head, \
                            _controller_obj_setup, _controller_json, _controller_utils

GET_PROJECT_ASSETS_FNC_NAME = 'getAssetsOfProject'
GET_PROJECT_DIVIDENDS_FNC_NAME = 'getDividendsOfProject'


C = typing.TypeVar("C", bound=_controller_base.ControllerBase)



def addAttributes(projectInputSubcontroller: typing.Type[C]) -> None:
  def getAssetsOfProjectFnc(self: C,
                   project: capsules.ProjectCapsule) -> typing.Dict[str, capsules.AssetCapsule]:
    result: typing.Dict[str, capsules.AssetCapsule] = dict[str, capsules.AssetCapsule]()
    for marketTransaction in self.marketTransactions(scope = _controller_base.ControllerDataScopes.ALL,
                                                     filterConditions = {'project_id': project.id}):
      asset = marketTransaction.asset
      if not asset.name in result: result[asset.name] = asset
    return result
  setattr(projectInputSubcontroller, GET_PROJECT_ASSETS_FNC_NAME, getAssetsOfProjectFnc)
  def getDividendsOfProjectFnc(self: C,
                   project: capsules.ProjectCapsule) -> typing.Dict[str, capsules.DividendCapsule]:
    result: typing.Dict[str, capsules.DividendCapsule] = dict[str, capsules.DividendCapsule]()
    for dividend in self.dividends(scope = _controller_base.ControllerDataScopes.ALL,
                                   filterConditions = {'project_id': project.id}):
      result[dividend.name] = dividend
    return result
  setattr(projectInputSubcontroller, GET_PROJECT_DIVIDENDS_FNC_NAME, getDividendsOfProjectFnc)

