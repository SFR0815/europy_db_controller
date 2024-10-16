from __future__ import annotations

import sys, uuid, typing, datetime, textwrap
from sqlalchemy import orm as sqlalchemy_orm
import sqlalchemy

sys.path.insert(0, '..\..\..')

from db_controllers.entity_capsules import capsules, _capsule_utils 
from db_controllers.xl import io_wkb 
from db_controllers import _controller_base, _controller_attr, controller_head, \
                            _controller_obj_setup, _controller_json, _controller_utils
from db_controllers.entity_capsules.specific_capsule_attr.asset import asset as spec_asset

C = typing.TypeVar("C", bound=_controller_base.ControllerBase)

GET_BASE_ASSETS_FNC_NAME = "getBaseAssets"

def addAttributes(assetClassificationController: typing.Type[C]) -> None:

  def getBaseAssetsFnc(self: C,
                       scope: _controller_base.ControllerDataScopes = \
                         _controller_base.ControllerDataScopes.ALL) -> typing.Dict[str, capsules.AssetCapsule]:
    result = dict[str, capsules.AssetCapsule]()
    for asset in self.assets(scope = scope):
      if getattr(asset, spec_asset.IS_ISOMORPHIC_BASE_ASSET_PROP_NAME):
        result[asset.name] = asset
    return result
  setattr(assetClassificationController, GET_BASE_ASSETS_FNC_NAME, getBaseAssetsFnc)