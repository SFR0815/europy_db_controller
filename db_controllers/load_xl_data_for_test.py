import os, inspect, sys

sys.path.insert(0, '..\..')

import sqlalchemy as sqla
from sqlalchemy import orm as sqla_orm

from db_controllers import controller, _controller_base

class TestDataDefinitions():
  client_admin_subController_key = _controller_base.ControllerKeyEnum.CLIENT_ADMIN
  asset_classification_subController_key = _controller_base.ControllerKeyEnum.ASSET_CLASSIFICATION
  asset_pricing_subController_key = _controller_base.ControllerKeyEnum.ASSET_PRICING
  project_input_subController_key = _controller_base.ControllerKeyEnum.PROJECT_INPUT
  def __init__(self,
               client_admin: str = None,
               asset_classification: str = None,
               asset_pricing: str = None,
               project_input: str = None) -> None:
    self.client_admin = client_admin
    self.asset_classification = asset_classification
    self.asset_pricing = asset_pricing
    self.project_input = project_input

  def loadData(self,
               session: sqla_orm.Session,
               xlFilePath: str) -> None:
    thisController = controller.Controller(session = session)
    for classElement in vars(self).keys():
      fileName = getattr(self, classElement)
      if fileName is None: continue
      thisExcelHead = controller.controller_head.Excel(
                      filePath=xlFilePath,
                      fileName=fileName,
                      subControllerKey = getattr(self, classElement + '_subController_key'),
                      action=_controller_base.XlControllerAction.UPLOAD)
      thisController.head.excel = thisExcelHead
      thisController.execute()
      session.commit()





