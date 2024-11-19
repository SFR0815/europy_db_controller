from __future__ import annotations

import sys, enum
from sqlalchemy import orm as sqlalchemy_orm


HEAD_KEY = "head"
# BASIC_SPECIFICATION_KEY = "basic_specification"
# ASSET_CLASSIFICATION_KEY = "asset_classification"
# ASSET_PRICING_KEY = "asset_pricing"
# CLIENT_ADMIN_KEY = "client_admin"
# PROJECT_INPUT_KEY = "project_input"
# FIFO_DATA_KEY = "fifo_data"

# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# # enum of the scope of data provided
# class ControllerKeyEnum(enum.Enum):
#   BASIC_SPECIFICATION = BASIC_SPECIFICATION_KEY
#   ASSET_CLASSIFICATION = ASSET_CLASSIFICATION_KEY
#   ASSET_PRICING = ASSET_PRICING_KEY
#   CLIENT_ADMIN = CLIENT_ADMIN_KEY
#   PROJECT_INPUT = PROJECT_INPUT_KEY
#   FIFO_DATA = FIFO_DATA_KEY
#   @classmethod
#   def from_str(self, label: str):
#     if label in ('BASIC_SPECIFICATION', BASIC_SPECIFICATION_KEY):
#         return ControllerKeyEnum.BASIC_SPECIFICATION
#     elif label in ('ASSET_CLASSIFICATION', ASSET_CLASSIFICATION_KEY):
#         return ControllerKeyEnum.ASSET_CLASSIFICATION
#     elif label in ('ASSET_PRICING', ASSET_PRICING_KEY):
#         return ControllerKeyEnum.ASSET_PRICING
#     elif label in ('CLIENT_ADMIN', CLIENT_ADMIN_KEY):
#         return ControllerKeyEnum.CLIENT_ADMIN
#     elif label in ('PROJECT_INPUT', PROJECT_INPUT_KEY):
#         return ControllerKeyEnum.PROJECT_INPUT
#     elif label in ('FIFO_DATA', FIFO_DATA_KEY):
#         return ControllerKeyEnum.FIFO_DATA
#     else:
#         raise Exception()  

class BaseControllerKeyEnum(enum.Enum):
  pass

class ControllerDataScopes(enum.Enum):
  ALL_IN_SESSION = 0
  NEW_AND_DIRTY = 1
  STORED_ON_DB = 2
  ALL = 3

class XlControllerAction(enum.Enum):
  GET_INPUT_TEMPLATE = "input_template"
  DOWNLOAD = "data"
  UPLOAD = "upload"
  @classmethod
  def from_str(self, label: str):
    if label in ('DOWNLOAD', 'download', 'data'):
        return XlControllerAction.DOWNLOAD
    elif label in ('UPLOAD', 'upload'):
        return XlControllerAction.UPLOAD
    else:
        raise Exception()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Decorator ensuring the session to be properly closed in case of an Exception
def cleanAndCloseSession(func):
  def wrapper(*args, **kwargs):
    session: sqlalchemy_orm.Session = None
    if 'session' in kwargs: # session passed as parameter
      session = kwargs['session']
    else: 
      session = args[0].session
    try:
      result = func(*args, **kwargs)
    except Exception as e:
      session.expunge_all()
      session.close()
      raise e
    return result
  return wrapper 

class ControllerBase():
  _key = ""
  _content = []
  _subControllerTypes = []
  _headSubControllerType = None
  def __init__(self,
               session: sqlalchemy_orm.Session) -> None:
    self.session = session
  
  def _raiseException(self, errMsg: str): 
    self.session.expunge_all()
    self.session.close()
    raise Exception(errMsg)
  


      