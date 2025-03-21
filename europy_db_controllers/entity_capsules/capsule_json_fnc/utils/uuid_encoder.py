import json
import uuid
import datetime


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        elif isinstance(obj, (datetime.datetime, datetime.date)):
          return str(obj)
        return json.JSONEncoder.default(self, obj)
