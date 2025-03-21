from __future__ import annotations

from europy_db_controllers.xl.sheet.io_sht_fnc.base_class import IoWorkSheet
from europy_db_controllers.xl.sheet.io_sht_fnc import identify
from europy_db_controllers.xl.sheet.io_sht_fnc  import identify_data
from europy_db_controllers.xl.sheet.io_sht_fnc import setup
from europy_db_controllers.xl.sheet.io_sht_fnc import to_dict
from europy_db_controllers.xl.sheet.io_sht_fnc import labels_and_names
from europy_db_controllers.xl.sheet.io_sht_fnc import range_and_position

setattr(IoWorkSheet, 'identify',
        identify.identifyFnc)
setattr(IoWorkSheet, 'identifyData', 
        identify_data.identifyDataFnc)
setattr(IoWorkSheet, 'setup', 
        setup.setupFnc)
setattr(IoWorkSheet, 'toDict', 
        to_dict.toDictFnc)
setattr(IoWorkSheet, 'name', 
        property(labels_and_names.get_name_fnc))
setattr(IoWorkSheet, 'isEmptyRow', 
      range_and_position.isEmptyRowFnc)
setattr(IoWorkSheet, 'firstEmptyRow', 
        property(range_and_position.firstEmptyRowFnc))
setattr(IoWorkSheet, 'getDataRowDelimiters', 
        range_and_position.getDataRowDelimitersFnc)