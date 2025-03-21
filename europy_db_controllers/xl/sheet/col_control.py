from __future__ import annotations

from europy_db_controllers.xl.sheet.col_control_fnc.base_class import ColControl
from europy_db_controllers.xl.sheet.col_control_fnc import init
from europy_db_controllers.xl.sheet.col_control_fnc import basics
from europy_db_controllers.xl.sheet.col_control_fnc import labels_and_names
from europy_db_controllers.xl.sheet.col_control_fnc import range_and_position

from europy_db_controllers.xl.sheet.col_control_fnc.internal_shared_content import is_main
from europy_db_controllers.xl.sheet.col_control_fnc.internal_shared_content import get_head_depth
from europy_db_controllers.xl.sheet.col_control_fnc.internal_shared_content import update_row_control
from europy_db_controllers.xl.sheet.col_control_fnc.internal_shared_content import get_relationship_features
from europy_db_controllers.xl.sheet.col_control_fnc.internal_shared_content import get_relationship_definitions
from europy_db_controllers.xl.sheet.col_control_fnc.internal_shared_content import get_relationship_definitions_of_column

from europy_db_controllers.xl.sheet.col_control_fnc.internal_sht_content import add_delete_control_column
from europy_db_controllers.xl.sheet.col_control_fnc.internal_sht_content import add_single_relationship
from europy_db_controllers.xl.sheet.col_control_fnc.internal_sht_content import add_list_relationship

from europy_db_controllers.xl.sheet.col_control_fnc import add_column
from europy_db_controllers.xl.sheet.col_control_fnc import add_sub_control

from europy_db_controllers.xl.sheet.col_control_fnc.write_to_file import setup_label_range
from europy_db_controllers.xl.sheet.col_control_fnc.write_to_file import setup_full_range
from europy_db_controllers.xl.sheet.col_control_fnc.write_to_file import setup_labels
from europy_db_controllers.xl.sheet.col_control_fnc.write_to_file import write_values
from europy_db_controllers.xl.sheet.col_control_fnc.write_to_file import set_formats_and_validations

from europy_db_controllers.xl.sheet.col_control_fnc.read_from_file import is_empty_col_control_row
from europy_db_controllers.xl.sheet.col_control_fnc.read_from_file import get_col_control_content
from europy_db_controllers.xl.sheet.col_control_fnc.read_from_file import identify
from europy_db_controllers.xl.sheet.col_control_fnc.read_from_file import identify_data
from europy_db_controllers.xl.sheet.col_control_fnc.read_from_file import parent_part_of_list_of_child_defs
from europy_db_controllers.xl.sheet.col_control_fnc.read_from_file import to_dict
from europy_db_controllers.xl.sheet.col_control_fnc.read_from_file import get_delete_dict


setattr(ColControl, '__init__', init.initFnc) 


setattr(ColControl, 'isList', 
        property(basics.isListFnc))


setattr(ColControl, 'tableName', 
        property(labels_and_names.tableNameFnc))
setattr(ColControl, 'label', 
        property(labels_and_names.labelFnc)) 
setattr(ColControl, 'subControlNameOfRelationshipKey', 
        labels_and_names.subControlNameOfRelationshipKeyFnc)


setattr(ColControl, 'firstRow', 
        property(range_and_position.firstRowFnc))
setattr(ColControl, 'lastRow', 
        property(range_and_position.lastRowFnc))
setattr(ColControl, 'lastCol', 
        property(range_and_position.lastColFnc))
setattr(ColControl, 'labelCellAddress', 
        property(range_and_position.labelCellAddressFnc))
setattr(ColControl, 'labelRangeAddress', 
        property(range_and_position.labelRangeAddressFnc))
setattr(ColControl, 'labelCell', 
        property(range_and_position.labelCellFnc))
setattr(ColControl, 'fullRange', 
        property(range_and_position.fullRangeFnc))
setattr(ColControl, '_width', 
        range_and_position.internalWidthFnc)
setattr(ColControl, 'getColControlRowDelimiters', 
        range_and_position.getColControlRowDelimitersFnc)


setattr(ColControl, '__isMain', 
        is_main.internalIsMainFnc)
setattr(ColControl, '__getHeadDepth', 
        get_head_depth.internalGetHeadDepthFnc)
setattr(ColControl, '__updateRowControl', 
        update_row_control.internalUpdateRowControlFnc)
setattr(ColControl, '__getRelationshipFeatures', 
        get_relationship_features.internalGetRelationshipFeaturesFnc)
setattr(ColControl, '__getRelationshipDefinitions', 
        get_relationship_definitions.internalGetRelationshipDefinitionsFnc)
setattr(ColControl, '__getRelationshipDefinitionsOfColumn', 
        get_relationship_definitions_of_column.internalGetRelationshipDefinitionsOfColumnFnc)


setattr(ColControl, '__addDeleteControlColumn', 
        add_delete_control_column.internalAddDeleteControlColumnFnc)
setattr(ColControl, '__addSingleRelationship', 
        add_single_relationship.internalAddSingleRelationshipFnc)
setattr(ColControl, '__addListRelationship', 
        add_list_relationship.internalAddListRelationshipFnc)


setattr(ColControl, '_addColumn', 
        add_column.addColumnFnc)
setattr(ColControl, '_addSubColControl', 
        add_sub_control.addSubColControlFnc)  


setattr(ColControl, '_setupLabelRange', 
        setup_label_range.setupLabelRangeFnc)
setattr(ColControl, '_setupFullRange', 
        setup_full_range.setupFullRangeFnc)
setattr(ColControl, 'setupLabels', 
        setup_labels.setupLabelsFnc)  
setattr(ColControl, 'writeValues', 
        write_values.writeValuesFnc)
setattr(ColControl, 'setFormatsAndValidations', 
        set_formats_and_validations.setFormatsAndValidationsFnc)


setattr(ColControl, 'isEmptyColControlRow', 
        is_empty_col_control_row.isEmptyColControlRowFnc)
setattr(ColControl, 'getColControlContent', 
        get_col_control_content.getColControlContentFnc)
setattr(ColControl, 'identify', 
        identify.identifyFnc)
setattr(ColControl, 'identifyData', 
        identify_data.identifyDataFnc)
setattr(ColControl, 'parentPartOfListOfChildDefs', 
        parent_part_of_list_of_child_defs.parentPartOfListOfChildDefsFnc)
setattr(ColControl, 'toDict', 
        to_dict.toDictFnc)
setattr(ColControl, 'getDeleteDict', 
        get_delete_dict.getDeleteDictFnc)







