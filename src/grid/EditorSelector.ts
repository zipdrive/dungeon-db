import { CellEditorSelectorResult } from 'ag-grid-community';
import { CellContent } from "../api/model/cell";
import { AdhocSingleSelectCellEditor } from './editor/AdhocSingleSelectCellEditor';
import { AdhocMultiSelectCellEditor } from './editor/AdhocMultiSelectCellEditor';
import { DropdownValue } from '../api/query';

export function selectEditor(content: CellContent, dropdownValues: {[tableOid: string]: DropdownValue[]}, onError: (e: unknown) => void): CellEditorSelectorResult {
    if ('textEntry' in content) {
        return {
            component: 'agLargeTextCellEditor',
            params: {
                maxLength: 524288
            },
            popup: true,
        };
    } else if ('integerEntry' in content || 'numberEntry' in content) {
        return {
            component: 'agNumberCellEditor'
        };
    } else if ('checkboxEntry' in content) {
        return {
            component: 'agCheckboxCellEditor'
        };
    } else if ('dateEntry' in content || 'datetimeEntry' in content) {
        return {
            component: 'agDateCellEditor',
            params: {
                includeTime: 'datetimeEntry' in content
            }
        };
    } else if ('singleSelectDropdown' in content) {
        return {
            component: AdhocSingleSelectCellEditor,
            params: {
                singleSelectDropdown: content.singleSelectDropdown,
                dropdownValues: content.singleSelectDropdown.dropdownTableOid.toString() in dropdownValues ? dropdownValues[content.singleSelectDropdown.dropdownTableOid.toString()] : [],
                onError,
            }
        };
    } else if ('multiSelectDropdown' in content) {
        return {
            component: AdhocMultiSelectCellEditor,
            params: {
                multiSelectDropdown: content.multiSelectDropdown,
                dropdownValues: content.multiSelectDropdown.dropdownTableOid.toString() in dropdownValues ? dropdownValues[content.multiSelectDropdown.dropdownTableOid.toString()] : [],
                onError,
            }
        };
    } else {
        return {
            component: 'agTextCellEditor'
        };
    }
}