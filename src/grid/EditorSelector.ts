import { CellEditorSelectorResult } from 'ag-grid-community';
import { CellContent } from "../api/model/cell";

export function selectEditor(content: CellContent): CellEditorSelectorResult {
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
            component: 'agDateCellEditor'
        };
    } else {
        return {
            component: 'agTextCellEditor'
        };
    }
}