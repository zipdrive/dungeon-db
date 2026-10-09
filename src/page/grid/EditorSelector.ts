import { CellEditorSelectorResult } from 'ag-grid-community';
import { AdhocSingleSelectCellEditor } from './editor/AdhocSingleSelectCellEditor';
import { AdhocMultiSelectCellEditor } from './editor/AdhocMultiSelectCellEditor';
import { TableCell, TableCellContent, TableRowLabel } from '../../api/model/tableRow';

export function selectEditor(cell: TableCell, dropdownValues: {[tableOid: string]: TableRowLabel[]}, onError: (e: unknown) => void): CellEditorSelectorResult {
    const content: TableCellContent = cell.content;
    if ('text' in content) {
        return {
            component: 'agLargeTextCellEditor',
            params: {
                maxLength: 524288
            },
            popup: true,
        };
    } else if ('integer' in content || 'number' in content) {
        return {
            component: 'agNumberCellEditor'
        };
    } else if ('boolean' in content) {
        return {
            component: 'agCheckboxCellEditor'
        };
    } else if ('date' in content || 'datetime' in content) {
        return {
            component: 'agDateCellEditor',
            params: {
                includeTime: 'datetime' in content
            }
        };
    } else if ('singleSelectDropdown' in content) {
        return {
            component: AdhocSingleSelectCellEditor,
            params: {
                singleSelectDropdown: content.singleSelectDropdown,
                dropdownValues: content.singleSelectDropdown.tableOid.toString() in dropdownValues ? dropdownValues[content.singleSelectDropdown.tableOid.toString()] : [],
                onError,
            }
        };
    } else if ('multiSelectDropdown' in content) {
        return {
            component: AdhocMultiSelectCellEditor,
            params: {
                multiSelectDropdown: content.multiSelectDropdown,
                dropdownValues: content.multiSelectDropdown.tableOid.toString() in dropdownValues ? dropdownValues[content.multiSelectDropdown.tableOid.toString()] : [],
                onError,
            }
        };
    } else {
        return {
            component: 'agTextCellEditor'
        };
    }
}