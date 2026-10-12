import { CellEditorSelectorResult } from 'ag-grid-community';
import { SingleSelectCellEditor } from './editor/SingleSelectCellEditor';
import { AdhocMultiSelectCellEditor } from './editor/MultiSelectCellEditor';
import { TableCell, TableCellContent, TableRowLabel } from '../../api/model/tableRow';
import { ReportCell } from '../../api/model/reportRow';

export function selectEditor(cell: TableCell | ReportCell, dropdownValues: {[tableOid: string]: TableRowLabel[]}, loadDropdownValues: (tableOid: number) => void, onError: (e: unknown) => void): CellEditorSelectorResult {
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
            component: SingleSelectCellEditor,
            params: {
                singleSelectDropdown: content.singleSelectDropdown,
                dropdownValues,
                loadDropdownValues,
                onError,
            }
        };
    } else if ('multiSelectDropdown' in content) {
        return {
            component: AdhocMultiSelectCellEditor,
            params: {
                multiSelectDropdown: content.multiSelectDropdown,
                dropdownValues,
                loadDropdownValues,
                onError,
            }
        };
    } else {
        return {
            component: 'agTextCellEditor'
        };
    }
}