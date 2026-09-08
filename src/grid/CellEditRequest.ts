import { executeAsync } from "../api/action";
import { CellContent, CellIdentifier } from "../api/model/cell";

export function editCellContents(content: CellContent, value: any, onError: (e: unknown) => void) {
    let promise: Promise<void>;
    let cellIdentifier: CellIdentifier;
    if ('textEntry' in content) {
        // Text cell
        promise = executeAsync({
            editCellContents: {
                tableOid: content.textEntry.dataTableOid,
                columnOid: content.textEntry.dataColumnOid,
                rowOid: content.textEntry.dataRowOid,
                value: {
                    text: typeof value === 'string' ? (value ?? null) : null
                }
            }
        });
        cellIdentifier = content.textEntry.cellIdentifier;
    } else if ('integerEntry' in content) {
        // Integer cell
        promise = executeAsync({
            editCellContents: {
                tableOid: content.integerEntry.dataTableOid,
                columnOid: content.integerEntry.dataColumnOid,
                rowOid: content.integerEntry.dataRowOid,
                value: {
                    integer: typeof value === 'number' ? Math.floor(value) : (typeof value === 'string' && Number.isFinite(parseInt(value)) ? parseInt(value) : null)
                }
            }
        });
        cellIdentifier = content.integerEntry.cellIdentifier;
    } else if ('numberEntry' in content) {
        // Number cell
        promise = executeAsync({
            editCellContents: {
                tableOid: content.numberEntry.dataTableOid,
                columnOid: content.numberEntry.dataColumnOid,
                rowOid: content.numberEntry.dataRowOid,
                value: {
                    number: typeof value === 'number' ? value : (typeof value === 'string' && Number.isFinite(parseFloat(value)) ? parseFloat(value) : null)
                }
            }
        });
        cellIdentifier = content.numberEntry.cellIdentifier;
    } else if ('dateEntry' in content) {
        // Date cell
        const date: Date | null = typeof value === 'string' ? (new Date(value) ?? null) : (value instanceof Date ? value : null);
        promise = executeAsync({
            editCellContents: {
                tableOid: content.dateEntry.dataTableOid,
                columnOid: content.dateEntry.dataColumnOid,
                rowOid: content.dateEntry.dataRowOid,
                value: {
                    date: {
                        label: date?.toISOString() ?? null
                    }
                }
            }
        });
        cellIdentifier = content.dateEntry.cellIdentifier;
    } else if ('datetimeEntry' in content) {
        // Datetime cell
        const datetime: Date | null = typeof value === 'string' ? (new Date(value) ?? null) : (value instanceof Date ? value : null);
        promise = executeAsync({
            editCellContents: {
                tableOid: content.datetimeEntry.dataTableOid,
                columnOid: content.datetimeEntry.dataColumnOid,
                rowOid: content.datetimeEntry.dataRowOid,
                value: {
                    datetime: {
                        label: datetime?.toISOString() ?? null
                    }
                }
            }
        });
        cellIdentifier = content.datetimeEntry.cellIdentifier;
    } else if ('checkboxEntry' in content) {
        // Checkbox cell
        promise = executeAsync({
            editCellContents: {
                tableOid: content.checkboxEntry.dataTableOid,
                columnOid: content.checkboxEntry.dataColumnOid,
                rowOid: content.checkboxEntry.dataRowOid,
                value: {
                    boolean: typeof value === 'boolean' ? value : null
                }
            }
        });
        cellIdentifier = content.checkboxEntry.cellIdentifier;
    } else if ('objectLink' in content) {
        // Object link cell
        promise = executeAsync({
            editCellContents: {
                tableOid: content.objectLink.dataTableOid,
                columnOid: content.objectLink.dataColumnOid,
                rowOid: content.objectLink.dataRowOid,
                value: {
                    object: {
                        linkedRowOid: 'new'
                    }
                }
            }
        });
        cellIdentifier = content.objectLink.cellIdentifier;
    } else if ('singleSelectDropdown' in content) {
        // Single-Select Dropdown cell
        promise = executeAsync({
            editCellContents: {
                tableOid: content.singleSelectDropdown.dataTableOid,
                columnOid: content.singleSelectDropdown.dataColumnOid,
                rowOid: content.singleSelectDropdown.dataRowOid,
                value: {
                    select: {
                        linkedRowOid: typeof value === 'number' ? value : null
                    }
                }
            }
        });
        cellIdentifier = content.singleSelectDropdown.cellIdentifier;
    } else if ('multiSelectDropdown' in content) {
        // Multi-Select Dropdown cell
        promise = executeAsync({
            editCellContents: {
                tableOid: content.multiSelectDropdown.dataTableOid,
                columnOid: content.multiSelectDropdown.dataColumnOid,
                rowOid: content.multiSelectDropdown.dataRowOid,
                value: {
                    multiselect: {
                        linkedRowOid: Array.isArray(value) && value.every((item) => typeof item === 'number') ? value : (typeof value === 'number' ? [value] : [])
                    }
                }
            }
        });
        cellIdentifier = content.multiSelectDropdown.cellIdentifier;
    } else {
        // Cell cannot be edited normally
        return;
    }

    promise.catch(onError);
}