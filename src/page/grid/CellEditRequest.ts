import { TableCell, TableCellContent } from "../../api/model/tableRow";
import { executeAsync } from "../../api/action";

export function editCellContents(cell: TableCell, value: any, onError: (e: unknown) => void) {
    try {
        const oldContent: TableCellContent = cell.content;
        let newContent: TableCellContent;
        if ('text' in oldContent) {
            newContent = {
                text: {
                    value: typeof value === 'string' ? (value ?? null) : null,
                    format: oldContent.text.format
                }
            };
        } else if ('boolean' in oldContent) {
            newContent = {
                boolean: {
                    value: typeof value === 'boolean' ? value : false
                }
            };
        } else if ('integer' in oldContent) {
            newContent = {
                integer: {
                    value: typeof value === 'number' ? Math.floor(value) : (typeof value === 'string' && Number.isFinite(parseInt(value)) ? parseInt(value) : null)
                }
            };
        } else if ('number' in oldContent) {
            newContent = {
                number: {
                    value: typeof value === 'number' ? value : (typeof value === 'string' && !Number.isNaN(parseFloat(value)) ? parseFloat(value) : null)
                }
            };
        } else if ('date' in oldContent) {
            newContent = {
                date: {
                    value: 0,
                    label: value instanceof Date ? value.toUTCString() : (typeof value === 'string' ? (value ?? null) : null)
                }
            };
        } else if ('datetime' in oldContent) {
            newContent = {
                datetime: {
                    value: 0,
                    label: value instanceof Date ? value.toUTCString() : (typeof value === 'string' ? (value ?? null) : null)
                }
            };
        } else if ('file' in oldContent) {
            newContent = {
                file: {
                    value: typeof value === 'object' && (
                        ('path' in value && 'oid' in value.path && typeof value.path.oid === 'number' && 'name' in value.path && typeof value.path.name === 'string' && 'path' in value.path && typeof value.path.path === 'string') 
                        || ('blob' in value && 'oid' in value.blob && typeof value.blob.oid === 'number' && 'name' in value.blob && typeof value.blob.name === 'string' && 'size' in value.blob && typeof value.blob.size === 'number')) ? value : null
                }
            };
        } else if ('object' in oldContent) {
            newContent = {
                object: {
                    tableOid: oldContent.object.tableOid,
                    value: typeof value === 'number' ? value : (typeof value === 'string' && Number.isFinite(parseInt(value)) ? parseInt(value) : null)
                }
            };
        } else if ('singleSelectDropdown' in oldContent) {
            newContent = {
                singleSelectDropdown: {
                    tableOid: oldContent.singleSelectDropdown.tableOid,
                    value: typeof value === 'number' ? value : (typeof value === 'string' && Number.isFinite(parseInt(value)) ? parseInt(value) : null)
                }
            };
        } else if ('multiSelectDropdown' in oldContent) {
            newContent = {
                multiSelectDropdown: {
                    tableOid: oldContent.multiSelectDropdown.tableOid,
                    value: (
                        Array.isArray(value) 
                        ? value.map((n) => typeof n === 'number' ? n : (typeof n === 'string' && Number.isFinite(parseInt(n)) ? parseInt(n) : null)) 
                        : (typeof value === 'string' ? value.split(',').map((n) => Number.isFinite(parseInt(n)) ? parseInt(n) : null) : [])
                    ).filter((oid) => oid !== null)
                }
            };
        } else {
            // Cell cannot be edited normally
            return;
        }
        
        // Edit the cell
        executeAsync({
            editTableCellContents: {
                tableOid: cell.tableOid,
                columnOid: cell.columnOid,
                rowOid: cell.rowOid,
                content: newContent
            }
        }).catch(onError);

    } catch (e) {
        onError(e);
    }
}