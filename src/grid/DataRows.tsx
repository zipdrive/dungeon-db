import { CellContent } from "../api/model/cell";

/**
 * Creates an object property entry for a cell.
 */
export function cellPropertyEntry(content: CellContent, imgFileSrcs: {[fileOid: number]: string}): [`column${number}`, CellContent] {
    if ('textEntry' in content) {
        // Text cell
        return [`column${content.textEntry.cellIdentifier.columnOid}`, content];
    } else if ('integerEntry' in content) {
        // Integer cell
        return [`column${content.integerEntry.cellIdentifier.columnOid}`, content];
    } else if ('numberEntry' in content) {
        // Number cell
        return [`column${content.numberEntry.cellIdentifier.columnOid}`, content];
    } else if ('dateEntry' in content) {
        // Date cell
        return [`column${content.dateEntry.cellIdentifier.columnOid}`, content];
    } else if ('datetimeEntry' in content) {
        // Datetime cell
        return [`column${content.datetimeEntry.cellIdentifier.columnOid}`, content];
    } else if ('checkboxEntry' in content) {
        // Checkbox cell
        return [`column${content.checkboxEntry.cellIdentifier.columnOid}`, content];
    } else if ('fileEntry' in content) {
        // File cell
        return [`column${content.fileEntry.cellIdentifier.columnOid}`, content];
    } else if ('imageEntry' in content) {
        // Image cell
        if (content.imageEntry.file) {
            const fileOid: number = 'path' in content.imageEntry.file ? content.imageEntry.file.path.oid : content.imageEntry.file.blob.oid;
            if (fileOid in imgFileSrcs) {
                content.imageEntry.fileSrc = imgFileSrcs[fileOid];
            }
        }
        return [`column${content.imageEntry.cellIdentifier.columnOid}`, content];
    } else if ('schemaLink' in content) {
        // Schema link cell
        return [`column${content.schemaLink.cellIdentifier.columnOid}`, content];
    } else if ('objectLink' in content) {
        // Object link cell
        return [`column${content.objectLink.cellIdentifier.columnOid}`, content];
    } else if ('singleSelectDropdown' in content) {
        // Single-Select Dropdown cell
        return [`column${content.singleSelectDropdown.cellIdentifier.columnOid}`, content];
    } else if ('multiSelectDropdown' in content) {
        // Multi-Select Dropdown cell
        return [`column${content.multiSelectDropdown.cellIdentifier.columnOid}`, content];
    } else {
        // Readonly cell
        return [`column${content.readonly.cellIdentifier.columnOid}`, content];
    }
}