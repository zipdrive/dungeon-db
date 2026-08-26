import { CellBase } from "react-spreadsheet";
import { CellContent } from "./api/model/cell";

/**
 * Creates a CellBase for a cell.
 */
export function createCell(content: CellContent): CellBase {
    if ('textEntry' in content) {
        // Text cell
        return {
            readOnly: false,
            className: `cell column${content.textEntry.cellIdentifier.columnOid}`,
            value: content.textEntry,
        };
    } else if ('integerEntry' in content) {
        // Integer cell
        return {
            readOnly: false,
            className: `cell column${content.integerEntry.cellIdentifier.columnOid}`,
            value: content.integerEntry,
        };
    } else if ('numberEntry' in content) {
        // Number cell
        return {
            readOnly: false,
            className: `cell column${content.numberEntry.cellIdentifier.columnOid}`,
            value: content.numberEntry,
        };
    } else if ('dateEntry' in content) {
        // Date cell
        return {
            readOnly: false,
            className: `cell column${content.dateEntry.cellIdentifier.columnOid}`,
            value: content.dateEntry,
        };
    } else if ('datetimeEntry' in content) {
        // Datetime cell
        return {
            readOnly: false,
            className: `cell column${content.datetimeEntry.cellIdentifier.columnOid}`,
            value: content.datetimeEntry,
        };
    } else if ('checkboxEntry' in content) {
        // Checkbox cell
        return {
            readOnly: false,
            className: `cell column${content.checkboxEntry.cellIdentifier.columnOid}`,
            value: content.checkboxEntry,
        };
    } else if ('fileEntry' in content) {
        // File cell
        return {
            readOnly: false,
            className: `cell column${content.fileEntry.cellIdentifier.columnOid}`,
            value: content.fileEntry,
        };
    } else if ('imageEntry' in content) {
        // Image cell
        return {
            readOnly: false,
            className: `cell column${content.imageEntry.cellIdentifier.columnOid}`,
            value: content.imageEntry,
        };
    } else if ('schemaLink' in content) {
        // Schema link cell
        return {
            readOnly: false,
            className: `cell column${content.schemaLink.cellIdentifier.columnOid}`,
            value: content.schemaLink,
        };
    } else if ('objectLink' in content) {
        // Object link cell
        return {
            readOnly: false,
            className: `cell column${content.objectLink.cellIdentifier.columnOid}`,
            value: content.objectLink,
        };
    } else if ('singleSelectDropdown' in content) {
        // Single-Select Dropdown cell
        return {
            readOnly: false,
            className: `cell column${content.singleSelectDropdown.cellIdentifier.columnOid}`,
            value: content.singleSelectDropdown,
        };
    } else if ('multiSelectDropdown' in content) {
        // Multi-Select Dropdown cell
        return {
            readOnly: false,
            className: `cell column${content.multiSelectDropdown.cellIdentifier.columnOid}`,
            value: content.multiSelectDropdown,
        };
    } else {
        // Readonly cell
        return {
            readOnly: true,
            className: `cell column${content.readonly.cellIdentifier.columnOid}`,
            value: content.readonly,
        };
    }
}