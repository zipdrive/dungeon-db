import { CellContent, CellDependency, CellIdentifier, ClipboardCellData, DataCellEntry, SchemaRow, TextEntryCellContent } from "../api/model/cell";
import { ColumnGrouping, ColumnProp, ColumnRegular as RevoGridColumn, ColumnType as RevoGridColumnType, DataType, RevoGrid } from "@revolist/react-datagrid";
import { TextEntryEditor } from "./TextEntry";


/**
 * Creates an object property entry for a cell.
 */
export function cellPropertyEntry(content: CellContent): [string, CellContent] {
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


export const baseColumnTypes: {[key: string]: RevoGridColumnType} = {
    rowIndex: {
        readonly: true,
        cellTemplate(createElement, props) {
            const rowMetadata: SchemaRow | null | undefined = props.model[props.prop];

            if (rowMetadata) {
                return createElement(
                    'div',
                    rowMetadata.index.toString()
                );
            }

            return createElement('div', []);
        }
    },
    any: {
        cellTemplate(createElement, props) {
            const content: CellContent | null | undefined = props.model[props.prop];

            if (content) {
                if ('textEntry' in content) {
                    switch (content.textEntry.format) {
                        default:
                            return createElement(
                                'div',
                                {
                                    'class': [`column${content.textEntry.cellIdentifier.columnOid}`]
                                },
                                content.textEntry.label || ''
                            );
                    }
                } else if ('integerEntry' in content) {
                    return createElement(
                        'div',
                        {
                            'class': [`column${content.integerEntry.cellIdentifier.columnOid}`]
                        },
                        content.integerEntry.value?.toString() || ''
                    );
                } else if ('numberEntry' in content) {
                    return createElement(
                        'div',
                        {
                            'class': [`column${content.numberEntry.cellIdentifier.columnOid}`]
                        },
                        content.numberEntry.value?.toString() || ''
                    );
                } else if ('checkboxEntry' in content) {

                } else if ('dateEntry' in content) {

                } else if ('datetimeEntry' in content) {

                } else if ('objectLink' in content) {

                } else if ('schemaLink' in content) {

                } else if ('singleSelectDropdown' in content) {

                } else if ('multiSelectDropdown' in content) {

                } else {

                }
            }
            
            return createElement('div', []);
        },
        editor(column, save, close) {
            const content: CellContent | null | undefined = column.model[column.prop];

            if (content) {
                if ('textEntry' in content) {
                    return new TextEntryEditor(content.textEntry);
                }
            }
            
            // TODO
        }
    }
};