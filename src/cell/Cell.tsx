import { CellContent, CellDependency, CellIdentifier, ClipboardCellData, DataCellEntry, SchemaRow, TextEntryCellContent } from "../api/model/cell";
import { ColumnGrouping, ColumnProp, ColumnRegular as RevoGridColumn, ColumnType as RevoGridColumnType, DataType, RevoGrid } from "@revolist/react-datagrid";
import { TextEntryEditor } from "./TextEntry";
import { FullMetadata as ColumnFullMetadata } from "../api/model/column";
import { useCallback, useMemo, useState } from "react";
import { queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { executeAsync } from "../api/action";

type RowModel = { rowMetadata: SchemaRow, blank: null } & {[key: `column${number}`]: CellContent};
export function createRowProxy(row: RowModel, onError: (e: unknown) => void): RowModel {
    return new Proxy(row, {
        get(target, prop, receiver): any {
            if (!prop.toString().startsWith('column')) {
                return Reflect.get(target, prop, receiver);
            }

            const content: CellContent = Reflect.get(target, prop, receiver);
            if ('textEntry' in content) {
                // Text cell
                return content.textEntry.label;
            } else if ('integerEntry' in content) {
                // Integer cell
                return content.integerEntry.value;
            } else if ('numberEntry' in content) {
                // Number cell
                return content.numberEntry.value;
            } else if ('dateEntry' in content) {
                // Date cell
                return content.dateEntry.label;
            } else if ('datetimeEntry' in content) {
                // Datetime cell
                return content.datetimeEntry.label;
            } else if ('checkboxEntry' in content) {
                // Checkbox cell
                return content.checkboxEntry.isChecked;
            } else if ('fileEntry' in content) {
                // File cell
                return content.fileEntry.label;
            } else if ('imageEntry' in content) {
                // Image cell
                return content.imageEntry.label;
            } else if ('schemaLink' in content) {
                // Schema link cell
                return content.schemaLink.label;
            } else if ('objectLink' in content) {
                // Object link cell
                return content.objectLink.label;
            } else if ('singleSelectDropdown' in content) {
                // Single-Select Dropdown cell
                return content.singleSelectDropdown.dropdownRowOid;
            } else if ('multiSelectDropdown' in content) {
                // Multi-Select Dropdown cell
                return content.multiSelectDropdown.dropdownRowOid;
            } else {
                // Readonly cell
                return content.readonly.label;
            }
        },
        set(target, prop, value, receiver): boolean {
            console.log(`Received set request for ${String(prop)} with value ${value}`)
            if (!String(prop).startsWith('column')) {
                return false;
            }

            const content: CellContent = Reflect.get(target, prop, receiver);

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
                            integer: typeof value === 'number' ? Math.floor(value) : null
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
                            number: typeof value === 'number' ? value : null
                        }
                    }
                });
                cellIdentifier = content.numberEntry.cellIdentifier;
            } else if ('dateEntry' in content) {
                // Date cell
                promise = executeAsync({
                    editCellContents: {
                        tableOid: content.dateEntry.dataTableOid,
                        columnOid: content.dateEntry.dataColumnOid,
                        rowOid: content.dateEntry.dataRowOid,
                        value: {
                            date: {
                                label: typeof value === 'string' ? (value ?? null) : null
                            }
                        }
                    }
                });
                cellIdentifier = content.dateEntry.cellIdentifier;
            } else if ('datetimeEntry' in content) {
                // Datetime cell
                promise = executeAsync({
                    editCellContents: {
                        tableOid: content.datetimeEntry.dataTableOid,
                        columnOid: content.datetimeEntry.dataColumnOid,
                        rowOid: content.datetimeEntry.dataRowOid,
                        value: {
                            datetime: {
                                label: typeof value === 'string' ? (value ?? null) : null
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
                return false;
            }
            promise.catch(onError);

            return true;
        }
    });
}


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

/**
 * Maps cell content to a reference to the corresponding table cell.
 */
export function cellDataRef(content: CellContent): { tableOid: number, columnOid: number, rowOid: number } | undefined {
    if ('textEntry' in content) {
        // Text cell
        return {
            tableOid: content.textEntry.dataTableOid,
            columnOid: content.textEntry.dataColumnOid,
            rowOid: content.textEntry.dataRowOid
        };
    } else if ('integerEntry' in content) {
        // Integer cell
        return {
            tableOid: content.integerEntry.dataTableOid,
            columnOid: content.integerEntry.dataColumnOid,
            rowOid: content.integerEntry.dataRowOid
        };
    } else if ('numberEntry' in content) {
        // Number cell
        return {
            tableOid: content.numberEntry.dataTableOid,
            columnOid: content.numberEntry.dataColumnOid,
            rowOid: content.numberEntry.dataRowOid
        };
    } else if ('dateEntry' in content) {
        // Date cell
        return {
            tableOid: content.dateEntry.dataTableOid,
            columnOid: content.dateEntry.dataColumnOid,
            rowOid: content.dateEntry.dataRowOid
        };
    } else if ('datetimeEntry' in content) {
        // Datetime cell
        return {
            tableOid: content.datetimeEntry.dataTableOid,
            columnOid: content.datetimeEntry.dataColumnOid,
            rowOid: content.datetimeEntry.dataRowOid
        };
    } else if ('checkboxEntry' in content) {
        // Checkbox cell
        return {
            tableOid: content.checkboxEntry.dataTableOid,
            columnOid: content.checkboxEntry.dataColumnOid,
            rowOid: content.checkboxEntry.dataRowOid
        };
    } else if ('fileEntry' in content) {
        // File cell
        return {
            tableOid: content.fileEntry.dataTableOid,
            columnOid: content.fileEntry.dataColumnOid,
            rowOid: content.fileEntry.dataRowOid
        };
    } else if ('imageEntry' in content) {
        // Image cell
        return {
            tableOid: content.imageEntry.dataTableOid,
            columnOid: content.imageEntry.dataColumnOid,
            rowOid: content.imageEntry.dataRowOid
        };
    } else if ('schemaLink' in content) {
        // Schema link cell
        return undefined;
    } else if ('objectLink' in content) {
        // Object link cell
        return {
            tableOid: content.objectLink.dataTableOid,
            columnOid: content.objectLink.dataColumnOid,
            rowOid: content.objectLink.dataRowOid
        };
    } else if ('singleSelectDropdown' in content) {
        // Single-Select Dropdown cell
        return {
            tableOid: content.singleSelectDropdown.dataTableOid,
            columnOid: content.singleSelectDropdown.dataColumnOid,
            rowOid: content.singleSelectDropdown.dataRowOid
        };
    } else if ('multiSelectDropdown' in content) {
        // Multi-Select Dropdown cell
        return {
            tableOid: content.multiSelectDropdown.dataTableOid,
            columnOid: content.multiSelectDropdown.dataColumnOid,
            rowOid: content.multiSelectDropdown.dataRowOid
        };
    } else {
        // Readonly cell
        return undefined;
    }
}


export function useBaseColumnTypes(onError: (e: unknown) => void) {
    return useMemo<{[key: string]: RevoGridColumnType}>(() => {
        return {
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
                editor(column, _save, close) {
                    const content: CellContent | null | undefined = column.model[column.prop];

                    if (content) {
                        if ('textEntry' in content) {
                            return new TextEntryEditor(content.textEntry, close, onError);
                        }
                    }
                    
                    // TODO
                }
            }
        };
    }, [onError]);
}

export function useExtraColumnTypes(columns: ColumnFullMetadata[], onError: (e: unknown) => void) {
    const [dropdownValues, setDropdownValues] = useState<{[key: `table${number}`]: { label: string, value: string }[]}>({});
    
    useCallback(() => {
        const tableOids: Set<number> = new Set();
        for (const columnMetadata of columns) {
            if ('select' in columnMetadata.columnType) {
                tableOids.add(columnMetadata.columnType.select.tableOid);
            } else if ('multiselect' in columnMetadata.columnType) {
                tableOids.add(columnMetadata.columnType.multiselect.tableOid);
            }
        }
        Promise.all([...tableOids].map<Promise<[`table${number}`, { label: string, value: string }[]]>>(async (tableOid) => {
            const tableDropdownValues: { label: string, value: string }[] = [];
            await queryAsync({
                columnValues: {
                    schemaOid: tableOid,
                    channel: new Channel((dropdownValue) => {
                        tableDropdownValues.push({ 
                            label: dropdownValue.label,
                            value: dropdownValue.value.toString()
                        });
                    })
                }
            });
            return [`table${tableOid}`, tableDropdownValues];
        }))
        .then((entries) => {
            setDropdownValues(Object.fromEntries(entries));
        })
        .catch((e) => {
            onError(e);
        });
    }, [columns]);

    const extraColumnTypes: {[key: string]: RevoGridColumnType} = useMemo(() => {
        const extras: {[key: string]: any} = {};
        for (const columnMetadata of columns) {
            if ('select' in columnMetadata.columnType) {
                const key: string = `singleSelectDropdown${columnMetadata.columnType.select.tableOid}`;
                if (!(key in extras)) {
                    extras[key] = {

                    };
                }
            } else if ('multiselect' in columnMetadata.columnType) {
                const key: string = `multiSelectDropdown${columnMetadata.columnType.multiselect.tableOid}`;
                if (!(key in extras)) {
                    extras[key] = {

                    };
                }
            }
        }
        return extras;
    }, [columns, dropdownValues]);
    return extraColumnTypes;
}