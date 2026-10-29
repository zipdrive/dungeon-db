import React, { useCallback, useEffect, useMemo, useReducer, useRef, useState, useTransition } from "react";
import { TableColumnMetadata } from "../api/model/tableColumn";
import { ObjectPageBreadcrumb, ReportPageBreadcrumb, TablePageBreadcrumb } from "../breadcrumb"
import { TableCell, TableCellContent, TableRow, TableRowLabel } from "../api/model/tableRow";
import { getDrillDownReportLabelAsync, getObjectLabelAsync, queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { Button, Spinner } from "@material-tailwind/react";
import { executeAsync } from "../api/action";
import { gridTheme } from "./Grid";
import { CellEditRequestEvent, ColDef, ColumnResizedEvent } from "ag-grid-community";
import classNames from "classnames";
import { Menu, MenuItem, Submenu } from "@tauri-apps/api/menu";
import { TableColumnHeaderRenderer } from "./grid/renderer/old/TableColumnHeaderRenderer";
import { AddNewColumnButton } from "./grid/ColDef";
import { selectRenderer } from "./grid/RendererSelector";
import { selectEditor } from "./grid/EditorSelector";
import { getValue } from "./grid/ValueGetter";
import { editCellContents } from "./grid/CellEditRequest";
import { TableListItem } from "../api/model/table";
import { DungeonGrid, DungeonGridCell, DungeonGridColumn, DungeonGridRow } from 'dungeon-grid-react';
import './Grid.css';
import { cellEntry } from "./grid/DungeonGridCell";
import { LogicalPosition } from "@tauri-apps/api/dpi";

type TablePageProps = Omit<TablePageBreadcrumb, 'key' | 'name'> & {
    onRequestCreateTableColumn: (tableOid: number, columnOid: number | null) => void,
    onRequestEditTableColumn: (columnMetadata: TableColumnMetadata) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

type TableGridRowData = {
    oid: number;
    index: number;
    blank: null;
} & {
    [key: `column${number}`]: TableCell;
};

export function TablePage(props: TablePageProps): React.JSX.Element {
    // Stores dropdown values used in single-select and multi-select columns
    const [dropdownValues, setDropdownValues] = useState<{[tableOid: number]: Promise<TableRowLabel[]>}>({});

    /**
     * Loads in dropdown values.
     */
    const loadDropdownValues = useCallback((tableOid: number, overwrite: boolean = true) => {
        setDropdownValues((oldDropdownValues) => {
            if (overwrite || tableOid in oldDropdownValues) {
                return {
                    ...oldDropdownValues,
                    [tableOid]: (async () => {
                        const temp: TableRowLabel[] = [];
                        try {
                            await queryAsync({
                                tableRowLabels: {
                                    tableOid,
                                    channel: new Channel<TableRowLabel>((item) => {
                                        temp.push(item);
                                    })
                                }
                            });
                        } catch (e) {
                            props.onError(e);
                        }
                        return temp;
                    })()
                };
            } else {
                return oldDropdownValues;
            }
        });
    }, [props.onError]);

    // Automatically reload dropdown values on change to table
    useEffect(() => {
        const unlistenDropdownValues = listen<TableListItem>('table', (event) => {
            if (event.payload.oid in dropdownValues) {
                loadDropdownValues(event.payload.oid);
            }
        });

        return () => {
            unlistenDropdownValues.then(f => f());
        };
    }, [dropdownValues]);



    // Stores the list of columns
    const [columnList, setColumnList] = useState<(TableColumnMetadata & { tableOid: number })[]>([]);

    /**
     * Resizes a column of the table.
     */
    const onColumnResized = useCallback(async (event: ColumnResizedEvent) => {
        if (event.finished && event.columns) {
            for (const column of event.columns) {
                if (column.getColId().startsWith('column')) {
                    const key: `column${number}` = column.getColId() as `column${number}`;
                    const metadata = columnList.find((c) => `column${c.oid}` === key);
                    if (metadata) {
                        try {
                            await executeAsync({
                                editTableColumnMetadata: {
                                    metadata: {
                                        ...metadata,
                                        size: Math.floor(column.getActualWidth())
                                    }
                                }
                            });
                        } catch (e) {
                            props.onError(e);
                        }
                    }
                }
            }
        }
    }, [columnList, props.onError]);


    // Stores the list of rows
    const [rowList, setRowList] = useState<TableRow[]>([]);
    const [isRowListPending, startRowListTransition] = useTransition();
    
    // Stores the DungeonGridCells of the grid
    const [cells, setCells] = useState<DungeonGridCell[][]>([]);
    type DispatchCellUpdateFlagAction = [{
        type: 'setFlag',
        rowOid: number,
        columnOid: number
    } | {
        type: 'unsetFlag',
        rowIndex: number,
        columnIndex: number
    }];
    const [cellUpdateFlags, dispatchCellUpdateFlag] = useReducer<Set<`row${number}column${number}`>, DispatchCellUpdateFlagAction>((prevCellUpdateFlags, action) => {
        switch (action.type) {
            case 'setFlag': {
                const newSet: Set<`row${number}column${number}`> = new Set(prevCellUpdateFlags);
                newSet.add(`row${action.rowOid}column${action.columnOid}`);
                return newSet;
            }
            case 'unsetFlag': {
                const newSet: Set<`row${number}column${number}`> = new Set(prevCellUpdateFlags);
                if (action.columnIndex !== null) {
                    const cell = rowList[action.rowIndex].cells[action.columnIndex];
                    newSet.delete(`row${cell.rowOid}column${cell.columnOid}`);
                }
                return newSet;
            }
        }
    }, new Set());
    useEffect(() => {
        setCells(rowList.map((row, rowIndex) => {
            return row.cells.map((cell, columnIndex) => {
                // Flag that the cell has been updated
                dispatchCellUpdateFlag({ type: 'setFlag', rowOid: cell.rowOid, columnOid: cell.columnOid });
                const columnMetadata = columnList[columnIndex];
                return cellEntry(
                    columnMetadata,
                    cell.content,
                    {
                        tableOid: cell.tableOid,
                        columnOid: cell.columnOid,
                        rowOid: cell.rowOid
                    },
                    dropdownValues,
                    props.onRequestOpenDrillDownReport,
                    props.onRequestOpenObject,
                    props.onRequestUploadFile,
                    props.onError
                )
            });
        }));
    }, [rowList, columnList, props.onRequestOpenDrillDownReport, props.onRequestOpenObject, props.onRequestUploadFile, props.onError]);
    useEffect(() => {
        const unlistenForCellUpdate = listen<TableCell>('tableCell', (event) => {
            const columnIndex: number = columnList.findIndex((col) => col.oid == event.payload.columnOid);
            if (columnIndex >= 0) {
                const rowIndex: number = rowList.findIndex((row) => row.cells[columnIndex].rowOid == event.payload.rowOid);
                if (rowIndex >= 0) {
                    dispatchCellUpdateFlag({ type: 'setFlag', rowOid: event.payload.rowOid, columnOid: event.payload.columnOid });
                    setCells((oldCells) => {
                        return oldCells.map((mappedRow, mappedRowIndex) => {
                            if (mappedRowIndex == rowIndex) {
                                return mappedRow.map((mappedCell, mappedColumnIndex) => {
                                    if (mappedColumnIndex == columnIndex) {
                                        if ('singleSelectDropdown' in event.payload.content) {
                                            loadDropdownValues(event.payload.content.singleSelectDropdown.tableOid, false);
                                        } else if ('multiSelectDropdown' in event.payload.content) {
                                            loadDropdownValues(event.payload.content.multiSelectDropdown.tableOid, false);
                                        }

                                        const columnMetadata = columnList[columnIndex];
                                        return cellEntry(
                                            columnMetadata,
                                            event.payload.content,
                                            {
                                                tableOid: event.payload.tableOid,
                                                columnOid: event.payload.columnOid,
                                                rowOid: event.payload.rowOid
                                            },
                                            dropdownValues,
                                            props.onRequestOpenDrillDownReport,
                                            props.onRequestOpenObject,
                                            props.onRequestUploadFile,
                                            props.onError
                                        );
                                    } else {
                                        return mappedCell;
                                    }
                                });
                            } else {
                                return mappedRow;
                            }
                        });
                    });
                }
            }
        });

        return () => {
            unlistenForCellUpdate.then(f => f());
        };
    }, [rowList, columnList, props.onRequestOpenDrillDownReport, props.onRequestOpenObject, props.onRequestUploadFile, props.onError]);

    const getCellContent = useCallback((rowIndex: number, columnIndex: number | null) => {
        if (columnIndex === null) {
            return {
                className: classNames('dngrid-cell-invisible'),
                selectable: false,
                editor: null
            };
        } else if (rowIndex < cells.length && columnIndex < cells[rowIndex].length) {
            return cells[rowIndex][columnIndex];
        } else {
            return {
                editor: null 
            };
        }
    }, [cells]);
    const getCellUpdate = useCallback((rowIndex: number, columnIndex: number | null) => {
        if (columnIndex !== null) {
            if (rowIndex < cells.length && columnIndex < cells[rowIndex].length) {
                const cell = rowList[rowIndex].cells[columnIndex];
                if (cellUpdateFlags.has(`row${cell.rowOid}column${cell.columnOid}`)) {
                    dispatchCellUpdateFlag({ type: 'unsetFlag', rowIndex, columnIndex });
                    return true;
                }
            }
        }
        return false;
    }, [rowList, cellUpdateFlags]);

    /**
     * Updates the columns and rows of the table.
     */
    const updateTable = useCallback(() => {
        startRowListTransition(async () => {
            const tempColumnList: (TableColumnMetadata & { tableOid: number })[] = [];
            const tempRowList: TableRow[] = [];
            try {
                const dropdownTableOid: Set<number> = new Set([props.tableOid]);
                await queryAsync({
                    tableCells: {
                        tableOid: props.tableOid,
                        columnChannel: new Channel<[number, TableColumnMetadata]>((item) => {
                            tempColumnList.push(Object.assign(item[1], { tableOid: item[0] }));
                            if ('singleSelect' in item[1].columnType) {
                                dropdownTableOid.add(item[1].columnType.singleSelect.tableOid);
                            } else if ('multiSelect' in item[1].columnType) {
                                dropdownTableOid.add(item[1].columnType.multiSelect.tableOid);
                            }
                        }),
                        rowChannel: new Channel<TableRow>((item) => {
                            tempRowList.push(item);
                        })
                    }
                });

                // Load dropdown values
                for (const tableOid of dropdownTableOid) {
                    loadDropdownValues(tableOid);
                }
            } catch (e) {
                props.onError(e);
            }
            startRowListTransition(() => {
                setColumnList(tempColumnList);
                setRowList(tempRowList);
            });
        });
    }, [props.tableOid, props.onError]);

    
    /**
     * Creates a new row in the table.
     */
    const createTableRow = useCallback(async (_event: React.MouseEvent) => {
        try {
            await executeAsync({
                createTableRow: {
                    tableOid: props.tableOid,
                    rowOid: null,
                    fixedParent: null
                }
            });
        } catch (e) {
            props.onError(e);
        }
    }, [props.tableOid, props.onError]);

    /**
     * Edits a cell.
     */
    const onCellEditRequest = useCallback((event: CellEditRequestEvent<TableGridRowData>) => {
        if (event.colDef.colId?.startsWith('column')) {
            const key: `column${number}` = event.colDef.colId as `column${number}`;
            const cell: TableCell = event.data[key];
            editCellContents(cell, event.newValue, props.onError);
        }
    }, []);

    // Refresh table on initialization and on signal emitted from backend
    useEffect(() => {
        updateTable();
        
        const unlistenTable = listen<TableListItem>('table', (event) => {
            if (event.payload.oid == props.tableOid) {
                updateTable();
            } else {
                loadDropdownValues(event.payload.oid);
            }
        });

        return () => {
            unlistenTable.then(f => f());
        };
    }, [props.tableOid, updateTable]);

    

    // Construct column definitions for DungeonGrid
    const columns = useMemo<DungeonGridColumn<number | null>[]>(() => {
        // Column with a button to add a new column
        const addNewColumn: DungeonGridColumn<null> = {
            key: null,
            label: {
                className: classNames('dngrid-cell-invisible'),
                component: <AddNewColumnButton
                    onClick={() => {
                        props.onRequestCreateTableColumn(props.tableOid, null);
                    }}
                />
            },
            width: 'auto'
        };

        return [
            ...columnList.map<DungeonGridColumn<number>>((columnMetadata, columnIndex) => {
                return {
                    key: columnIndex,
                    label: {
                        text: `${(columnMetadata.isPrimaryKey ? '🔑 ' : '')}${columnMetadata.name}`,
                        className: "text-center",
                        onContextMenu: async (e) => {
                            e.preventDefault();
                            const menu = await Menu.new({
                                items: [
                                    {
                                        text: "Insert Column Before",
                                        action: () => {
                                            props.onRequestCreateTableColumn(props.tableOid, columnMetadata.oid);
                                        }
                                    },
                                    {
                                        text: "Edit",
                                        action: () => {
                                            props.onRequestEditTableColumn(columnMetadata);
                                        }
                                    },
                                    {
                                        text: "Delete",
                                        action: async () => {
                                            try {
                                                await executeAsync({
                                                    trashTableColumn: {
                                                        columnOid: columnMetadata.oid,
                                                    }
                                                });
                                            } catch (e) {
                                                props.onError(e);
                                            }
                                        }
                                    },
                                    {
                                        text: "Move Before...",
                                        items: columnList.map((item) => {
                                            return {
                                                text: item.name,
                                                enabled: item.oid != columnMetadata.oid,
                                                action: async () => {
                                                    try {
                                                        await executeAsync({
                                                            moveTableColumnBefore: {
                                                                metadata: columnMetadata,
                                                                beforeColumnOid: item.oid
                                                            }
                                                        });
                                                    } catch (e) {
                                                        props.onError(e);
                                                    }
                                                }
                                            }
                                        })
                                    },
                                    {
                                        text: "Move After...",
                                        items: columnList.map((item, idx) => {
                                            const next = idx < columnList.length - 1 ? columnList[idx + 1] : null;
                                            return {
                                                text: item.name,
                                                enabled: item.oid != columnMetadata.oid,
                                                action: async () => {
                                                    try {
                                                        await executeAsync({
                                                            moveTableColumnBefore: {
                                                                metadata: columnMetadata,
                                                                beforeColumnOid: next?.oid || null
                                                            }
                                                        });
                                                    } catch (e) {
                                                        props.onError(e);
                                                    }
                                                }
                                            }
                                        })
                                    }
                                ]
                            });
                            await menu.popup(new LogicalPosition(e.clientX, e.clientY));
                        }
                    },
                    width: columnMetadata.size,
                    resizable: true,
                    onResizeColumn: async (width) => {
                        try {
                            await executeAsync({
                                editTableColumnMetadata: {
                                    metadata: {
                                        ...columnMetadata,
                                        size: width 
                                    }
                                }
                            });
                        } catch (e) {
                            props.onError(e);
                        }
                    }
                };
            }),
            addNewColumn
        ];
    }, [
        columnList, 
        props.tableOid, 
        props.onRequestCreateTableColumn, 
        props.onRequestEditTableColumn, 
        props.onError
    ]);

    // Construct row data for DungeonGrid
    const rows = useMemo<DungeonGridRow<number>[]>(() => {
        return rowList.map<DungeonGridRow<number>>((row, rowIndex) => {
            return {
                key: rowIndex,
                label: {
                    text: `${row.index}`,
                    onContextMenu: async (e) => {
                        // Display context menu for row label
                        const menu = await Menu.new({
                            items: [
                                {
                                    text: "Insert Row Above",
                                    action: async () => {
                                        try {
                                            await executeAsync({
                                                createTableRow: {
                                                    tableOid: props.tableOid,
                                                    rowOid: row.oid,
                                                    fixedParent: null
                                                }
                                            });
                                        } catch (e) {
                                            props.onError(e);
                                        }
                                    }
                                },
                                {
                                    text: "Edit",
                                    action: () => {
                                        props.onRequestOpenObject({
                                            page: 'object',
                                            name: `#${rowIndex}`,
                                            tableOid: props.tableOid,
                                            rowOid: row.oid
                                        });
                                    }
                                },
                                {
                                    text: "Delete",
                                    action: async () => {
                                        try {
                                            await executeAsync({
                                                trashTableRow: {
                                                    tableOid: props.tableOid,
                                                    rowOid: row.oid
                                                }
                                            });
                                        } catch (e) {
                                            props.onError(e);
                                        }
                                    }
                                },
                                {
                                    text: "Move Before...",
                                    items: props.tableOid in dropdownValues ? (await dropdownValues[props.tableOid]).map((item) => {
                                        return {
                                            text: item.label,
                                            enabled: row.oid != item.oid,
                                            action: async () => {
                                                try {
                                                    await executeAsync({
                                                        moveTableRowBefore: {
                                                            tableOid: props.tableOid,
                                                            rowOid: row.oid,
                                                            beforeRowOid: item.oid
                                                        }
                                                    });
                                                } catch (e) {
                                                    props.onError(e);
                                                }
                                            }
                                        };
                                    }) : []
                                },
                                {
                                    text: "Move After...",
                                    items: props.tableOid in dropdownValues ? await dropdownValues[props.tableOid].then((labels) => {
                                        return labels.map((item, idx) => {
                                            const next = idx < labels.length - 1 ? labels[idx + 1] : null;
                                            return {
                                                text: item.label,
                                                enabled: row.oid != item.oid,
                                                action: async () => {
                                                    try {
                                                        await executeAsync({
                                                            moveTableRowBefore: {
                                                                tableOid: props.tableOid,
                                                                rowOid: row.oid,
                                                                beforeRowOid: next?.oid || null
                                                            }
                                                        });
                                                    } catch (e) {
                                                        props.onError(e);
                                                    }
                                                }
                                            };
                                        });
                                    }) : []
                                },
                            ]
                        });
                        await menu.popup(new LogicalPosition(e.clientX, e.clientY));
                    }
                },
                resizable: true
            };
        });
    }, [rowList]);


    return (<div className="grid grid-col grid-rows-[calc(100vh-(var(--spacing)*20))_calc(var(--spacing)*10)] w-full">
        {columnList.map((columnMetadata) => (<style>{`.column${columnMetadata.oid}`} &#123; {columnMetadata.style} &#125;</style>))}
        <div 
            className="relative m-4 mr-8 grid grid-col grid-rows-[1fr_auto] gap-y-2"
        >
            <div className="relative overflow-auto pr-2">
                <DungeonGrid
                    rows={rows}
                    columns={columns}
                    getCellContent={getCellContent}
                    getCellUpdate={getCellUpdate}
                    style={{
                        fontFamily: 'var(--font-sans), sans-serif',
                        ["--dngrid-background-color"]: "rgb(var(--color-surface)/1)",
                        ["--dngrid-border-color"]: "rgb(var(--color-surface-dark)/1)",
                        ["--dngrid-cell-cursor-border-color"]: "rgb(var(--color-primary)/1)",
                        ["--dngrid-cell-selected-background-color"]: "color-mix(in oklch, rgb(var(--color-primary-light)/1) 25%, var(--dngrid-background-color) 75%)",
                        ["--dngrid-cell-selected-border-color"]: "color-mix(in oklch, rgb(var(--color-primary-light)/1) 50%, var(--dngrid-border-color) 50%)",
                        ["--dngrid-editor-background-color"]: "rgb(var(--color-surface-light)/1)"
                    } as React.CSSProperties}
                />
                {isRowListPending && (<div className="absolute w-full h-full flex justify-center items-center"><Spinner /></div>)}
            </div>
            <div>
                <Button
                    variant="gradient"
                    className="cursor-pointer"
                    onClick={createTableRow}
                >
                    Add New Row
                </Button>
            </div>
        </div>
        {/*
        <div className="h-10 border-t-1 border-t-[rgb(var(--color-surface-dark)/1)] bg-[rgb(var(--color-surface)/1)] flex flex-row gap-x-4 justify-center items-center">
            {pageNum == 1 ? (<div className="cursor-default">1</div>) : (<a href="#"
                className="text-[rgb(var(--color-info)/1)]"
                onClick={() => { setPageNum(1); }}
            >
                1
            </a>)}
            {pageNum > 6 && (<div className="cursor-default">...</div>)}
            {[...Array(9).keys()].map((n) => pageNum - 4 + n)
                .filter((n) => n > 1 && n < maxPageNum)
                .map((n) => {
                    if (n == pageNum) {
                        return (<div className="cursor-default">{n}</div>)
                    } else {
                        return (<a href="#"
                            className="text-[rgb(var(--color-info)/1)]"
                            onClick={() => { setPageNum(n); }}
                        >
                            {n}
                        </a>);
                    }
                })
            }
            {pageNum < maxPageNum - 5 && (<div className="cursor-default">...</div>)}
            {maxPageNum > 1 && (pageNum == maxPageNum ? (<div className="cursor-default">{maxPageNum}</div>) : (<a href="#"
                className="text-[rgb(var(--color-info)/1)]"
                onClick={() => { setPageNum(maxPageNum); }}
            >
                {maxPageNum}
            </a>))}
        </div>
        */}
    </div>);
}