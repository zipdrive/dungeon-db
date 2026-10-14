import React, { useCallback, useEffect, useMemo, useRef, useState, useTransition } from "react";
import { TableColumnMetadata } from "../api/model/tableColumn";
import { ObjectPageBreadcrumb, ReportPageBreadcrumb, TablePageBreadcrumb } from "../breadcrumb"
import { TableCell, TableCellContent, TableRow, TableRowLabel } from "../api/model/tableRow";
import { queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { Button, Spinner } from "@material-tailwind/react";
import { executeAsync } from "../api/action";
import { gridTheme } from "./Grid";
import { AgGridReact } from "ag-grid-react";
import { CellEditRequestEvent, ColDef, ColumnResizedEvent } from "ag-grid-community";
import classNames from "classnames";
import { Menu, MenuItem, Submenu } from "@tauri-apps/api/menu";
import { ColumnHeaderRenderer } from "./grid/renderer/ColumnHeaderRenderer";
import { AddNewColumnButton } from "./grid/ColDef";
import { selectRenderer } from "./grid/RendererSelector";
import { selectEditor } from "./grid/EditorSelector";
import { getValue } from "./grid/ValueGetter";
import { editCellContents } from "./grid/CellEditRequest";
import { TableListItem } from "../api/model/table";
import './Grid.css';

type TablePageProps = Omit<TablePageBreadcrumb, 'key' | 'name'> & {
    onRequestCreateTableColumn: (tableOid: number, ordering: number | null) => void,
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
    const [dropdownValues, setDropdownValues] = useState<{[tableOid: number]: TableRowLabel[]}>({});

    /**
     * Loads in dropdown values.
     */
    const loadDropdownValues = useCallback((tableOid: number) => {
        (async () => {
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
        })().then((items) => {
            setDropdownValues((oldDropdownValues) => {
                return {
                    ...oldDropdownValues,
                    [tableOid]: items
                };
            });
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
            }
        });

        return () => {
            unlistenTable.then(f => f());
        };
    }, [props.tableOid, updateTable]);

    useEffect(() => {
        const unlistenTableCell = listen<TableCell>('tableCell', (event) => {
            const colIndex: number = columnList.findIndex((col) => col.oid == event.payload.columnOid);
            if (colIndex >= 0) {
                setRowList((oldRowList) => {
                    return oldRowList.map((row) => {
                        if (row.cells[colIndex].tableOid == event.payload.tableOid && row.cells[colIndex].rowOid == event.payload.rowOid) {
                            return {
                                ...row,
                                cells: row.cells.slice(0, colIndex).concat([event.payload]).concat(row.cells.slice(colIndex + 1))
                            };
                        } else {
                            return row;
                        }
                    });
                });
            }
        });

        return () => {
            unlistenTableCell.then(f => f());
        };
    }, [columnList]);

    


    // Construct column definitions for AgGrid
    const columnDefs = useMemo<ColDef<TableGridRowData>[]>(() => {
        const indexColumn: ColDef<TableGridRowData> = {
            field: 'index',
            headerName: "",
            cellClass: classNames('text-center', 'opacity-30'),
            width: 55,
            editable: false,
            pinned: 'left',
            lockPinned: true,
            onCellContextMenu: async (event) => {
                const rowOid: number | undefined = event.data?.oid;
                if (rowOid !== undefined) {
                    const menu = await Menu.new({
                        items: [
                            await MenuItem.new({
                                text: "Insert Row Above",
                                action: async () => {
                                    try {
                                        await executeAsync({
                                            createTableRow: {
                                                tableOid: props.tableOid,
                                                rowOid,
                                                fixedParent: null
                                            }
                                        });
                                    } catch (e) {
                                        props.onError(e);
                                    }
                                }
                            }),
                            await MenuItem.new({
                                text: "Edit",
                                action: () => {
                                    props.onRequestOpenObject({
                                        page: 'object',
                                        name: `#${event.data?.index}`,
                                        tableOid: props.tableOid,
                                        rowOid
                                    });
                                }
                            }),
                            await MenuItem.new({
                                text: "Delete",
                                action: async () => {
                                    try {
                                        await executeAsync({
                                            trashTableRow: {
                                                tableOid: props.tableOid,
                                                rowOid: rowOid
                                            }
                                        });
                                    } catch (e) {
                                        props.onError(e);
                                    }
                                }
                            }),
                            await Submenu.new({
                                text: "Move Before...",
                                items: props.tableOid in dropdownValues ? await Promise.all(dropdownValues[props.tableOid].map((item) => {
                                    return MenuItem.new({
                                        text: item.label,
                                        enabled: rowOid != item.oid,
                                        action: async () => {
                                            try {
                                                await executeAsync({
                                                    moveTableRowBefore: {
                                                        tableOid: props.tableOid,
                                                        rowOid,
                                                        beforeRowOid: item.oid
                                                    }
                                                });
                                            } catch (e) {
                                                props.onError(e);
                                            }
                                        }
                                    });
                                })) : []
                            }),
                            await Submenu.new({
                                text: "Move After...",
                                items: props.tableOid in dropdownValues ? await Promise.all(dropdownValues[props.tableOid].map((item, idx) => {
                                    const next = idx < dropdownValues[props.tableOid].length - 1 ? dropdownValues[props.tableOid][idx + 1] : null;
                                    return MenuItem.new({
                                        text: item.label,
                                        enabled: rowOid != item.oid,
                                        action: async () => {
                                            try {
                                                await executeAsync({
                                                    moveTableRowBefore: {
                                                        tableOid: props.tableOid,
                                                        rowOid,
                                                        beforeRowOid: next?.oid || null
                                                    }
                                                });
                                            } catch (e) {
                                                props.onError(e);
                                            }
                                        }
                                    });
                                })) : []
                            }),
                        ]
                    });
                    menu.popup();
                }
            },
        };
        const addNewColumn: ColDef<TableGridRowData> = {
            field: 'blank',
            headerName: "Add New Column",
            editable: false,
            resizable: false,
            width: 200,
            headerComponent: AddNewColumnButton,
            headerComponentParams: {
                onClick: () => {
                    props.onRequestCreateTableColumn(props.tableOid, null);
                }
            }
        };
        return [
            indexColumn,
            ...columnList.map((columnMetadata): ColDef<TableGridRowData> => {
                const key: `column${number}` = `column${columnMetadata.oid}`;

                return {
                    colId: key,
                    headerComponent: ColumnHeaderRenderer,
                    headerComponentParams: {
                        columnMetadata,
                        onRequestEditColumn: props.onRequestEditTableColumn,
                        onError: props.onError,
                    },
                    cellClass({ data }) {
                        if (data) {
                            const content: TableCellContent = data[key].content;
                            return classNames(
                                key,
                                { 'ag-allow-overflow': 'singleSelectDropdown' in content || 'multiSelectDropdown' in content }
                            );
                        }
                        return classNames(key);
                    },
                    width: columnMetadata.size,
                    resizable: true,
                    wrapText: true,
                    autoHeight: true,
                    editable({ data }) {
                        if (data) {
                            if (key in data) {
                                const content: TableCellContent = data[key].content;
                                return !('subreport' in content);
                            }
                        }
                        return false;
                    },
                    cellRendererSelector({ data }) {
                        if (data) {
                            if (key in data) {
                                const cell: TableCell = data[key];
                                return selectRenderer(cell, dropdownValues, props.onRequestOpenDrillDownReport, props.onRequestOpenObject, props.onError);
                            }
                        }
                        return undefined;
                    },
                    cellEditorSelector({ data }) {
                        if (data) {
                            if (key in data) {
                                const cell: TableCell = data[key];
                                return selectEditor(cell, dropdownValues, loadDropdownValues, props.onError);
                            }
                        }
                        return {
                            component: 'agTextCellEditor'
                        };
                    },
                    valueGetter({ data }) {
                        if (data) {
                            if (key in data) {
                                const cell: TableCell = data[key];
                                return getValue(cell);
                            }
                        }
                        return null;
                    }
                };
            }),
            addNewColumn
        ];
    }, [
        columnList, 
        props.tableOid, 
        dropdownValues, 
        loadDropdownValues,
        props.onRequestCreateTableColumn, 
        props.onRequestEditTableColumn, 
        props.onRequestOpenDrillDownReport,
        props.onRequestOpenObject, 
        props.onError
    ]);

    // Construct row data for AgGrid
    const rowData = useMemo<TableGridRowData[]>(() => {
        return rowList.map((row) => {
            return Object.fromEntries(
                ([
                    ['oid', row.oid],
                    ['index', row.index],
                    ['blank', null]
                ] as {[K in keyof TableGridRowData]: [K, TableGridRowData[K]]}[keyof TableGridRowData][])
                .concat(row.cells.map<[`column${number}`, TableCell]>((cell) => [`column${cell.columnOid}`, cell]))
            );
        });
    }, [rowList]);


    // Correct for subpixels
    const grid = useRef<AgGridReact | null>(null);
    const subPixelCorrectionDiv = useRef<HTMLDivElement | null>(null);
    useEffect(() => {
        if (subPixelCorrectionDiv.current) {
            const subPixelWidth: number = subPixelCorrectionDiv.current.clientWidth;
            const subPixelHeight: number = subPixelCorrectionDiv.current.clientHeight;
            subPixelCorrectionDiv.current.style.maxWidth = `${Math.floor(subPixelWidth)-1}px`;
            subPixelCorrectionDiv.current.style.maxHeight = `${Math.floor(subPixelHeight)-1}px`;
        }
    }, [subPixelCorrectionDiv]);

    return (<div className="grid grid-col grid-rows-[calc(100vh-(var(--spacing)*20))_calc(var(--spacing)*10)] w-full">
        {columnList.map((columnMetadata) => (<style>{`.column${columnMetadata.oid}`} &#123; {columnMetadata.style} &#125;</style>))}
        <div 
            ref={subPixelCorrectionDiv}
            className="relative m-4 mr-8 grid grid-col grid-rows-[1fr_auto] gap-y-2"
        >
            {isRowListPending ? (<Spinner />) : (<AgGridReact
                ref={grid}
                columnDefs={columnDefs}
                onColumnResized={onColumnResized}
                rowData={rowData}
                getRowId={({ data }) => data.index.toString()}
                readOnlyEdit={true}
                onCellEditRequest={onCellEditRequest}
                theme={gridTheme}
                animateRows={false}
                suppressRowVirtualisation={true}
                suppressColumnVirtualisation={true}
            />)}
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