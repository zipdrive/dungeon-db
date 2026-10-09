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
import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { ColumnHeaderRenderer } from "./grid/renderer/ColumnHeaderRenderer";
import { AddNewColumnButton } from "./grid/ColDef";
import { selectRenderer } from "./grid/RendererSelector";
import { selectEditor } from "./grid/EditorSelector";
import { getValue } from "./grid/ValueGetter";
import { editCellContents } from "./grid/CellEditRequest";

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
    const [columnList, setColumnList] = useState<TableColumnMetadata[]>([]);

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
                            executeAsync({
                                editTableColumnMetadata: {
                                    metadata: {
                                        ...metadata,
                                        size: Math.floor(column.getActualWidth())
                                    }
                                }
                            })
                        } catch (e) {
                            props.onError(e);
                        }
                    }
                }
            }
        }
    }, [columnList, props.onError]);


    const [rowList, setRowList] = useState<TableRow[]>([]);
    const [isRowListPending, startRowListTransition] = useTransition();

    /**
     * Updates the columns and rows of the table.
     */
    const updateTable = useCallback(() => {
        startRowListTransition(async () => {
            const tempColumnList: TableColumnMetadata[] = [];
            const tempRowList: TableRow[] = [];
            try {
                await queryAsync({
                    tableCells: {
                        tableOid: props.tableOid,
                        columnChannel: new Channel<TableColumnMetadata>((item) => {
                            tempColumnList.push(item);
                        }),
                        rowChannel: new Channel<TableRow>((item) => {
                            tempRowList.push(item);
                        })
                    }
                });
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
                    fixedParentDatasource: null
                }
            });
        } catch (e) {
            props.onError(e);
        }
    }, [props.tableOid, props.onError]);

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
        
        const unlistenTable = listen<number[]>('table', (event) => {
            if (event.payload.indexOf(props.tableOid) >= 0) {
                updateTable();
            }
        });

        return () => {
            unlistenTable.then(f => f());
        };
    }, [props.tableOid, props.onError]);

    
    // Load in dropdown values
    const [dropdownValues, setDropdownValues] = useState<{[tableOid: string]: TableRowLabel[]}>({});
    // TODO


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
                            })
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

                let columnType: string;
                if ('singleSelect' in columnMetadata.columnType) {
                    columnType = `singleSelectDropdown${columnMetadata.columnType.singleSelect.tableOid}`;
                } else if ('multiSelect' in columnMetadata.columnType) {
                    columnType = `multiSelectDropdown${columnMetadata.columnType.multiSelect.tableOid}`;
                } else {
                    columnType = 'any';
                }
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
                                return selectRenderer(cell, props.onRequestOpenDrillDownReport, props.onRequestOpenObject);
                            }
                        }
                        return undefined;
                    },
                    cellEditorSelector({ data }) {
                        if (data) {
                            if (key in data) {
                                const cell: TableCell = data[key];
                                return selectEditor(cell, dropdownValues, props.onError);
                            }
                        }
                        return {
                            component: 'agTextCellEditor'
                        };
                    },
                    valueGetter({ data }) {
                        if (data) {
                            if (key in data) {
                                const content: TableCellContent = data[key].content;
                                return getValue(content);
                            }
                        }
                        return null;
                    }
                };
            }),
            addNewColumn
        ];
    }, [columnList, props.tableOid, dropdownValues, props.onError]);

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