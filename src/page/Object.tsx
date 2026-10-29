import { AgGridReact } from "ag-grid-react";
import { TableColumnMetadata } from "../api/model/tableColumn";
import { ObjectPageBreadcrumb, ReportPageBreadcrumb } from "../breadcrumb";
import { useCallback, useEffect, useMemo, useRef, useState, useTransition } from "react";
import { listen } from "@tauri-apps/api/event";
import { TableListItem } from "../api/model/table";
import { TableCell, TableCellContent, TableRow, TableRowLabel } from "../api/model/tableRow";
import { Channel } from "@tauri-apps/api/core";
import { getObjectRow, queryAsync } from "../api/query";
import { Spinner } from "@material-tailwind/react";
import { AutoSizeStrategy, CellEditRequestEvent, ColDef, ColumnResizedEvent } from "ag-grid-community";
import classNames from "classnames";
import { selectRenderer } from "./grid/RendererSelector";
import { selectEditor } from "./grid/EditorSelector";
import { SubtypeEditor } from "./grid/editor/old/SubtypeEditor";
import { getValue } from "./grid/ValueGetter";
import { editCellContents } from "./grid/CellEditRequest";
import { executeAsync } from "../api/action";
import { objectTheme } from "./Grid";

export type ObjectPageProps = ObjectPageBreadcrumb & {
    onRequestEditTableColumn: (columnMetadata: TableColumnMetadata) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

type ObjectGridRowData = {
    rowId: 'subtype',
    subtypes: TableListItem[],
    subtypeValue: number
} | {
    rowId: `column${number}`,
    columnMetadata: TableColumnMetadata & { tableOid: number },
    rowOid: number,
    content: TableCellContent
};

export function ObjectPage(props: ObjectPageProps): React.JSX.Element {
    useEffect(() => {
        console.log(props);
    }, [props]);

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



    // Stores subtypes of the table
    const [subtypes, setSubtypes] = useState<TableListItem[]>([]);

    /**
     * Load the subtypes of the object.
     */
    const updateSubtypes = useCallback(() => {
        (async () => {
            const temp: TableListItem[] = [];
            try {
                await queryAsync({
                    tableSubtypes: {
                        tableOid: props.tableOid,
                        channel: new Channel<TableListItem>((item) => {
                            temp.push(item);
                        })
                    }
                });
            } catch (e) {
                props.onError(e);
            }
            setSubtypes(temp);
        })();
    }, [props.tableOid, props.onError]);

    /**
     * Listens for possible changes in subtypes.
     */
    useEffect(() => {
        updateSubtypes();
        
        const unlistenSubtypes = listen<TableListItem>('table', (_event) => {
            updateSubtypes();
        });

        return () => {
            unlistenSubtypes.then(f => f());
        };
    }, [updateSubtypes, props.tableOid, props.onError]);




    // Stores the list of columns
    const [columnList, setColumnList] = useState<(TableColumnMetadata & { tableOid: number })[]>([]);

    // Stores the list of rows
    const [row, setRow] = useState<TableRow & { tableOid: number } | null>(null);
    const [isRowPending, startRowTransition] = useTransition();

    /**
     * Updates the columns and rows of the object.
     */
    const updateRow = useCallback(() => {
        startRowTransition(async () => {
            let tempColumnList: (TableColumnMetadata & { tableOid: number })[] = [];
            let tempRow: TableRow & { tableOid: number } | null = null;
            try {
                // Load object cells
                let objectTableOid: number;
                let tempColumnTupleList: [number, TableColumnMetadata][];
                let tempRowNoTableOid: TableRow;
                [objectTableOid, tempColumnTupleList, tempRowNoTableOid] = await getObjectRow(
                    props.tableOid,
                    props.rowOid
                );

                // Parse in columns
                tempRow = Object.assign(tempRowNoTableOid, { tableOid: objectTableOid });
                tempColumnList = tempColumnTupleList.map(([tableOid, column]) => Object.assign(column, { tableOid }));

                // Load dropdown values
                const dropdownTableOid: Set<number> = new Set();
                for (const cell of tempRow.cells) {
                    if ('singleSelectDropdown' in cell.content) {
                        dropdownTableOid.add(cell.content.singleSelectDropdown.tableOid);
                    } else if ('multiSelectDropdown' in cell.content) {
                        dropdownTableOid.add(cell.content.multiSelectDropdown.tableOid);
                    }
                }
                for (const tableOid of dropdownTableOid) {
                    loadDropdownValues(tableOid);
                }
            } catch (e) {
                props.onError(e);
            }
            startRowTransition(() => {
                setColumnList(tempColumnList);
                setRow(tempRow);
            });
        });
    }, [props.tableOid, props.rowOid, props.onError]);

    useEffect(() => {
        updateRow();

        const unlistenForTable = listen<TableListItem>('table', (event) => {
            if (event.payload.oid == props.tableOid) {
                updateRow();
            }
        });

        return () => {
            unlistenForTable.then(f => f());
        };
    }, [updateRow, props.tableOid, props.rowOid]);
    
    useEffect(() => {
        const unlistenTableCell = listen<TableCell>('tableCell', (event) => {
            const colIndex: number = columnList.findIndex((col) => col.oid == event.payload.columnOid);
            if (colIndex >= 0) {
                setRow((oldRow) => {
                    if (oldRow 
                        && oldRow.cells[colIndex].tableOid == event.payload.tableOid 
                        && oldRow.cells[colIndex].rowOid == event.payload.rowOid
                    ) {
                        return {
                            ...oldRow,
                            cells: oldRow.cells.slice(0, colIndex).concat([event.payload]).concat(oldRow.cells.slice(colIndex + 1))
                        };
                    } else {
                        return oldRow;
                    }
                });
            }
        });

        return () => {
            unlistenTableCell.then(f => f());
        };
    }, [columnList]);




    const columnDefs: ColDef<ObjectGridRowData>[] = useMemo(() => {
        return [
            {
                headerName: "Name",
                valueGetter({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {
                            return "Subtype";
                        } else {
                            const columnMetadata = data.columnMetadata;
                            return `${columnMetadata.isPrimaryKey ? '🔑 ' : ''}${columnMetadata.name}`;
                        }
                    } else {
                        return '';
                    }
                },
                cellClass: classNames('font-bold', 'py-2'),
                width: 150,
                resizable: true,
                suppressSizeToFit: true,
                editable: false,
                pinned: 'left',
                lockPinned: true,
            },
            {
                headerName: "Value",
                cellClass({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {
                            return classNames('ag-allow-overflow');
                        } else {
                            const columnMetadata = data.columnMetadata;
                            const content = data.content;
                            return classNames(
                                `column${columnMetadata.oid}`,
                                { 'ag-allow-overflow': 'singleSelectDropdown' in content || 'multiSelectDropdown' in content }
                            );
                        }
                    }
                    return '';
                },
                //flex: 1,
                resizable: false,
                wrapText: true,
                autoHeight: true,
                editable({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {
                            return true;
                        } else {
                            const cell: TableCell = {
                                tableOid: data.columnMetadata.tableOid,
                                columnOid: data.columnMetadata.oid,
                                rowOid: data.rowOid,
                                content: data.content
                            };
                            return !('subreport' in cell.content);
                        }
                    }
                    return false;
                },
                cellRendererSelector({ data }) {
                    if (data) {
                        if (data.rowId !== 'subtype') {
                            const cell: TableCell = {
                                tableOid: data.columnMetadata.tableOid,
                                columnOid: data.columnMetadata.oid,
                                rowOid: data.rowOid,
                                content: data.content
                            };
                            return selectRenderer(cell, dropdownValues, props.onRequestOpenDrillDownReport, props.onRequestOpenObject, props.onError);
                        }
                    }
                    return undefined;
                },
                cellEditorSelector({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {
                            return {
                                component: SubtypeEditor,
                                params: {
                                    subtypes,
                                    subtypeTableOid: data.subtypeValue,
                                    deferRender: true
                                }
                            };
                        } else {
                            const cell: TableCell = {
                                tableOid: data.columnMetadata.tableOid,
                                columnOid: data.columnMetadata.oid,
                                rowOid: data.rowOid,
                                content: data.content
                            };
                            return selectEditor(cell, dropdownValues, loadDropdownValues, props.onError);
                        }
                    }
                    return {
                        component: 'agTextCellEditor'
                    };
                },
                valueGetter({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {
                            return data.subtypes.find(({ oid }) => oid == data.subtypeValue)?.name ?? '';
                        } else {
                            const cell: TableCell = {
                                tableOid: data.columnMetadata.tableOid,
                                columnOid: data.columnMetadata.oid,
                                rowOid: data.rowOid,
                                content: data.content
                            };
                            return getValue(cell);
                        }
                    }
                    return null;
                }
            }
        ];
    }, [columnList, dropdownValues, props.onRequestEditTableColumn, props.onRequestOpenDrillDownReport, props.onRequestOpenObject, props.onError]);
    
    const rowData: ObjectGridRowData[] = useMemo(() => {
        if (row) {
            return ([{
                rowId: 'subtype',
                subtypes,
                subtypeValue: row.tableOid
            }] as (ObjectGridRowData | null)[])
            .concat(row.cells.map((rowCell) => {
                const columnMetadata = columnList.find((col) => col.oid == rowCell.columnOid);
                if (!columnMetadata) {
                    return null;
                }
                return {
                    rowId: `column${columnMetadata.oid}`,
                    tableOid: columnMetadata.tableOid,
                    columnMetadata,
                    rowOid: row.oid,
                    content: rowCell.content
                };
            }))
            .filter((r) => r !== null);
        } else {
            return [];
        }
    }, [row, columnList, subtypes]);



    /**
     * Edits a cell or the Object subtype.
     */
    const onCellEditRequest = useCallback((event: CellEditRequestEvent<ObjectGridRowData>) => {
        if (event.data.rowId === 'subtype') {
            const newSubtypeTableOid: number = typeof event.newValue === 'number' ? event.newValue : (typeof event.newValue === 'string' ? parseInt(event.newValue) : props.tableOid);
            executeAsync({
                editTableRowSubtype: {
                    tableOid: props.tableOid,
                    rowOid: props.rowOid,
                    inheritorTableOid: newSubtypeTableOid
                }
            }).catch(props.onError);
        } else {
            const cell: TableCell = {
                tableOid: event.data.columnMetadata.tableOid,
                columnOid: event.data.columnMetadata.oid,
                rowOid: event.data.rowOid,
                content: event.data.content
            };
            editCellContents(cell, event.newValue, props.onError);
        }
    }, [props.tableOid, props.rowOid, props.onError]);
    
    const autoSizeStrategy = useMemo<AutoSizeStrategy>(() => {
        return {
            type: 'fitGridWidth'
        }
    }, []);
    
    const onColumnResized = useCallback((event: ColumnResizedEvent) => {
        event.api.sizeColumnsToFit();
    }, []);
    
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

    return (<div 
        ref={subPixelCorrectionDiv}
        className="grid grid-col w-[calc(100%-(var(--spacing)*12))] ml-4 mr-8 mt-4 mb-5"
    >
        {columnList.map((columnMetadata) => (<style>{`.column${columnMetadata.oid}`} &#123; {columnMetadata.style} &#125;</style>))}
        {isRowPending ? <Spinner /> : (row && <AgGridReact
            ref={grid}
            columnDefs={columnDefs}
            rowData={rowData}
            getRowId={({ data }) => data.rowId}
            autoSizeStrategy={autoSizeStrategy}
            onColumnResized={onColumnResized}
            readOnlyEdit={true}
            onCellEditRequest={onCellEditRequest}
            theme={objectTheme}
            animateRows={false}
        />)}
    </div>);
}