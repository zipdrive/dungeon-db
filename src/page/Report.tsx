import React, { useCallback, useEffect, useMemo, useRef, useState, useTransition } from "react";
import { ObjectPageBreadcrumb, ReportPageBreadcrumb } from "../breadcrumb"
import { TableCell, TableCellContent, TableRowLabel } from "../api/model/tableRow";
import { getReportMetadataAsync, queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { Button, Spinner } from "@material-tailwind/react";
import { executeAsync } from "../api/action";
import { gridTheme } from "./Grid";
import { AgGridReact } from "ag-grid-react";
import { CellContextMenuEvent, CellEditRequestEvent, ColDef, ColumnResizedEvent } from "ag-grid-community";
import classNames from "classnames";
import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { ColumnHeaderRenderer } from "./grid/renderer/ColumnHeaderRenderer";
import { AddNewColumnButton } from "./grid/ColDef";
import { selectRenderer } from "./grid/RendererSelector";
import { selectEditor } from "./grid/EditorSelector";
import { getValue } from "./grid/ValueGetter";
import { editCellContents } from "./grid/CellEditRequest";
import { TableListItem } from "../api/model/table";
import { ReportCell, ReportRow } from "../api/model/reportRow";
import { ReportColumnMetadata } from "../api/model/reportColumn";
import './Grid.css';
import { ReportMetadata } from "../api/model/report";

type ReportPageProps = Omit<ReportPageBreadcrumb, 'key' | 'name'> & {
    onRequestCreateReportColumn: (reportOid: number, ordering: number | null) => void,
    onRequestEditReportColumn: (columnMetadata: ReportColumnMetadata) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

type ReportRowData = {
    index: number;
    baseline: number[];
    blank: null;
} & {
    [key: `column${number}`]: ReportCell;
};

export function ReportPage(props: ReportPageProps): React.JSX.Element {
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


    // Stores the report metadata
    const [report, setReport] = useState<ReportMetadata | null>(null);

    // Stores the list of columns
    const [columnList, setColumnList] = useState<ReportColumnMetadata[]>([]);

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
                                editReportColumnMetadata: {
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
    const [rowList, setRowList] = useState<ReportRow[]>([]);
    const [isRowListPending, startRowListTransition] = useTransition();

    /**
     * Updates the columns and rows of the table.
     */
    const updateReport = useCallback(() => {
        startRowListTransition(async () => {
            const tempReport: ReportMetadata = await getReportMetadataAsync(props.reportOid);
            const tempColumnList: ReportColumnMetadata[] = [];
            const tempRowList: ReportRow[] = [];
            try {
                const dropdownTableOid: Set<number> = new Set();
                await queryAsync({
                    reportCells: {
                        reportOid: props.reportOid,
                        oidFilters: props.oidFilters,
                        columnChannel: new Channel<ReportColumnMetadata>((item) => {
                            tempColumnList.push(item);
                        }),
                        rowChannel: new Channel<ReportRow>((item) => {
                            tempRowList.push(item);
                            for (const cell of item.cells) {
                                if ('singleSelectDropdown' in cell.content) {
                                    dropdownTableOid.add(cell.content.singleSelectDropdown.tableOid);
                                } else if ('multiSelectDropdown' in cell.content) {
                                    dropdownTableOid.add(cell.content.multiSelectDropdown.tableOid);
                                }
                            }
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
                setReport(tempReport);
                setColumnList(tempColumnList);
                setRowList(tempRowList);
            });
        });
    }, [props.reportOid, props.onError, loadDropdownValues]);

    /**
     * Edits a cell.
     */
    const onCellEditRequest = useCallback((event: CellEditRequestEvent<ReportRowData>) => {
        if (event.colDef.colId?.startsWith('column')) {
            const key: `column${number}` = event.colDef.colId as `column${number}`;
            const cell: ReportCell = event.data[key];
            if (cell.reference) {
                editCellContents({ ...cell.reference, content: cell.content }, event.newValue, props.onError);
            }
        }
    }, []);

    // Refresh table on initialization and on signal emitted from backend
    useEffect(() => {
        updateReport();
        
        const unlistenTable = listen<number>('report', (event) => {
            if (event.payload == props.reportOid) {
                updateReport();
            }
        });

        return () => {
            unlistenTable.then(f => f());
        };
    }, [props.reportOid, updateReport]);

    // Listen for updates to report cells
    useEffect(() => {
        const unlistenTableCell = listen<TableCell>('tableCell', (event) => {
            const colIndex: number = columnList.findIndex((col) => col.oid == event.payload.columnOid);
            if (colIndex >= 0) {
                setRowList((oldRowList) => {
                    return oldRowList.map((row) => {
                        // Check if cells have dependency
                        return row;
                    });
                });
            }
        });

        return () => {
            unlistenTableCell.then(f => f());
        };
    }, [columnList]);


    /**
     * Creates a new row in the baseline table. 
     */
    const createBaselineTableRow = useCallback(() => {
        const parentOidFilters: number[] = props.oidFilters.find(([ord, _]) => ord === report?.baseline?.parentOidName)?.[1] || [];
        if (report?.baseline) {
            executeAsync({
                createTableRow: {
                    tableOid: report.baseline.tableOid,
                    rowOid: null,
                    fixedParent: report.baseline.parentOidName && parentOidFilters.length == 1 ? [report.baseline.formula, parentOidFilters[0]] : null
                }
            }).catch(props.onError);
        } else {
            props.onError(`Cannot create new row in a report that has no baseline table.`);
        }
    }, [report?.baseline, props.onError, props.oidFilters]);

    


    // Construct column definitions for AgGrid
    const columnDefs = useMemo<ColDef<ReportRowData>[]>(() => {
        const indexColumn: ColDef<ReportRowData> = {
            field: 'index',
            headerName: "",
            cellClass: classNames('text-center', 'opacity-30'),
            width: 55,
            editable: false,
            pinned: 'left',
            lockPinned: true,
            onCellContextMenu: async (event) => {
                const oidFilters: number[] = event.data?.baseline || [];
                if (report?.baseline && oidFilters.length > 0) {
                    const reportBaseline = report.baseline;
                    const menu = await Menu.new({
                        items: [
                            ...(oidFilters.length == 1 ? [
                                await MenuItem.new({
                                    text: "Edit Baseline Row",
                                    action: () => {
                                        props.onRequestOpenObject({
                                            page: 'object',
                                            name: `#${event.data?.index}`,
                                            tableOid: reportBaseline.tableOid,
                                            rowOid: oidFilters[0]
                                        });
                                    }
                                }),
                            ] : []),
                            await MenuItem.new({
                                text: oidFilters.length == 1 ? "Delete Baseline Row" : "Delete Baseline Rows",
                                action: async () => {
                                    try {
                                        for (const rowOid of oidFilters) {
                                            await executeAsync({
                                                trashTableRow: {
                                                    tableOid: reportBaseline.tableOid,
                                                    rowOid: rowOid
                                                }
                                            });
                                        }
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
        const addNewColumn: ColDef<ReportRowData> = {
            field: 'blank',
            headerName: "Add New Column",
            editable: false,
            resizable: false,
            width: 200,
            headerComponent: AddNewColumnButton,
            headerComponentParams: {
                onClick: () => {
                    props.onRequestCreateReportColumn(props.reportOid, null);
                }
            }
        };
        return [
            indexColumn,
            ...columnList.map((columnMetadata): ColDef<ReportRowData> => {
                const key: `column${number}` = `column${columnMetadata.oid}`;
                return {
                    colId: key,
                    headerComponent: ColumnHeaderRenderer,
                    headerComponentParams: {
                        columnMetadata,
                        onRequestEditColumn: props.onRequestEditReportColumn,
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
                                const cell: ReportCell = data[key];
                                const content: TableCellContent = cell.content;
                                return cell.reference !== null && !('subreport' in content);
                            }
                        }
                        return false;
                    },
                    cellRendererSelector({ data }) {
                        if (data) {
                            if (key in data) {
                                const cell: ReportCell = data[key];
                                return selectRenderer(cell, dropdownValues, props.onRequestOpenDrillDownReport, props.onRequestOpenObject, props.onError);
                            }
                        }
                        return undefined;
                    },
                    cellEditorSelector({ data }) {
                        if (data) {
                            if (key in data) {
                                const cell: ReportCell = data[key];
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
                                const cell: ReportCell = data[key];
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
        props.reportOid, 
        report?.baseline,
        dropdownValues, 
        loadDropdownValues,
        props.onRequestCreateReportColumn, 
        props.onRequestEditReportColumn, 
        props.onRequestOpenDrillDownReport,
        props.onRequestOpenObject, 
        props.onError
    ]);

    // Construct row data for AgGrid
    const rowData = useMemo<ReportRowData[]>(() => {
        return rowList.map((row) => {
            return Object.fromEntries(
                ([
                    ['index', row.index],
                    ['baseline', row.oidFilters.find(([ord, _]) => ord === report?.baseline?.oidName)?.[1] || []],
                    ['blank', null]
                ] as {[K in keyof ReportRowData]: [K, ReportRowData[K]]}[keyof ReportRowData][])
                .concat(row.cells.map<[`column${number}`, ReportCell]>((cell) => {
                    return [`column${cell.columnOid}`, cell];
                }))
            );
        });
    }, [rowList, report?.baseline]);


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
            {report?.baseline && (<div>
                <Button
                    variant="gradient"
                    className="cursor-pointer"
                    onClick={createBaselineTableRow}
                >
                    Add New Row
                </Button>
            </div>)}
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