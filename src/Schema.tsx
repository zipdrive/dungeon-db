import { createElement, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getCellAsync, getImageSrcAsync, queryAsync } from "./api/query";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata } from "./api/model/column";
import { SchemaRow, CellContent, CellStream, AddNewRowButton, CellIdentifier, CellDependency, File } from "./api/model/cell";
import { useExtraColumnTypes, useBaseColumnTypes, cellDataRef, createRowProxy } from "./cell/Cell";
import { executeAsync } from "./api/action";
import { ObjectPageBreadcrumb, SchemaPageBreadcrumb } from "./breadcrumb";
import { Schema as SchemaMetadata, FullMetadata as SchemaFullMetadata } from "./api/model/schema";
import { listen } from "@tauri-apps/api/event";
import { Button } from "@material-tailwind/react";
import { ColumnGrouping, ColumnProp, ColumnRegular as RevoGridColumn, ColumnType as RevoGridColumnType, DataType, RevoGrid, CellProps, RowDefinition } from "@revolist/react-datagrid";
import classNames from "classnames";
import './Grid.css';
import { columnContextMenu } from "./cell/grid";
import { CellEditRequestEvent, ColDef, ColumnResizedEvent, RowDataTransaction, RowResizeEndedEvent } from 'ag-grid-community';
import { AgGridReact } from 'ag-grid-react';
import { cellPropertyEntry } from "./grid/DataRows";
import { AddNewColumnButton } from "./grid/ColDef";
import { selectEditor } from "./grid/EditorSelector";
import { getValue } from "./grid/ValueGetter";
import { editCellContents } from "./grid/CellEditRequest";


type SchemaGridProps = {
    columns: ColumnFullMetadata[],
    rows: [SchemaRow, CellContent[]][],
    onRequestUpdateSchema: () => Promise<void>,
    onRequestCreateColumn: (ordering: number | null) => void,
    onRequestEditColumn: (columnMetadata: ColumnFullMetadata) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

type SchemaGridRow = {
    rowMetadata: SchemaRow,
    blank: null,
} & {
    [key: `column${number}`]: CellContent
};

function SchemaGrid(props: SchemaGridProps): React.JSX.Element {
    /*
    const baseColumnTypes = useBaseColumnTypes(props.onRequestOpenSchema, props.onRequestOpenObject, props.onRequestUploadFile, props.onError);
    const extraColumnTypes = useExtraColumnTypes(props.columns, props.onError);
    */

    const columnDefs: ColDef<SchemaGridRow>[] = useMemo(() => {
        const indexColumn: ColDef<SchemaGridRow> = {
            field: 'rowMetadata.index',
            headerName: "",
            cellClass: classNames('text-center'),
            width: 55,
            editable: false,
            pinned: 'left',
            lockPinned: true,
        };
        const addNewColumn: ColDef<SchemaGridRow> = {
            field: 'blank',
            headerName: "Add New Column",
            editable: false,
            resizable: false,
            width: 200,
            headerComponent: AddNewColumnButton,
            headerComponentParams: {
                onClick: () => {
                    props.onRequestCreateColumn(null);
                }
            }
        };
        return [
            indexColumn,
            ...props.columns.map((columnMetadata): ColDef<SchemaGridRow> => {
                const key: `column${number}` = `column${columnMetadata.oid}`;

                let columnType: string;
                if ('select' in columnMetadata.columnType) {
                    columnType = `singleSelectDropdown${columnMetadata.columnType.select.tableOid}`;
                } else if ('multiselect' in columnMetadata.columnType) {
                    columnType = `multiSelectDropdown${columnMetadata.columnType.multiselect.tableOid}`;
                } else {
                    columnType = 'any';
                }
                return {
                    colId: key,
                    headerName: `${columnMetadata.isPrimaryKey ? '🔑 ' : ''}${columnMetadata.name}`,
                    cellClass: classNames(key),
                    width: columnMetadata.size,
                    resizable: true,
                    autoHeight: true,
                    wrapText: true,
                    editable({ data }) {
                        if (data) {
                            if (key in data) {
                                const content: CellContent = data[key];
                                return !('schemaLink' in content || 'readonly' in content);
                            }
                        }
                        return false;
                    },
                    cellEditorSelector({ data }) {
                        if (data) {
                            if (key in data) {
                                const content: CellContent = data[key];
                                console.log(key, selectEditor(content));
                                return selectEditor(content);
                            }
                        }
                        return {
                            component: 'agTextCellEditor'
                        };
                    },
                    valueGetter({ data }) {
                        if (data) {
                            if (key in data) {
                                const content: CellContent = data[key];
                                console.log(content, getValue(content));
                                return getValue(content);
                            }
                        }
                        return null;
                    }
                };
            }),
            addNewColumn
        ];
    }, [props.columns]);



    const [imgFiles, setImgFiles] = useState<File[]>([]);
    useEffect(() => {
        const newImgFiles: File[] = [];
        for (const [_rowMetadata, rowCells] of props.rows) {
            for (const rowCell of rowCells) {
                if ('imageEntry' in rowCell && rowCell.imageEntry.file) {
                    newImgFiles.push(rowCell.imageEntry.file);
                }
            }
        }
        setImgFiles(newImgFiles);
    }, [props.rows]);

    const [imgFileSrcs, setImgFileSrcs] = useState<{[fileOid: number]: string}>({});
    useEffect(() => {
        function getFileOid(f: File): number {
            return 'path' in f ? f.path.oid : f.blob.oid;
        }

        (async () => {
            try {
                const fileSrcLoads: [number, Promise<[File, string]>][] = imgFiles
                    .filter((file) => {
                        const fileOid: number = getFileOid(file);
                        return !Object.keys(imgFileSrcs).some((o) => parseInt(o) == fileOid);
                    })
                    .map<[number, Promise<[File, string]>]>((file) => {
                        return [getFileOid(file), (async () => { return [file, await getImageSrcAsync({ file })]; })()];
                    });
                let loadedImgFileSrcs: {[fileOid: number]: string} = {};
                while (fileSrcLoads.length > 0) {
                    const [file, fileSrc] = await Promise.race(fileSrcLoads.map(([_o, p]) => p));
                    const fileOid: number = getFileOid(file);
                    const idx: number = fileSrcLoads.findIndex(([o, _p]) => o == fileOid);
                    if (idx >= 0) {
                        fileSrcLoads.splice(idx, 1);
                    }
                    loadedImgFileSrcs = {
                        ...loadedImgFileSrcs,
                        [fileOid]: fileSrc
                    };
                    setImgFileSrcs({ 
                        ...imgFileSrcs, 
                        ...loadedImgFileSrcs
                    });
                }
            } catch (e) {
                props.onError(e);
            }
        })();
    }, [imgFiles]);

    const rowData: SchemaGridRow[] = useMemo(() => {
        return props.rows.map(([rowMetadata, rowCells]) => {
            return Object.assign({ rowMetadata, blank: null },
                Object.fromEntries(rowCells.map((rowCell) => cellPropertyEntry(rowCell, imgFileSrcs))) as {[key: `column${number}`]: CellContent}
            );
        });
    }, [props.rows, imgFileSrcs]);

    const grid = useRef<AgGridReact | null>(null);
    useEffect(() => {
        const unlistenCell = listen<CellIdentifier>('cell', async (e) => {
            const changedCellIdentifier = e.payload;

            function extractIdentifiersAndDependencies(content: CellContent): { cellIdentifier: CellIdentifier, isolatedCellDependencies: CellDependency[], fullReloadCellDependencies: CellDependency[] } {
                if ('textEntry' in content) {
                    return content.textEntry;
                } else if ('integerEntry' in content) {
                    return content.integerEntry;
                } else if ('numberEntry' in content) {
                    return content.numberEntry;
                } else if ('dateEntry' in content) {
                    return content.dateEntry;
                } else if ('datetimeEntry' in content) {
                    return content.datetimeEntry;
                } else if ('checkboxEntry' in content) {
                    return content.checkboxEntry;
                } else if ('fileEntry' in content) {
                    return content.fileEntry;
                } else if ('imageEntry' in content) {
                    return content.imageEntry;
                } else if ('objectLink' in content) {
                    return content.objectLink;
                } else if ('schemaLink' in content) {
                    return content.schemaLink;
                } else if ('singleSelectDropdown' in content) {
                    return content.singleSelectDropdown;
                } else if ('multiSelectDropdown' in content) {
                    return content.multiSelectDropdown;
                } else {
                    return content.readonly;
                }
            }
            
            if ('tableOid' in changedCellIdentifier) {
                const { tableOid: changedTableOid, columnOid: changedColumnOid, rowOid: changedRowOid } = changedCellIdentifier;
                function check(cellIdentifier: CellIdentifier, isolatedCellDependencies: CellDependency[], fullReloadCellDependencies: CellDependency[]): 'schema' | 'cell' | null {
                    if (fullReloadCellDependencies.some(({ tableOid, columnOid, rowOid }) => {
                        return tableOid == changedTableOid
                            && columnOid == changedColumnOid
                            && (rowOid === null || rowOid == changedRowOid)
                        ;
                    })) {
                        return 'schema';
                    }
                    if ('tableOid' in cellIdentifier) {
                        if (cellIdentifier.tableOid == changedTableOid
                            && cellIdentifier.columnOid == changedColumnOid
                            && cellIdentifier.rowOid == changedRowOid
                        ) {
                            return 'cell';
                        }
                    }
                    if (isolatedCellDependencies.some(({ tableOid, columnOid, rowOid }) => {
                        return tableOid == changedTableOid
                            && columnOid == changedColumnOid
                            && (rowOid === null || rowOid == changedRowOid)
                        ;
                    })) {
                        return 'cell';
                    }
                    return null;
                }

                const trans: RowDataTransaction<SchemaGridRow> = {
                    update: []
                };
                const newImgFiles: File[] = [...imgFiles];
                for (let rowIndex: number = 0; rowIndex < rowData.length; ++rowIndex) {
                    const row = {...rowData[rowIndex]};
                    for (const key in row) {
                        if (key.startsWith('column')) {
                            const typedKey: `column${number}` = key as `column${number}`;
                            const rowCell: CellContent = row[typedKey];

                            const { cellIdentifier, isolatedCellDependencies, fullReloadCellDependencies } = extractIdentifiersAndDependencies(rowCell);
                            const result = check(cellIdentifier, isolatedCellDependencies, fullReloadCellDependencies);
                            if (result === 'schema') {
                                await props.onRequestUpdateSchema();
                                return;
                            } else if (result === 'cell') {
                                const updatedCell = await getCellAsync(cellIdentifier);
                                if ('imageEntry' in updatedCell && updatedCell.imageEntry.file) {
                                    newImgFiles.push(updatedCell.imageEntry.file);
                                }
                                row[typedKey] = updatedCell;
                                if (trans.update && trans.update[trans.update.length - 1] !== row) {
                                    trans.update.push(row);
                                }
                            }
                        }
                    }
                }
                setImgFiles(newImgFiles);
                grid.current?.api.applyTransaction(trans);
            } // Ignore emitted cells located on a report
        });

        return () => {
            unlistenCell.then(f => f());
        }
    }, [rowData, props.onRequestUpdateSchema]);

    const onCellEditRequest = useCallback((event: CellEditRequestEvent) => {
        if (event.colDef.colId?.startsWith('column')) {
            const key: `column${number}` = event.colDef.colId as `column${number}`;
            const content: CellContent = event.data[key];
            editCellContents(content, event.newValue, props.onError);
        }
    }, [props.onError]);

    const onColumnResized = useCallback((event: ColumnResizedEvent) => {
        if (event.finished && event.columns) {
            for (const column of event.columns) {
                if (column.getColId().startsWith('column')) {
                    const key: `column${number}` = column.getColId() as `column${number}`;
                    const metadata = props.columns.find((c) => `column${c.oid}` === key);
                    if (metadata) {
                        executeAsync({
                            editColumn: {
                                metadata,
                                newColumnSize: Math.floor(column.getActualWidth()),
                                newColumnStyle: null
                            }
                        })
                        .catch(props.onError);
                    }
                }
            }
        }
    }, [props.columns, props.onError]);

    return (<AgGridReact
        ref={grid}
        columnDefs={columnDefs}
        onColumnResized={onColumnResized}
        rowData={rowData}
        getRowId={({ data }) => data.rowMetadata.index.toString()}
        readOnlyEdit={true}
        onCellEditRequest={onCellEditRequest}
    />);
}

type SchemaProps = {
    name: string,
    schema: SchemaMetadata,
    oidFilters: [string, number][],
    customFilters: string[],
    onChangeCustomFilters: (newFilters: string[]) => void,
    onRequestCreateColumn: (schema: SchemaFullMetadata, isTableColumn: boolean, ordering: number | null) => void,
    onRequestEditColumn: (columnMetadata: ColumnFullMetadata, isTableColumn: boolean) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

export function SchemaPage(props: SchemaProps): React.JSX.Element {
    const [pageNum, setPageNum] = useState<number>(1);
    const [pageSize, setPageSize] = useState<number>(100);
    const [maxPageNum, setMaxPageNum] = useState<number>(1);

    const [columns, setColumns] = useState<ColumnFullMetadata[]>([]);
    const [rows, setRows] = useState<[SchemaRow, CellContent[]][]>([]);
    const [addNewRowButton, setAddNewRowButton] = useState<AddNewRowButton | null>(null);

    useEffect(() => {
        const unlistenSchema = listen<number[]>('schema', (e) => {
            const updatedSchemas = e.payload;
            if (updatedSchemas.indexOf('table' in props.schema ? props.schema.table.schema.oid : props.schema.report.schema.oid) >= 0) {
                updateSchemaAsync();
            }
        });

        return () => {
            unlistenSchema.then(f => f());
        };
    }, [props.schema]);

    useEffect(() => {
        updateSchemaAsync();
    }, [props.schema, props.oidFilters, props.customFilters, pageNum, pageSize]);

    const subPixelCorrectionDiv = useRef<HTMLDivElement | null>(null);
    useEffect(() => {
        if (subPixelCorrectionDiv.current) {
            const subPixelWidth: number = subPixelCorrectionDiv.current.clientWidth;
            const subPixelHeight: number = subPixelCorrectionDiv.current.clientHeight;
            subPixelCorrectionDiv.current.style.maxWidth = `${Math.floor(subPixelWidth)-1}px`;
            subPixelCorrectionDiv.current.style.maxHeight = `${Math.floor(subPixelHeight)-1}px`;
        }
    }, [subPixelCorrectionDiv]);

    async function updateSchemaAsync() {
        const queriedColumns: ColumnFullMetadata[] = [];
        const queriedRows: [SchemaRow, CellContent[]][] = [];
        let queriedAddNewRowButton: AddNewRowButton | null = null;
        await queryAsync({
            cells: {
                schemaOid: 'table' in props.schema ? props.schema.table.schema.oid : props.schema.report.schema.oid,
                oidFilters: props.oidFilters,
                customFilters: props.customFilters,
                limit: {
                    page: {
                        num: pageNum,
                        size: pageSize
                    }
                },
                columnChannel: new Channel((columnMetadata) => {
                    queriedColumns.push(columnMetadata);
                }),
                cellChannel: new Channel((cellStream) => {
                    if ('maxIndex' in cellStream) {
                        setMaxPageNum(1 + Math.floor(cellStream.maxIndex / pageSize));
                    } else if ('addNewRowButton' in cellStream) {
                        queriedAddNewRowButton = cellStream.addNewRowButton;
                    } else if ('row' in cellStream) {
                        queriedRows.push([cellStream.row, []]);
                    } else {
                        if (queriedRows.length > 0) {
                            queriedRows[queriedRows.length - 1][1].push(cellStream.cell);
                        }
                    }
                }),
            }
        });
        
        setColumns(queriedColumns);
        setRows(queriedRows);
        setAddNewRowButton(queriedAddNewRowButton);
    }

    return (<div className="grid grid-col grid-rows-[calc(100vh-(var(--spacing)*20))_calc(var(--spacing)*10)] w-full">
        {columns.map((columnMetadata) => (<style>{`.column${columnMetadata.oid}`} &#123; {columnMetadata.style} &#125;</style>))}
        <div 
            ref={subPixelCorrectionDiv}
            className="m-4 mr-8 grid grid-col grid-rows-[1fr_auto] gap-y-10"
        >
            <SchemaGrid 
                columns={columns}
                rows={rows}
                onRequestUpdateSchema={updateSchemaAsync}
                onRequestCreateColumn={(ordering) => props.onRequestCreateColumn('table' in props.schema ? props.schema.table.schema : props.schema.report.schema, 'table' in props.schema, ordering)}
                onRequestEditColumn={(columnMetadata) => props.onRequestEditColumn(columnMetadata, 'table' in props.schema)}
                onRequestUploadFile={props.onRequestUploadFile}
                onRequestOpenSchema={props.onRequestOpenSchema}
                onRequestOpenObject={props.onRequestOpenObject}
                onError={props.onError}
            />
            {addNewRowButton && (<div>
                <Button
                    variant="gradient"
                    className="cursor-pointer"
                    onClick={async () => {
                        await executeAsync({
                            createRow: {
                                tableOid: addNewRowButton.tableOid,
                                rowOid: null,
                                fixedParentDatasource: addNewRowButton.fixedParentDatasource
                            }
                        });
                    }}
                >
                    Add New Row
                </Button>
            </div>)}
        </div>
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
    </div>);
}