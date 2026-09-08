import { Channel } from "@tauri-apps/api/core";
import { getCellAsync, getImageSrcAsync, queryAsync } from "./api/query";
import { SchemaPageBreadcrumb, ObjectPageBreadcrumb } from "./breadcrumb";
import { CellContent, CellDependency, CellIdentifier, File, SchemaRow } from "./api/model/cell";
import { FullMetadata as ColumnFullMetadata } from "./api/model/column";
import { Schema } from "./api/model/schema";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import classNames from "classnames";
import { RevoGrid, ColumnRegular as RevoGridColumn } from "@revolist/react-datagrid";
import { createRowProxy, useBaseColumnTypes, useExtraColumnTypes } from "./cell/Cell";
import './Grid.css';
import { columnContextMenu } from "./cell/grid";
import { AgGridReact } from "ag-grid-react";
import { cellPropertyEntry } from "./grid/DataRows";
import { AutoSizeStrategy, CellEditRequestEvent, ColDef, ColumnResizedEvent, RowDataTransaction, themeBalham } from "ag-grid-community";
import { selectRenderer } from "./grid/RendererSelector";
import { selectEditor } from "./grid/EditorSelector";
import { getValue } from "./grid/ValueGetter";
import { editCellContents } from "./grid/CellEditRequest";


type ObjectGridProps = {
    columns: ColumnFullMetadata[],
    row: [SchemaRow, CellContent[]],
    onRequestUpdateSchema: () => Promise<void>,
    onRequestEditColumn: (columnMetadata: ColumnFullMetadata) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

type ObjectGridRow = {
    rowId: `column${number}`,
    rowMetadata: SchemaRow,
    columnMetadata: ColumnFullMetadata,
    content: CellContent
} | {
    rowId: 'subtype'
};

const objectGridTheme = themeBalham.withParams({
    backgroundColor: 'rgb(var(--color-surface-light)/1)',
    oddRowBackgroundColor: 'inherit',
    textColor: 'rgb(var(--color-surface-foreground)/1)',
    chromeBackgroundColor: 'rgb(var(--color-surface)/1)',
    headerTextColor: 'inherit',
    borderColor: 'rgb(var(--color-surface-dark)/1)',
    fontFamily: 'inherit',
    fontSize: 'inherit',
    rowHoverColor: 'rgb(var(--color-primary)/0.05)',
});

function ObjectGrid(props: ObjectGridProps): React.JSX.Element {
    /*
    const baseColumnTypes = useBaseColumnTypes(props.onRequestOpenSchema, props.onRequestOpenObject, props.onRequestUploadFile, props.onError);
    const extraColumnTypes = useExtraColumnTypes(props.columns, props.onError);

    const columns: RevoGridColumn[] = [{
        prop: 'column0',
        name: 'Value',
        columnType: 'any',
        readonly(params) {
            const content: CellContent | null | undefined = params.model[`${String(params.prop)}content`];
            if (content) {
                return 'checkboxEntry' in content
                    || 'objectLink' in content
                    || 'schemaLink' in content 
                    || 'readonly' in content;
            }
            return true;
        }
    }];
    */
    
    const columnDefs: ColDef<ObjectGridRow>[] = useMemo(() => {
        return [
            {
                headerName: "Name",
                valueGetter({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {

                        } else {
                            const columnMetadata = data.columnMetadata;
                            return `${columnMetadata.isPrimaryKey ? '🔑 ' : ''}${columnMetadata.name}`;
                        }
                    } else {
                        return '';
                    }
                },
                cellClass: classNames('font-bold'),
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

                        } else {
                            const columnMetadata = data.columnMetadata;
                            return `column${columnMetadata.oid}`;
                        }
                    } else {
                        return '';
                    }
                },
                flex: 1,
                resizable: false,
                wrapText: true,
                editable({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {

                        } else {
                            const content: CellContent = data.content;
                            return !('schemaLink' in content || 'readonly' in content);
                        }
                    }
                    return false;
                },
                cellRendererSelector({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {

                        } else {
                            const content: CellContent = data.content;
                            return selectRenderer(content, props.onRequestOpenSchema, props.onRequestOpenObject);
                        }
                    }
                    return undefined;
                },
                cellEditorSelector({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {

                        } else {
                            const content: CellContent = data.content;
                            return selectEditor(content);
                        }
                    }
                    return {
                        component: 'agTextCellEditor'
                    };
                },
                valueGetter({ data }) {
                    if (data) {
                        if (data.rowId === 'subtype') {

                        } else {
                            const content: CellContent = data.content;
                            return getValue(content);
                        }
                    }
                    return null;
                }
            }
        ];
    }, [props.columns]);


    const [imgFiles, setImgFiles] = useState<File[]>([]);
    useEffect(() => {
        const newImgFiles: File[] = [];
        const [_rowMetadata, rowCells] = props.row;
        for (const rowCell of rowCells) {
            if ('imageEntry' in rowCell && rowCell.imageEntry.file) {
                newImgFiles.push(rowCell.imageEntry.file);
            }
        }
        setImgFiles(newImgFiles);
    }, [props.row]);

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


    const rowData: ObjectGridRow[] = useMemo(() => {
        const [rowMetadata, rowCells] = props.row;
        return rowCells.map((rowCell, idx) => {
            if (idx >= props.columns.length) {
                throw new Error(`Was streamed less columns than cells.`);
            }
            const columnMetadata = props.columns[idx];
            const [key, content] = cellPropertyEntry(rowCell, imgFileSrcs);
            return {
                rowId: key,
                columnMetadata,
                rowMetadata,
                content
            };
        });
    }, [props.columns, props.row, props.onError, imgFileSrcs]);

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

                const trans: RowDataTransaction<ObjectGridRow> = {
                    update: []
                };
                const newImgFiles: File[] = [...imgFiles];
                for (let rowIndex: number = 0; rowIndex < rowData.length; ++rowIndex) {
                    const row = rowData[rowIndex];
                    if (row.rowId !== 'subtype') {
                        const rowCell: CellContent = row.content;

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
                            row.content = updatedCell;
                            if (trans.update) {
                                trans.update.push(row);
                            }
                        }
                    }
                }
                setImgFiles(newImgFiles);
                console.log(trans);
                grid.current?.api.applyTransaction(trans);
            } // Ignore emitted cells located on a report
        });

        return () => {
            unlistenCell.then(f => f());
        }
    }, [rowData, props.onRequestUpdateSchema]);

    const autoSizeStrategy = useMemo<AutoSizeStrategy>(() => {
        return {
            type: 'fitGridWidth'
        }
    }, []);
    
    const onCellEditRequest = useCallback((event: CellEditRequestEvent<ObjectGridRow>) => {
        if (event.data.rowId === 'subtype') {
            // TODO
        } else {
            const content: CellContent = event.data.content;
            editCellContents(content, event.newValue, props.onError);
        }
    }, [props.onError]);
    
    const onColumnResized = useCallback((event: ColumnResizedEvent) => {
        //if (event.finished) {
            event.api.sizeColumnsToFit();
        //}
    }, []);


    return (<AgGridReact
        ref={grid}
        columnDefs={columnDefs}
        rowData={rowData}
        getRowId={({ data }) => data.rowId}
        autoSizeStrategy={autoSizeStrategy}
        onColumnResized={onColumnResized}
        readOnlyEdit={true}
        onCellEditRequest={onCellEditRequest}
        theme={objectGridTheme}
        animateRows={false}
    />);
}


type ObjectProps = {
    schema: Schema,
    oidFilters: [string, number][],
    onRequestEditColumn: (columnMetadata: ColumnFullMetadata, isTableColumn: boolean) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

export function ObjectPage(props: ObjectProps): React.JSX.Element {
    const [columns, setColumns] = useState<ColumnFullMetadata[]>([]);
    const [row, setRow] = useState<[SchemaRow, CellContent[]] | null>(null);

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
    }, [props.schema, props.oidFilters]);
    
    const [imgFiles, setImgFiles] = useState<File[]>([]);
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

    useEffect(() => {
        if (row) {
            const [rowMetadata, [...rowCells]] = row;
            for (let k = 0; k < rowCells.length; ++k) {
                const rowCell = rowCells[k];
                if ('imageEntry' in rowCell && rowCell.imageEntry.file) {
                    let fileOid: number = 'path' in rowCell.imageEntry.file ? rowCell.imageEntry.file.path.oid : rowCell.imageEntry.file?.blob.oid;
                    if (fileOid in imgFileSrcs) {
                        rowCells[k] = {
                            imageEntry: {
                                ...rowCell.imageEntry,
                                fileSrc: imgFileSrcs[fileOid]
                            }
                        };
                    }
                }
            }
            setRow([rowMetadata, rowCells]);
        }
    }, [imgFileSrcs]);

    const subPixelCorrectionDiv = useRef<HTMLDivElement | null>(null);
    useEffect(() => {
        if (subPixelCorrectionDiv.current) {
            const subPixelWidth: number = subPixelCorrectionDiv.current.clientWidth;
            const subPixelHeight: number = subPixelCorrectionDiv.current.clientHeight;
            subPixelCorrectionDiv.current.style.maxWidth = `${Math.floor(subPixelWidth)-1}px`;
            subPixelCorrectionDiv.current.style.maxHeight = `${Math.floor(subPixelHeight)-1}px`;
        }
    }, [subPixelCorrectionDiv]);

    /**
     * Updates the columns and data of the schema.
     */
    async function updateSchemaAsync() {
        const queriedColumns: ColumnFullMetadata[] = [];
        let queriedRowMetadata: SchemaRow | null = null;
        const queriedRowCells: CellContent[] = [];
        const queriedImgFiles: File[] = [];
        await queryAsync({
            object: {
                schemaOid: 'table' in props.schema ? props.schema.table.schema.oid : props.schema.report.schema.oid,
                oidFilters: props.oidFilters,
                columnChannel: new Channel((columnMetadata) => {
                    queriedColumns.push(columnMetadata);
                }),
                cellChannel: new Channel((cellStream) => {
                    if ('row' in cellStream) {
                        queriedRowMetadata = cellStream.row;
                    } else if ('cell' in cellStream) {
                        if ('imageEntry' in cellStream.cell && cellStream.cell.imageEntry.file) {
                            const fileOid: number = 'path' in cellStream.cell.imageEntry.file ? cellStream.cell.imageEntry.file.path.oid : cellStream.cell.imageEntry.file.blob.oid;
                            if (fileOid in imgFileSrcs) {
                                cellStream.cell.imageEntry.fileSrc = imgFileSrcs[fileOid];
                            } else {
                                queriedImgFiles.push(cellStream.cell.imageEntry.file);
                            }
                        }
                        queriedRowCells.push(cellStream.cell);
                    }
                }),
            }
        });
        
        setColumns(queriedColumns);
        setRow(queriedRowMetadata ? [queriedRowMetadata, queriedRowCells] : null);
        setImgFiles(queriedImgFiles);
    }

    return (<div 
        ref={subPixelCorrectionDiv}
        className="grid grid-col w-[calc(100%-(var(--spacing)*12))] ml-4 mr-8 mt-4 mb-5"
    >
        {columns.map((columnMetadata) => (<style>{`.column${columnMetadata.oid}`} &#123; {columnMetadata.style} &#125;</style>))}
        {row && <ObjectGrid 
            columns={columns}
            row={row}
            onRequestUpdateSchema={updateSchemaAsync}
            onRequestEditColumn={(columnMetadata) => props.onRequestEditColumn(columnMetadata, 'table' in props.schema)}
            onRequestUploadFile={props.onRequestUploadFile}
            onRequestOpenSchema={props.onRequestOpenSchema}
            onRequestOpenObject={props.onRequestOpenObject}
            onError={props.onError}
        />}
    </div>);
}