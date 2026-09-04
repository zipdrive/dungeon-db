import { createElement, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getCellAsync, getImageSrcAsync, queryAsync } from "./api/query";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata } from "./api/model/column";
import { SchemaRow, CellContent, CellStream, AddNewRowButton, CellIdentifier, CellDependency, File } from "./api/model/cell";
import { cellPropertyEntry, useExtraColumnTypes, useBaseColumnTypes, cellDataRef, createRowProxy } from "./cell/Cell";
import { executeAsync } from "./api/action";
import { ObjectPageBreadcrumb, SchemaPageBreadcrumb } from "./breadcrumb";
import { Schema as SchemaMetadata, FullMetadata as SchemaFullMetadata } from "./api/model/schema";
import { listen } from "@tauri-apps/api/event";
import { Button } from "@material-tailwind/react";
import { ColumnGrouping, ColumnProp, ColumnRegular as RevoGridColumn, ColumnType as RevoGridColumnType, DataType, RevoGrid, CellProps, RowDefinition } from "@revolist/react-datagrid";
import classNames from "classnames";
import './Grid.css';
import { columnContextMenu } from "./cell/grid";


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

function SchemaGrid(props: SchemaGridProps): React.JSX.Element {
    const baseColumnTypes = useBaseColumnTypes(props.onRequestOpenSchema, props.onRequestOpenObject, props.onRequestUploadFile, props.onError);
    const extraColumnTypes = useExtraColumnTypes(props.columns, props.onError);

    const columns: RevoGridColumn[] = useMemo(() => {
        return props.columns.map((columnMetadata): RevoGridColumn => {
            let columnType: string;
            if ('select' in columnMetadata.columnType) {
                columnType = `singleSelectDropdown${columnMetadata.columnType.select.tableOid}`;
            } else if ('multiselect' in columnMetadata.columnType) {
                columnType = `multiSelectDropdown${columnMetadata.columnType.multiselect.tableOid}`;
            } else {
                columnType = 'any';
            }
            return {
                prop: `column${columnMetadata.oid}`,
                name: columnMetadata.name,
                size: columnMetadata.size,
                columnType,
                columnProperties: () => {
                    return {
                        'class': classNames(
                            { "before:content-['🔑']": columnMetadata.isPrimaryKey },
                            { "before:px-2": columnMetadata.isPrimaryKey }
                        ),
                        'onContextMenu': async (e) => {
                            await columnContextMenu(e, columnMetadata, props.onRequestEditColumn, props.onError);
                        }
                    };
                },
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
            };
        })
        .concat([{
            prop: 'blank',
            name: 'Add New Column',
            readonly: true,
            size: 200,
            columnTemplate(createElement, _p) {
                return createElement(
                    'button',
                    {
                        'class': 'absolute left-4 inline-flex items-center justify-center border align-middle select-none font-sans font-medium text-center transition-all duration-300 ease-in disabled:opacity-50 disabled:shadow-none disabled:cursor-not-allowed data-[shape=pill]:rounded-full data-[width=full]:w-full focus:shadow-none text-sm rounded-md py-2 px-4 shadow-sm hover:shadow-md bg-gradient-to-tr from-primary-dark to-primary-light border-primary text-primary-foreground hover:brightness-105 cursor-pointer',
                        'data-shape': "default",
                        'data-width': "default",
                        'onClick': () => {
                            props.onRequestCreateColumn(null);
                        }
                    },
                    "Add New Column"
                );
            }
        }]);
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

    const dataRows = useMemo(() => {
        return props.rows.map(([rowMetadata, rowCells]) => {
            const rowModel = Object.assign({ rowMetadata, blank: null },
                Object.fromEntries(rowCells.map((rowCell) => cellPropertyEntry(rowCell)))
            );
            return createRowProxy<{ rowMetadata: SchemaRow, blank: null }>(rowModel, imgFileSrcs, props.onError);
        });
    }, [props.columns, props.rows, props.onError, imgFileSrcs]);

    const grid = useRef<HTMLRevoGridElement | null>(null);
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

                const newImgFiles: File[] = [...imgFiles];
                for (let rowIndex: number = 0; rowIndex < dataRows.length; ++rowIndex) {
                    const rowProxy = dataRows[rowIndex];
                    for (const key in rowProxy) {
                        const typedKey: keyof typeof rowProxy = key as keyof typeof rowProxy;
                        if (typedKey.startsWith('column')) {
                            const contentKey = typedKey.endsWith('content') ? typedKey as `column${number}content` : `${typedKey}content` as `column${number}content`;
                            const rowCell: CellContent = rowProxy[contentKey];

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
                                rowProxy[contentKey] = updatedCell;
                            }
                        }
                    }
                }
                setImgFiles(newImgFiles);
                grid.current?.refresh();
            } // Ignore emitted cells located on a report
        });

        return () => {
            unlistenCell.then(f => f());
        }
    }, [dataRows, props.onRequestUpdateSchema]);


    return (<RevoGrid
        ref={grid}
        className="schema-grid"
        columnTypes={Object.assign(baseColumnTypes, extraColumnTypes)}
        columns={columns}
        source={dataRows}
        rowHeaders={{
            prop: 'rowMetadata',
            cellTemplate(createElement, props) {
                const rowMetadata: SchemaRow = props.model[props.prop];
                return createElement(
                    'div',
                    {
                        'class': classNames('text-center')
                    },
                    rowMetadata.index.toString()
                )
            }
        }}
        stretch={true}
        resizeRow={true}
        range={true}
        applyOnClose={true}
        onAftercolumnresize={async (event) => {
            for (const rowIndex in event.detail) {
                const detail = event.detail[rowIndex];
                try {
                    const columnMetadata: ColumnFullMetadata | undefined = props.columns.find((c) => String(detail.prop) === `column${c.oid}`);
                    if (columnMetadata) {
                        await executeAsync({
                            editColumn: {
                                metadata: columnMetadata,
                                newColumnSize: detail.size ?? 150,
                                newColumnStyle: null
                            }
                        });
                    }
                } catch (e) {
                    props.onError(e);
                }
            }
        }}
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
    const [pageSize, setPageSize] = useState<number>(2000);
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