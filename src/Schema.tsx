import { createElement, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getCellAsync, queryAsync } from "./api/query";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata } from "./api/model/column";
import { SchemaRow, CellContent, CellStream, AddNewRowButton, CellIdentifier, CellDependency } from "./api/model/cell";
import { cellPropertyEntry, useExtraColumnTypes, useBaseColumnTypes, cellDataRef, createRowProxy } from "./cell/Cell";
import { executeAsync } from "./api/action";
import { ObjectPageBreadcrumb, SchemaPageBreadcrumb } from "./breadcrumb";
import { Schema as SchemaMetadata, FullMetadata as SchemaFullMetadata } from "./api/model/schema";
import { listen } from "@tauri-apps/api/event";
import { Button } from "@material-tailwind/react";
import { ColumnGrouping, ColumnProp, ColumnRegular as RevoGridColumn, ColumnType as RevoGridColumnType, DataType, RevoGrid, CellProps } from "@revolist/react-datagrid";
import classNames from "classnames";


type SchemaGridProps = {
    columns: ColumnFullMetadata[],
    rows: [SchemaRow, CellContent[]][],
    onSetContent: (rowIndex: number, colIndex: number, newContent: CellContent) => void,
    onRequestCreateColumn: (ordering: number | null) => void,
    onRequestEditColumn: (columnMetadata: ColumnFullMetadata) => void,
    onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

function SchemaGrid(props: SchemaGridProps): React.JSX.Element {
    const baseColumnTypes = useBaseColumnTypes(props.onError);
    const extraColumnTypes = useExtraColumnTypes(props.columns, props.onError);

    const columns: RevoGridColumn[] = useMemo(() => {
        return props.columns.map((columnMetadata): RevoGridColumn => {
            const columnLabel: string = (columnMetadata.isPrimaryKey ? '🔑 ' : '') + columnMetadata.name;
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
                name: columnLabel
            };
        })
        .concat([{
            prop: 'blank',
            name: 'test'
        }, {
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

    const dataRows = useMemo(() => {
        return props.rows.map(([rowMetadata, rowCells]) => {
            const rowModel = Object.assign({ rowMetadata, blank: null },
                Object.fromEntries(rowCells.map(cellPropertyEntry))
            );
            return createRowProxy(rowModel, props.onError);
        });
    }, [props.columns, props.rows, props.onError]);

    return (<RevoGrid
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
        applyOnClose={true}
        onAfteredit={(event) => {
            event.detail.data
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
    onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

export function Schema(props: SchemaProps): React.JSX.Element {
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

                const newRows: [SchemaRow, CellContent[]][] = [];
                for (let rowIndex: number = 0; rowIndex < rows.length; ++rowIndex) {
                    const rowCells = rows[rowIndex][1];
                    const newRowCells: CellContent[] = [];

                    for (let colIndex: number = 0; colIndex < rowCells.length; ++colIndex) {
                        const rowCell = rowCells[colIndex];

                        const { cellIdentifier, isolatedCellDependencies, fullReloadCellDependencies } = extractIdentifiersAndDependencies(rowCell);
                        const result = check(cellIdentifier, isolatedCellDependencies, fullReloadCellDependencies);
                        if (result === 'schema') {
                            await updateSchemaAsync();
                            return;
                        } else if (result === 'cell') {
                            newRowCells.push(await getCellAsync(cellIdentifier));
                        } else {
                            newRowCells.push(rowCell);
                        }
                    }
                    newRows.push([rows[rowIndex][0], newRowCells]);
                }
                setRows(newRows);
            } // Ignore emitted cells located on a report
        });

        return () => {
            unlistenCell.then(f => f());
        }
    }, [rows]);

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
        <div className="m-4 grid grid-col grid-rows-[1fr_auto] gap-y-2">
            <SchemaGrid 
                columns={columns}
                rows={rows}
                onSetContent={(rowIndex: number, colIndex: number, newContent: CellContent) => {
                    const [changedRowMetadata, changedRowCells] = rows[rowIndex];
                    const newRows = rows.slice(0, rowIndex)
                        .concat([[
                            changedRowMetadata, 
                            changedRowCells.slice(0, colIndex)
                                .concat([newContent])
                                .concat(changedRowCells.slice(colIndex + 1))
                        ]])
                        .concat(rows.slice(rowIndex + 1));
                    setRows(newRows);
                }}
                onRequestCreateColumn={(ordering) => props.onRequestCreateColumn('table' in props.schema ? props.schema.table.schema : props.schema.report.schema, 'table' in props.schema, ordering)}
                onRequestEditColumn={(columnMetadata) => props.onRequestEditColumn(columnMetadata, 'table' in props.schema)}
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