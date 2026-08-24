import { useEffect, useState } from "react";
import { queryAsync } from "./api/query";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata } from "./api/model/column";
import { SchemaRow, CellContent, CellStream, AddNewRowButton } from "./api/model/cell";
import { Cell } from "./Cell";
import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { LogicalPosition } from "@tauri-apps/api/dpi";
import { executeAsync } from "./api/action";
import { ObjectPageBreadcrumb, SchemaPageBreadcrumb } from "./breadcrumb";
import { Schema as SchemaMetadata, FullMetadata as SchemaFullMetadata } from "./api/model/schema";
import { listen } from "@tauri-apps/api/event";

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
};

export function Schema(props: SchemaProps): React.JSX.Element {
    const [pageNum, setPageNum] = useState<number>(1);
    const [pageSize, setPageSize] = useState<number>(2000);
    const [maxPageNum, setMaxPageNum] = useState<number>(1);

    const [columns, setColumns] = useState<ColumnFullMetadata[]>([]);
    const [rows, setRows] = useState<[SchemaRow, CellContent[]][]>([]);
    const [addNewRowButton, setAddNewRowButton] = useState<AddNewRowButton | null>(null);

    const [focusedCell, setFocusedCell] = useState<{ columnIdx: number, rowIdx: number }>({ columnIdx: 0, rowIdx: 0 });

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
                        setMaxPageNum(1 + Math.ceil(cellStream.maxIndex / pageSize));
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

    return (<div className="grow flex flex-col">
        <div className="grow mx-4 my-4">
            <table>
                <thead>
                    <tr>
                        <th>Index</th>
                        {columns.map((columnMetadata) => {
                            return (<th
                                onContextMenu={async (e) => {
                                    const menu: Menu = await Menu.new({
                                        items: [
                                            await MenuItem.new({
                                                text: 'Edit',
                                                action: () => {
                                                    props.onRequestEditColumn(columnMetadata, 'table' in props.schema);
                                                }
                                            }),
                                            await MenuItem.new({
                                                text: 'Insert',
                                                action: () => {
                                                    props.onRequestCreateColumn('table' in props.schema ? props.schema.table.schema : props.schema.report.schema, 'table' in props.schema, columnMetadata.ordering);
                                                }
                                            }),
                                            await MenuItem.new({
                                                text: 'Delete',
                                                action: async () => {
                                                    await executeAsync({
                                                        trashColumn: {
                                                            schemaOid: columnMetadata.schema.oid,
                                                            columnOid: columnMetadata.oid,
                                                        }
                                                    });
                                                }
                                            }),
                                        ]
                                    });
                                    await menu.popup(new LogicalPosition({
                                        x: e.pageX,
                                        y: e.pageY
                                    }));
                                }}
                            >
                                {columnMetadata.isPrimaryKey ? '🔑 ' : ''}{columnMetadata.name}
                            </th>);
                        })}
                        <th>
                            <a href="#"
                                onClick={() => {
                                    props.onRequestCreateColumn('table' in props.schema ? props.schema.table.schema : props.schema.report.schema, 'table' in props.schema, null);
                                }}
                            >
                                Add New Column
                            </a>
                        </th>
                    </tr>
                </thead>
                <tbody>
                    {pageNum > 1 && (<tr><th />
                        <th colSpan={columns.length}>
                            <a href="#" onClick={() => { setPageNum(pageNum - 1); }} className="text-center">Previous Page</a>
                        </th>
                    </tr>)}
                    {rows.map(([rowMetadata, rowCells], rowIdx) => {
                        return (<tr>
                            <th
                                onContextMenu={async (e) => {
                                    const menuItems: MenuItem[] = [
                                        await MenuItem.new({
                                            text: 'Edit',
                                            action: () => {
                                                // Open the row in an Object page
                                                props.onRequestOpenObject({
                                                    name: 'Row',
                                                    schemaOid: 'table' in props.schema ? props.schema.table.schema.oid : props.schema.report.schema.oid,
                                                    oidFilters: rowMetadata.oidFilters,
                                                });
                                            }
                                        })
                                    ];

                                    if (rowMetadata.tableRowIdentifier !== null) {
                                        const { tableOid, rowOid } = rowMetadata.tableRowIdentifier;
                                        menuItems.push(await MenuItem.new({
                                            text: 'Delete',
                                            action: async () => {
                                                // Delete the row
                                                await executeAsync({
                                                    trashRow: {
                                                        tableOid,
                                                        rowOid
                                                    }
                                                });
                                            }
                                        }));
                                    }

                                    const menu = await Menu.new({
                                        items: menuItems
                                    });
                                    await menu.popup(new LogicalPosition({
                                        x: e.pageX,
                                        y: e.pageY
                                    }));
                                }}
                            >
                                {rowMetadata.index}
                            </th>
                            {rowCells.map((content, columnIdx) => {
                                return (<Cell 
                                    content={content} 
                                    isFocused={rowIdx == focusedCell.rowIdx && columnIdx == focusedCell.columnIdx} 
                                />);
                            })}
                        </tr>);
                    })}
                    {pageNum < maxPageNum && (<tr><th />
                        <th colSpan={columns.length}>
                            <a href="#" onClick={() => { setPageNum(pageNum + 1); }} className="text-center">Next Page</a>
                        </th>
                    </tr>)}
                    {addNewRowButton && (<tr><th />
                        <th colSpan={columns.length}>
                            <a 
                                href="#" 
                                onClick={async () => {
                                    await executeAsync({
                                        createRow: {
                                            tableOid: addNewRowButton.tableOid,
                                            rowOid: null,
                                            fixedParentDatasource: addNewRowButton.fixedParentDatasource
                                        }
                                    });
                                }}
                                className="text-center"
                            >
                                Add New Row
                            </a>
                        </th>
                    </tr>)}
                </tbody>
            </table>
        </div>
        <div className="h-10 border-t-1 border-t-blue-gray-100 bg-blue-gray-50 bg-opacity-60">

        </div>
    </div>);
}