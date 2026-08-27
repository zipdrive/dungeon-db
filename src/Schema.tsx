import { useCallback, useEffect, useRef, useState } from "react";
import { queryAsync } from "./api/query";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata } from "./api/model/column";
import { SchemaRow, CellContent, CellStream, AddNewRowButton } from "./api/model/cell";
import { createCell } from "./Cell";
import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { LogicalPosition } from "@tauri-apps/api/dpi";
import { executeAsync } from "./api/action";
import { ObjectPageBreadcrumb, SchemaPageBreadcrumb } from "./breadcrumb";
import { Schema as SchemaMetadata, FullMetadata as SchemaFullMetadata } from "./api/model/schema";
import { listen } from "@tauri-apps/api/event";
import { Button, ThemeProvider } from "@material-tailwind/react";
import ReactDOMServer from 'react-dom/server';
import { createPortal } from "react-dom";
import { Spreadsheet } from "react-spreadsheet";
import classNames from "classnames";

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

    const iframeDocRef = useRef<HTMLIFrameElement>(null);

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

    return (<div className="grid grid-col grid-rows-[calc(100vh-(var(--spacing)*20))_calc(var(--spacing)*10)]">
        <iframe ref={iframeDocRef} className="size-full">
            {iframeDocRef.current?.contentWindow?.document && createPortal(
                <>
                    <link rel="stylesheet" href="/react-spreadsheet/assets/css/styles.115ba647.css" />
                    <link rel="stylesheet" type="text/css" href="/src/Schema.css" />
                    {columns.map((columnMetadata) => {
                        return (<style>
                            {`.column${columnMetadata.oid}`} &#123;
                                {columnMetadata.style}
                            &#125;
                        </style>)
                    })}
                    <div className="mx-4 my-4 overflow-auto text-sm">
                        <div className="flex flex-col gap-y-2">
                            <div className="flex flex-row">
                                <Spreadsheet
                                    columnLabels={columns.filter((columnMetadata) => !columnMetadata.hidden).map((columnMetadata) => (columnMetadata.isPrimaryKey ? '🔑 ' : '') + columnMetadata.name)}
                                    ColumnIndicator={
                                        ({
                                            column: columnIndex,
                                            label,
                                            selected,
                                            onSelect,
                                        }) => {
                                            const handleClick = useCallback(
                                                (event: React.MouseEvent) => {
                                                    onSelect(columnIndex, event.shiftKey);
                                                },
                                                [onSelect, columnIndex]
                                            );

                                            const handleContextMenu = useCallback(
                                                async (event: React.MouseEvent) => {
                                                    const columnMetadata: ColumnFullMetadata = columns[columnIndex];

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
                                                        x: event.screenX,
                                                        y: event.screenY
                                                    }));
                                                },
                                                [columnIndex]
                                            );
                                            
                                            return (
                                                <th
                                                    className={classNames("Spreadsheet__header", {
                                                        "Spreadsheet__header--selected": selected,
                                                    })}
                                                    onClick={handleClick}
                                                    onContextMenu={handleContextMenu}
                                                    tabIndex={0}
                                                >
                                                    {label ?? ''}
                                                </th>
                                            );
                                        }
                                    }
                                    rowLabels={rows.map(([rowMetadata, _rowCells]) => rowMetadata.index.toString())}
                                    RowIndicator={
                                        ({
                                            row: rowNum,
                                            label,
                                            selected,
                                            onSelect,
                                        }) => {
                                            const handleClick = useCallback(
                                                (event: React.MouseEvent) => {
                                                onSelect(rowNum, event.shiftKey);
                                                },
                                                [onSelect, rowNum]
                                            );

                                            const handleContextMenu = useCallback(
                                                async (event: React.MouseEvent) => {
                                                    const rowMetadata: SchemaRow = rows[rowNum][0];

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
                                                        x: event.screenX,
                                                        y: event.screenY
                                                    }));
                                                },
                                                [rowNum]
                                            );

                                            return (
                                                <th
                                                    className={classNames("px-2", "Spreadsheet__header", {
                                                        "Spreadsheet__header--selected": selected,
                                                    })}
                                                    onClick={handleClick}
                                                    onContextMenu={handleContextMenu}
                                                    tabIndex={0}
                                                >
                                                    {label !== undefined ? label : 'N/A'}
                                                </th>
                                            );
                                        }
                                    }
                                    data={rows.map(([_rowMetadata, rowCells]) => rowCells.filter((_rowCell, idx) => !columns[idx].hidden).map(createCell))}
                                />
                                <div>
                                    <Button 
                                        variant="gradient"
                                        className="rounded-l-none aspect-square px-8 cursor-pointer"
                                        style={{ lineHeight: 'normal', paddingTop: '4px', paddingBottom: '4px', fontSize: '16px', minHeight: 'calc(1.9em + 1px)', maxHeight: 'calc(1.9em + 1px)', height: 'calc(1.9em + 1px)' }}
                                        onClick={() => { 
                                            if ('table' in props.schema) {
                                                props.onRequestCreateColumn(props.schema.table.schema, true, null);
                                            } else {
                                                props.onRequestCreateColumn(props.schema.report.schema, false, null);
                                            }
                                        }}
                                    >
                                        +
                                    </Button>
                                </div>
                            </div>
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
                    </div>
                </>,
                iframeDocRef.current.contentWindow.document.body
            )}
        </iframe>
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