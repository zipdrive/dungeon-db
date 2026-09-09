import { CellContent, CellDependency, CellIdentifier, ClipboardCellData, DataCellEntry, SchemaRow, TextEntryCellContent } from "../api/model/cell";
import { ColumnGrouping, ColumnProp, ColumnRegular as RevoGridColumn, ColumnType as RevoGridColumnType, DataType, RevoGrid } from "@revolist/react-datagrid";
import { TextEntryEditor } from "./TextEntry";
import { FullMetadata as ColumnFullMetadata } from "../api/model/column";
import { useCallback, useEffect, useMemo, useState } from "react";
import { downloadFileAsync, getCellAsync, getColumnAsync, getSchemaMetadataAsync, queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { executeAsync } from "../api/action";
import classNames from "classnames";
import { ObjectPageBreadcrumb, SchemaPageBreadcrumb } from "../breadcrumb";
import uploadIconSrc from '../assets/upload.png';
import downloadIconSrc from '../assets/download.png';
import { open, save } from "@tauri-apps/plugin-dialog";
import { appDataDir, join, resolve, resourceDir, sep } from "@tauri-apps/api/path";


type RowModel<T extends {[key: Exclude<string, `column${string}`>]: any}> = T & {[key: `column${number}content`]: CellContent} & {[key: `column${number}`]: any};
export function createRowProxy<T extends object>(row: RowModel<T>, imgFileSrcs: {[fileOid: number]: string}, onError: (e: unknown) => void): RowModel<T> {
    return new Proxy(row, {
        get(target, prop, receiver): any {
            if ((String(prop).startsWith('column') && String(prop).endsWith('content'))) {
                const content: CellContent | null | undefined = Reflect.get(target, String(prop).replace('content', '') as `column${number}`, receiver);
                if (content && 'imageEntry' in content && content.imageEntry.file) {
                    const fileOid: number = 'path' in content.imageEntry.file ? content.imageEntry.file.path.oid : content.imageEntry.file.blob.oid;
                    if (fileOid in imgFileSrcs) {
                        return {
                            imageEntry: {
                                ...content.imageEntry,
                                fileSrc: imgFileSrcs[fileOid]
                            }
                        };
                    }
                }
                return content;
            } else if (!String(prop).startsWith('column')) {
                return Reflect.get(target, prop, receiver);
            }

            const content: CellContent | null | undefined = Reflect.get(target, prop, receiver) as CellContent | null | undefined;
            if (content) {
                if ('textEntry' in content) {
                    // Text cell
                    return content.textEntry.label;
                } else if ('integerEntry' in content) {
                    // Integer cell
                    return content.integerEntry.value;
                } else if ('numberEntry' in content) {
                    // Number cell
                    return content.numberEntry.value;
                } else if ('dateEntry' in content) {
                    // Date cell
                    return content.dateEntry.label;
                } else if ('datetimeEntry' in content) {
                    // Datetime cell
                    return content.datetimeEntry.label;
                } else if ('checkboxEntry' in content) {
                    // Checkbox cell
                    return content.checkboxEntry.isChecked;
                } else if ('fileEntry' in content) {
                    // File cell
                    return content.fileEntry.label;
                } else if ('imageEntry' in content) {
                    // Image cell
                    return content.imageEntry.label;
                } else if ('schemaLink' in content) {
                    // Schema link cell
                    return content.schemaLink.label;
                } else if ('objectLink' in content) {
                    // Object link cell
                    return content.objectLink.label;
                } else if ('singleSelectDropdown' in content) {
                    // Single-Select Dropdown cell
                    return content.singleSelectDropdown.dropdownRowOid;
                } else if ('multiSelectDropdown' in content) {
                    // Multi-Select Dropdown cell
                    return content.multiSelectDropdown.dropdownRowOid;
                } else {
                    // Readonly cell
                    return content.readonly.label;
                }
            } else {
                return null;
            }
        },
        set(target, prop, value, receiver): boolean {
            if (String(prop).startsWith('column') && String(prop).endsWith('content')) {
                Reflect.set(target, String(prop).replace('content', ''), value, receiver);
                return true;
            } else if (!String(prop).startsWith('column')) {
                return false;
            }

            const content: CellContent | null | undefined = Reflect.get(target, prop, receiver) as CellContent | null | undefined;
            if (content) {
                let promise: Promise<void>;
                let cellIdentifier: CellIdentifier;
                if ('textEntry' in content) {
                    // Text cell
                    promise = executeAsync({
                        editCellContents: {
                            tableOid: content.textEntry.dataTableOid,
                            columnOid: content.textEntry.dataColumnOid,
                            rowOid: content.textEntry.dataRowOid,
                            value: {
                                text: typeof value === 'string' ? (value ?? null) : null
                            }
                        }
                    });
                    cellIdentifier = content.textEntry.cellIdentifier;
                } else if ('integerEntry' in content) {
                    // Integer cell
                    promise = executeAsync({
                        editCellContents: {
                            tableOid: content.integerEntry.dataTableOid,
                            columnOid: content.integerEntry.dataColumnOid,
                            rowOid: content.integerEntry.dataRowOid,
                            value: {
                                integer: typeof value === 'number' ? Math.floor(value) : (typeof value === 'string' && Number.isFinite(parseInt(value)) ? parseInt(value) : null)
                            }
                        }
                    });
                    cellIdentifier = content.integerEntry.cellIdentifier;
                } else if ('numberEntry' in content) {
                    // Number cell
                    promise = executeAsync({
                        editCellContents: {
                            tableOid: content.numberEntry.dataTableOid,
                            columnOid: content.numberEntry.dataColumnOid,
                            rowOid: content.numberEntry.dataRowOid,
                            value: {
                                number: typeof value === 'number' ? value : (typeof value === 'string' && Number.isFinite(parseFloat(value)) ? parseFloat(value) : null)
                            }
                        }
                    });
                    cellIdentifier = content.numberEntry.cellIdentifier;
                } else if ('dateEntry' in content) {
                    // Date cell
                    const date: Date | null = typeof value === 'string' ? (new Date(value) ?? null) : null;
                    promise = executeAsync({
                        editCellContents: {
                            tableOid: content.dateEntry.dataTableOid,
                            columnOid: content.dateEntry.dataColumnOid,
                            rowOid: content.dateEntry.dataRowOid,
                            value: {
                                date: {
                                    label: date?.toISOString() ?? null
                                }
                            }
                        }
                    });
                    cellIdentifier = content.dateEntry.cellIdentifier;
                } else if ('datetimeEntry' in content) {
                    // Datetime cell
                    const datetime: Date | null = typeof value === 'string' ? (new Date(value) ?? null) : null;
                    promise = executeAsync({
                        editCellContents: {
                            tableOid: content.datetimeEntry.dataTableOid,
                            columnOid: content.datetimeEntry.dataColumnOid,
                            rowOid: content.datetimeEntry.dataRowOid,
                            value: {
                                datetime: {
                                    label: datetime?.toISOString() ?? null
                                }
                            }
                        }
                    });
                    cellIdentifier = content.datetimeEntry.cellIdentifier;
                } else if ('checkboxEntry' in content) {
                    // Checkbox cell
                    promise = executeAsync({
                        editCellContents: {
                            tableOid: content.checkboxEntry.dataTableOid,
                            columnOid: content.checkboxEntry.dataColumnOid,
                            rowOid: content.checkboxEntry.dataRowOid,
                            value: {
                                boolean: typeof value === 'boolean' ? value : null
                            }
                        }
                    });
                    cellIdentifier = content.checkboxEntry.cellIdentifier;
                } else if ('objectLink' in content) {
                    // Object link cell
                    promise = executeAsync({
                        editCellContents: {
                            tableOid: content.objectLink.dataTableOid,
                            columnOid: content.objectLink.dataColumnOid,
                            rowOid: content.objectLink.dataRowOid,
                            value: {
                                object: {
                                    linkedRowOid: 'new'
                                }
                            }
                        }
                    });
                    cellIdentifier = content.objectLink.cellIdentifier;
                } else if ('singleSelectDropdown' in content) {
                    // Single-Select Dropdown cell
                    promise = executeAsync({
                        editCellContents: {
                            tableOid: content.singleSelectDropdown.dataTableOid,
                            columnOid: content.singleSelectDropdown.dataColumnOid,
                            rowOid: content.singleSelectDropdown.dataRowOid,
                            value: {
                                select: {
                                    linkedRowOid: typeof value === 'number' ? value : null
                                }
                            }
                        }
                    });
                    cellIdentifier = content.singleSelectDropdown.cellIdentifier;
                } else if ('multiSelectDropdown' in content) {
                    // Multi-Select Dropdown cell
                    promise = executeAsync({
                        editCellContents: {
                            tableOid: content.multiSelectDropdown.dataTableOid,
                            columnOid: content.multiSelectDropdown.dataColumnOid,
                            rowOid: content.multiSelectDropdown.dataRowOid,
                            value: {
                                multiselect: {
                                    linkedRowOid: Array.isArray(value) && value.every((item) => typeof item === 'number') ? value : (typeof value === 'number' ? [value] : [])
                                }
                            }
                        }
                    });
                    cellIdentifier = content.multiSelectDropdown.cellIdentifier;
                } else {
                    // Cell cannot be edited normally
                    return false;
                }

                promise.catch(onError);

                return true;
            } else {
                return false;
            }
        }
    });
}




/**
 * Maps cell content to a reference to the corresponding table cell.
 */
export function cellDataRef(content: CellContent): { tableOid: number, columnOid: number, rowOid: number } | undefined {
    if ('textEntry' in content) {
        // Text cell
        return {
            tableOid: content.textEntry.dataTableOid,
            columnOid: content.textEntry.dataColumnOid,
            rowOid: content.textEntry.dataRowOid
        };
    } else if ('integerEntry' in content) {
        // Integer cell
        return {
            tableOid: content.integerEntry.dataTableOid,
            columnOid: content.integerEntry.dataColumnOid,
            rowOid: content.integerEntry.dataRowOid
        };
    } else if ('numberEntry' in content) {
        // Number cell
        return {
            tableOid: content.numberEntry.dataTableOid,
            columnOid: content.numberEntry.dataColumnOid,
            rowOid: content.numberEntry.dataRowOid
        };
    } else if ('dateEntry' in content) {
        // Date cell
        return {
            tableOid: content.dateEntry.dataTableOid,
            columnOid: content.dateEntry.dataColumnOid,
            rowOid: content.dateEntry.dataRowOid
        };
    } else if ('datetimeEntry' in content) {
        // Datetime cell
        return {
            tableOid: content.datetimeEntry.dataTableOid,
            columnOid: content.datetimeEntry.dataColumnOid,
            rowOid: content.datetimeEntry.dataRowOid
        };
    } else if ('checkboxEntry' in content) {
        // Checkbox cell
        return {
            tableOid: content.checkboxEntry.dataTableOid,
            columnOid: content.checkboxEntry.dataColumnOid,
            rowOid: content.checkboxEntry.dataRowOid
        };
    } else if ('fileEntry' in content) {
        // File cell
        return {
            tableOid: content.fileEntry.dataTableOid,
            columnOid: content.fileEntry.dataColumnOid,
            rowOid: content.fileEntry.dataRowOid
        };
    } else if ('imageEntry' in content) {
        // Image cell
        return {
            tableOid: content.imageEntry.dataTableOid,
            columnOid: content.imageEntry.dataColumnOid,
            rowOid: content.imageEntry.dataRowOid
        };
    } else if ('schemaLink' in content) {
        // Schema link cell
        return undefined;
    } else if ('objectLink' in content) {
        // Object link cell
        return {
            tableOid: content.objectLink.dataTableOid,
            columnOid: content.objectLink.dataColumnOid,
            rowOid: content.objectLink.dataRowOid
        };
    } else if ('singleSelectDropdown' in content) {
        // Single-Select Dropdown cell
        return {
            tableOid: content.singleSelectDropdown.dataTableOid,
            columnOid: content.singleSelectDropdown.dataColumnOid,
            rowOid: content.singleSelectDropdown.dataRowOid
        };
    } else if ('multiSelectDropdown' in content) {
        // Multi-Select Dropdown cell
        return {
            tableOid: content.multiSelectDropdown.dataTableOid,
            columnOid: content.multiSelectDropdown.dataColumnOid,
            rowOid: content.multiSelectDropdown.dataRowOid
        };
    } else {
        // Readonly cell
        return undefined;
    }
}


/**
 * Converts an absolute filepath to a filepath relative to the resource directory.
 * @param absoluteFilepath An absolute filepath.
 * @returns A relative filepath.
 */
async function convertAbsoluteToRelative(absoluteFilepath: string): Promise<string> {
    const baseDir: string = await resourceDir();

    function trim(arr: string[]) {
        var start = 0;
        for (; start < arr.length; start++) {
            if (arr[start] !== '') break;
        }

        var end = arr.length - 1;
        for (; end >= 0; end--) {
            if (arr[end] !== '') break;
        }

        if (start > end) return [];
        return arr.slice(start, end - start + 1);
    }

    const pathSeparator = await sep();
    const fromParts = trim(baseDir.split(pathSeparator));
    const toParts = trim(absoluteFilepath.split(pathSeparator));
    console.log(`Separator: '${pathSeparator}'`);
    console.log(fromParts);
    console.log(toParts);

    const length = Math.min(fromParts.length, toParts.length);
    let samePartsLength = length;
    for (let i: number = 0; i < length; i++) {
        if (fromParts[i] !== toParts[i]) {
            samePartsLength = i;
            break;
        }
    }

    var outputParts = [];
    for (var i = samePartsLength; i < fromParts.length; i++) {
        outputParts.push('..');
    }

    outputParts = outputParts.concat(toParts.slice(samePartsLength));
    return outputParts.join(pathSeparator);
}

export function useBaseColumnTypes(onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void, onRequestOpenObject: (object: ObjectPageBreadcrumb) => void, onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void, onError: (e: unknown) => void) {
    return useMemo<{[key: string]: RevoGridColumnType}>(() => {
        return {
            'any': {
                cellTemplate(createElement, props) {
                    const content: CellContent | null | undefined = props.model[`${String(props.prop)}content`];

                    if (content) {
                        if ('integerEntry' in content) {
                            return createElement(
                                'div',
                                {
                                    'class': classNames(
                                        `column${content.integerEntry.cellIdentifier.columnOid}`,
                                        'truncate',
                                        'text-right'
                                    )
                                },
                                content.integerEntry.value?.toString() || ''
                            );
                        } else if ('numberEntry' in content) {
                            return createElement(
                                'div',
                                {
                                    'class': classNames(
                                        `column${content.numberEntry.cellIdentifier.columnOid}`
                                    )
                                },
                                content.numberEntry.value?.toString() || ''
                            );
                        } else if ('checkboxEntry' in content) {
                            const isChecked: boolean = content.checkboxEntry.isChecked ?? false;
                            return createElement(
                                'label',
                                {
                                    'class': classNames(
                                        `column${content.checkboxEntry.cellIdentifier.columnOid}`,
                                        'relative',
                                        'w-full',
                                        'h-full',
                                        'flex',
                                        'justify-center',
                                        'cursor-pointer'
                                    )
                                },
                                createElement(
                                    'div',
                                    {
                                        'class': "group shadow-sm shadow-black/5 inline-block relative h-3 w-3 mt-2 rounded bg-transparent border border-surface transition-all duration-200 ease-in aria-disabled:opacity-50 aria-disabled:pointer-events-none hover:shadow-md data-[checked=true]:bg-primary data-[checked=true]:border-primary text-primary-foreground",
                                        'data-checked': isChecked ? 'true' : 'false',
                                    },
                                    createElement(
                                        'input',
                                        {
                                            'type': "checkbox",
                                            'checked': isChecked,
                                            'class': classNames('hidden'),
                                            'onChange': (_e: InputEvent) => {
                                                console.log(`Setting ${String(props.prop)} to ${!isChecked}`);
                                                props.model[props.prop] = !isChecked;
                                            }
                                        }
                                    )
                                )
                            );
                        } else if ('dateEntry' in content) {
                            const date: Date | null = content.dateEntry.label ? new Date(content.dateEntry.label) : null;
                            return createElement(
                                'div',
                                {
                                    'class': classNames(
                                        `column${content.dateEntry.cellIdentifier.columnOid}`
                                    )
                                },
                                date?.toLocaleDateString() ?? ''
                            );
                        } else if ('datetimeEntry' in content) {
                            const datetime: Date | null = content.datetimeEntry.label ? new Date(content.datetimeEntry.label) : null;
                            return createElement(
                                'div',
                                {
                                    'class': classNames(
                                        `column${content.datetimeEntry.cellIdentifier.columnOid}`
                                    )
                                },
                                datetime?.toLocaleString() ?? ''
                            );
                        } else if ('fileEntry' in content) {
                            const uploadFile = async () => {
                                const absoluteFilepath = await open({
                                    filters: [{
                                        name: 'All Files',
                                        extensions: ['*']
                                    }]
                                });
                                if (absoluteFilepath) {
                                    const relativeFilepath = await convertAbsoluteToRelative(absoluteFilepath);
                                    onRequestUploadFile(
                                        absoluteFilepath,
                                        relativeFilepath,
                                        async (fileOid: number) => {
                                            try {
                                                await executeAsync({
                                                    editCellContents: {
                                                        tableOid: content.fileEntry.dataTableOid,
                                                        columnOid: content.fileEntry.dataColumnOid,
                                                        rowOid: content.fileEntry.dataRowOid,
                                                        value: {
                                                            file: { fileOid }
                                                        }
                                                    }
                                                });
                                            } catch (e) {
                                                onError(e);
                                            }
                                        }
                                    );
                                }
                            };

                            if (content.fileEntry.fileOid !== null) {

                            } else {
                                
                            }
                        } else if ('imageEntry' in content) {
                            const uploadImage = async () => {
                                const absoluteFilepath = await open({
                                    filters: [{
                                        name: 'Image (*.png, *.jpg, *.jpeg, *.gif)',
                                        extensions: ['png', 'jpg', 'jpeg', 'gif']
                                    }, {
                                        name: 'All Files',
                                        extensions: ['*']
                                    }]
                                });
                                if (absoluteFilepath) {
                                    const relativeFilepath = await convertAbsoluteToRelative(absoluteFilepath);
                                    console.log(relativeFilepath);
                                    onRequestUploadFile(
                                        absoluteFilepath,
                                        relativeFilepath,
                                        async (fileOid: number) => {
                                            try {
                                                await executeAsync({
                                                    editCellContents: {
                                                        tableOid: content.imageEntry.dataTableOid,
                                                        columnOid: content.imageEntry.dataColumnOid,
                                                        rowOid: content.imageEntry.dataRowOid,
                                                        value: {
                                                            file: { fileOid }
                                                        }
                                                    }
                                                });
                                            } catch (e) {
                                                onError(e);
                                            }
                                        }
                                    );
                                }
                            };

                            if (content.imageEntry.file !== null) {
                                const file = content.imageEntry.file;

                                return createElement(
                                    'div',
                                    {
                                        'class': classNames(
                                            'relative',
                                            'w-full',
                                            'h-full',
                                            'inline-block',
                                        )
                                    },
                                    [
                                        createElement(
                                            'img',
                                            {
                                                'class': classNames(
                                                    'object-scale-down',
                                                    'm-auto',
                                                ),
                                                'src': content.imageEntry.fileSrc ? new URL(content.imageEntry.fileSrc, import.meta.url) : null
                                            }
                                        ),
                                        createElement(
                                            'div',
                                            {
                                                'class': classNames(
                                                    'absolute',
                                                    'top-0',
                                                    'bottom-0',
                                                    'left-0',
                                                    'right-0',
                                                    'flex',
                                                    'justify-start',
                                                    'justify-items-start',
                                                    'content-end',
                                                    'items-end',
                                                )
                                            },
                                            [
                                                createElement(
                                                    'img',
                                                    {
                                                        'class': classNames(
                                                            'opacity-25',
                                                            'hover:opacity-100',
                                                            'transition-opacity',
                                                            'cursor-pointer',
                                                            'object-scale-down',
                                                            'shrink',
                                                        ),
                                                        'src': downloadIconSrc,
                                                        'onClick': async () => {
                                                            const saveFilepath = await save({
                                                                filters: [{
                                                                    name: 'Image (*.png, *.jpg, *.jpeg, *.gif)',
                                                                    extensions: ['png', 'jpg', 'jpeg', 'gif']
                                                                }, {
                                                                    name: 'All Files',
                                                                    extensions: ['*']
                                                                }]
                                                            });
                                                            if (saveFilepath) {
                                                                try {
                                                                    await downloadFileAsync({
                                                                        fileOid: 'path' in file ? file.path.oid : file.blob.oid,
                                                                        downloadToPath: saveFilepath
                                                                    });
                                                                } catch (e) {
                                                                    onError(e);
                                                                }
                                                            }
                                                        }
                                                    }
                                                ),
                                                createElement(
                                                    'img',
                                                    {
                                                        'class': classNames(
                                                            'opacity-25',
                                                            'hover:opacity-100',
                                                            'transition-opacity',
                                                            'cursor-pointer',
                                                            'object-scale-down',
                                                            'shrink',
                                                        ),
                                                        'src': uploadIconSrc,
                                                        'onClick': async () => { await uploadImage(); }
                                                    }
                                                )
                                            ]
                                        )
                                    ]
                                )
                            } else {
                                return createElement(
                                    'div',
                                    {
                                        'class': classNames(
                                            'absolute',
                                            'w-full',
                                            'h-full',
                                            'flex',
                                            'justify-start',
                                            'justify-items-start',
                                            'content-end',
                                            'items-end',
                                        )
                                    },
                                    [
                                        createElement(
                                            'img',
                                            {
                                                'class': classNames(
                                                    'opacity-25',
                                                    'hover:opacity-100',
                                                    'transition-opacity',
                                                    'cursor-pointer',
                                                    'object-scale-down',
                                                    'shrink',
                                                ),
                                                'src': uploadIconSrc,
                                                'onClick': async () => { await uploadImage(); }
                                            }
                                        )
                                    ]
                                );
                            }
                        } else if ('objectLink' in content) {
                            return createElement(
                                'a',
                                {
                                    'class': classNames(
                                        `column${content.objectLink.cellIdentifier.columnOid}`,
                                        'inline-block',
                                        'w-[calc(100%)]',
                                        'h-full',
                                        'text-center',
                                        'truncate',
                                        'text-[rgb(var(--color-primary)/1)]',
                                        'cursor-pointer',
                                    ),
                                    'onClick': async () => {
                                        if (content.objectLink.linkRowOid) {
                                            const columnMetadata = await getColumnAsync(content.objectLink.cellIdentifier.columnOid);
                                            const schema = await getSchemaMetadataAsync(content.objectLink.linkSchemaOid);
                                            onRequestOpenObject({
                                                name: columnMetadata.name,
                                                schema,
                                                oidFilters: [['OID', content.objectLink.linkRowOid]]
                                            });
                                        } else {
                                            await executeAsync({
                                                editCellContents: {
                                                    tableOid: content.objectLink.dataTableOid,
                                                    columnOid: content.objectLink.dataColumnOid,
                                                    rowOid: content.objectLink.dataRowOid,
                                                    value: {
                                                        object: {
                                                            linkedRowOid: 'new'
                                                        }
                                                    }
                                                }
                                            })
                                            .catch(onError);
                                        }
                                    }
                                },
                                content.objectLink.label ?? ''
                            );
                        } else if ('schemaLink' in content) {
                            return createElement(
                                'a',
                                {
                                    'class': classNames(
                                        `column${content.schemaLink.cellIdentifier.columnOid}`,
                                        'inline-block',
                                        'w-[calc(100%)]',
                                        'h-full',
                                        'text-center',
                                        'truncate',
                                        'text-[rgb(var(--color-primary)/1)]',
                                        'cursor-pointer',
                                    ),
                                    'onClick': async () => {
                                        const columnMetadata = await getColumnAsync(content.schemaLink.cellIdentifier.columnOid);
                                        const schema = await getSchemaMetadataAsync(content.schemaLink.linkSchemaOid);
                                        onRequestOpenSchema({
                                            name: columnMetadata.name,
                                            schema,
                                            oidFilters: content.schemaLink.linkOidFilters,
                                            customFilters: []
                                        });
                                    }
                                },
                                content.schemaLink.label ?? ''
                            );
                        } else if ('singleSelectDropdown' in content) {

                        } else if ('multiSelectDropdown' in content) {

                        } else {
                            const { 
                                cellIdentifier: contentCellIdentifier, 
                                format: contentFormat, 
                                label: contentLabel 
                            } = 'textEntry' in content ? content.textEntry : content.readonly;
                            switch (contentFormat) {
                                default:
                                    return createElement(
                                        'div',
                                        {
                                            'class': classNames(
                                                `column${contentCellIdentifier.columnOid}`,
                                                'text-wrap'
                                            )
                                        },
                                        contentLabel || ''
                                    );
                            }
                        }
                    }
                    
                    return createElement('div', []);
                }
            }
        };
    }, [onError]);
}

export function useExtraColumnTypes(columns: ColumnFullMetadata[], onError: (e: unknown) => void) {
    const [dropdownValues, setDropdownValues] = useState<{[key: `table${number}`]: { label: string, value: string }[]}>({});
    
    useEffect(() => {
        const tableOids: Set<number> = new Set();
        for (const columnMetadata of columns) {
            if ('select' in columnMetadata.columnType) {
                tableOids.add(columnMetadata.columnType.select.tableOid);
            } else if ('multiselect' in columnMetadata.columnType) {
                tableOids.add(columnMetadata.columnType.multiselect.tableOid);
            }
        }
        Promise.all([...tableOids].map<Promise<[`table${number}`, { label: string, value: string }[]]>>(async (tableOid) => {
            const tableDropdownValues: { label: string, value: string }[] = [];
            await queryAsync({
                tableRowLabels: {
                    tableOid: tableOid,
                    channel: new Channel((dropdownValue) => {
                        tableDropdownValues.push({ 
                            label: dropdownValue.label,
                            value: dropdownValue.value.toString()
                        });
                    })
                }
            });
            return [`table${tableOid}`, tableDropdownValues];
        }))
        .then((entries) => {
            setDropdownValues(Object.fromEntries(entries));
        })
        .catch((e) => {
            onError(e);
        });
    }, [columns]);

    const extraColumnTypes: {[key: string]: RevoGridColumnType} = useMemo(() => {
        const extras: {[key: string]: any} = {};
        for (const columnMetadata of columns) {
            if ('select' in columnMetadata.columnType) {
                const key: string = `singleSelectDropdown${columnMetadata.columnType.select.tableOid}`;
                if (!(key in extras)) {
                    extras[key] = {

                    };
                }
            } else if ('multiselect' in columnMetadata.columnType) {
                const key: string = `multiSelectDropdown${columnMetadata.columnType.multiselect.tableOid}`;
                if (!(key in extras)) {
                    extras[key] = {

                    };
                }
            }
        }
        return extras;
    }, [columns, dropdownValues]);
    return extraColumnTypes;
}