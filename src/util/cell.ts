import { listen, UnlistenFn } from "@tauri-apps/api/event";
import { FullMetadata as ColumnFullMetadata, Primitive, ColumnType } from "./column";
import { openDialogAsync } from "./dialog";
import { executeAsync } from "./action";
import { open, save, message, ask } from "@tauri-apps/plugin-dialog";
import { DropdownValue, getCellAsync, getImageSrcAsync, getProcessidAsync, queryAsync, SelectedHierarchicalListItemMetadata, uploadFileAsync } from "./query";
import { fileTypeFromBuffer, FileTypeResult } from "file-type";
import { Channel } from "@tauri-apps/api/core";
import { Menu, MenuItem } from "@tauri-apps/api/menu";
import jSuites, {} from "jsuites";
import drop from "@interactjs/actions/drop/plugin";
import { DropdownItem } from "jsuites/dist/types/dropdown";


/**
 * Clipboard data for cells.
 */
export type ClipboardCellsData = {
    content: ClipboardCellData[][],
    shape: 'rect' | 'free'
} | {
    content: ClipboardCellData,
    shape: 'cell'
};




type ValidationFailures = {
    message: string
}[];

type CellDependency = {
    tableOid: number,
    columnOid: number,
    rowOid: number
};

export type CellIdentifier = {
    tableOid: number,
    columnOid: number,
    rowOid: number
} | {
    columnOid: number,
    queryFilter: string,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[]
};

export type File = {
    path: {
        oid: number,
        path: string 
    }
} | {
    blob: {
        oid: number
    }
};

export type SchemaRow = {
    rowIdentifier: [number, number] | null,
    index: number,
    fixedParentDatasource: [number, number, ColumnFullMetadata] | null,
    validationFailures: ValidationFailures
};
export type AddNewRowButton = {
    tableOid: number,
    fixedParentDatasource: [number, number, ColumnFullMetadata] | null,
    columnSpan: number
};


type CellContentTextFormat = 'plain' | 'jSON';

type ReadonlyCellContent = {
    cellIdentifier: CellIdentifier,
    label: string | null,
    format: CellContentTextFormat,
    validationFailures: ValidationFailures
};
type TextEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    format: CellContentTextFormat,
    validationFailures: ValidationFailures 
};
type IntegerEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    value: number | null,
    validationFailures: ValidationFailures 
};
type NumberEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    value: number | null,
    validationFailures: ValidationFailures 
};
type DateEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    validationFailures: ValidationFailures 
};
type DatetimeEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    validationFailures: ValidationFailures 
};
type CheckboxEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    isChecked: boolean | null,
    validationFailures: ValidationFailures 
};
type FileEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    fileOid: number | null,
    validationFailures: ValidationFailures
};
type ImageEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    file: File | null,
    label: string | null,
    fileSrc: string | null,
    validationFailures: ValidationFailures
};
type SchemaLinkCellContent = {
    cellIdentifier: CellIdentifier,
    label: string | null,
    linkSchemaOid: number,
    linkQueryFilter: string | null,
    validationFailures: ValidationFailures
};
type ObjectLinkCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    linkSchemaOid: number,
    linkRowOid: number | null,
    linkQueryFilter: string | null,
    clipboardData: [number, DataCellEntry[]] | null,
    validationFailures: ValidationFailures
};
type SingleSelectDropdownCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    dropdownTableOid: number,
    dropdownRowOid: number | null,
    validationFailures: ValidationFailures
};
type MultiSelectDropdownCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    dropdownTableOid: number,
    dropdownRowOid: number[],
    validationFailures: ValidationFailures
};

export type CellContent = { readonly: ReadonlyCellContent } 
| { textEntry: TextEntryCellContent }
| { integerEntry: IntegerEntryCellContent }
| { numberEntry: NumberEntryCellContent }
| { dateEntry: DateEntryCellContent }
| { datetimeEntry: DatetimeEntryCellContent }
| { checkboxEntry: CheckboxEntryCellContent } 
| { fileEntry: FileEntryCellContent }
| { imageEntry: ImageEntryCellContent }
| { schemaLink: SchemaLinkCellContent }
| { objectLink: ObjectLinkCellContent } 
| { singleSelectDropdown: SingleSelectDropdownCellContent } 
| { multiSelectDropdown: MultiSelectDropdownCellContent };
export type CellStream = { cell: CellContent }
| { maxIndex: number }
| { row: SchemaRow } 
| { addNewRowButton: AddNewRowButton };


export type DataCellEntry = {
    tableOid: number,
    columnOid: number,
    rowOid: number,
    value: {
        text: string | null
    } | {
        integer: number | null 
    } | {
        number: number | null 
    } | {
        boolean: boolean | null 
    } | {
        date: {
            label: string | null 
        } 
    } | {
        datetime: {
            label: string | null 
        }
    } | {
        file: {
            fileOid: number | null 
        }
    } | {
        object: {
            linkedRowOid: 'new' | 'delete' | {
                setExisting: [number]
            } | {
                copyExisting: {
                    tableOid: number,
                    cells: DataCellEntry[]
                }
            }
        }
    } | {
        select: {
            linkedRowOid: number | null 
        }
    } | {
        multiselect: {
            linkedRowOid: number[]
        }
    }
};

function hasNullProperty(obj: any, prop: string) {
    return prop in obj ? obj[prop] === null : false;
}

function hasStringProperty(obj: any, prop: string) {
    return prop in obj ? (typeof obj[prop] === 'string' || obj[prop] instanceof String) : false;
}

function hasNumberProperty(obj: any, prop: string) {
    return prop in obj ? (typeof obj[prop] === 'number') : false;
}

function hasArrayProperty(obj: any, prop: string) {
    return prop in obj ? Array.isArray(obj[prop]) : false;
}

/**
 * Check if an arbitrary object is an instance of a DataCellEntry type.
 * @param obj An arbitrary object.
 * @returns True if the object can be cast to a DataCellEntry. False otherwise.
 */
function isDataCellEntry(obj: any): boolean {
    // Check for all properties
    if (!('tableOid' in obj) || typeof obj.tableOid !== 'number'
        || !('columnOid' in obj) || typeof obj.columnOid !== 'number'
        || !('rowOid' in obj) || typeof obj.rowOid !== 'number'
        || !('value' in obj)
    ) 
        return false;

    const value: any = obj.value;

    // Check for text
    if (hasStringProperty(value, 'text') || hasNullProperty(value, 'text'))
        return true;

    // Check for integer
    if (hasNumberProperty(value, 'integer') || hasNullProperty(value, 'integer')) 
        return true;

    // Check for number
    if (hasNumberProperty(value, 'number') || hasNullProperty(value, 'number'))
        return true;

    // Check for boolean
    if ('boolean' in value && (value.boolean === null || typeof value.boolean === 'boolean'))
        return true;
    
    // Check for date
    if ('date' in value) {
        return (hasStringProperty(value.date, 'label') || hasNullProperty(value.date, 'label'));
    }
    
    // Check for datetime
    if ('datetime' in value) {
        return (hasStringProperty(value.datetime, 'label') || hasNullProperty(value.datetime, 'label'));
    }

    // Check for file
    if ('file' in value) {
        return (hasNumberProperty(value.file, 'fileOid') || hasNullProperty(value.file, 'fileOid'));
    }

    // Check for object
    if ('object' in value) {
        return 'linkedRowOid' in value.object 
            && (value.object.linkedRowOid === 'new'
                || value.object.linkedRowOid === 'delete'
                || (hasArrayProperty(value.object.linkedRowOid, 'setExisting') && value.object.linkedRowOid.setExisting.length === 1 && typeof value.object.linkedRowOid.setExisting[0] === 'number')
                || ('copyExisting' in value.object.linkedRowOid 
                    && hasNumberProperty(value.object.linkedRowOid.copyExisting, 'tableOid')
                    && (hasArrayProperty(value.object.linkedRowOid.copyExisting, 'cells') && value.object.linkedRowOid.copyExisting.cells.every((item: any) => isDataCellEntry(item)))
                )
            )
    }

    // Check for select
    if ('select' in value) {
        return (hasNumberProperty(value.select, 'linkedRowOid') || hasNullProperty(value.select, 'linkedRowOid'));
    }

    // Check for multiselect
    if ('multiselect' in value) {
        return (hasArrayProperty(value.multiselect, 'linkedRowOid') && value.multiselect.linkedRowOid.every((item: any) => typeof item === 'number'));
    }

    // Matches no known type
    return false;
}


export type ClipboardCellData = {
    columnOid: number,
    value: null | {
        text: string
    } | {
        numeric: number
    } | {
        file: {
            oid: number,
            label: string | null
        }
    } | {
        object: {
            tableOid: number,
            label: string | null,
            data: DataCellEntry[]
        }
    } | {
        reference: {
            tableOid: number,
            label: string | null,
            rowOid: number[]
        }
    }
};

/**
 * Check if an arbitrary object is an instance of a ClipboardCellData type.
 * @param obj An arbitrary object.
 * @returns True if the object can be cast as a ClipboardCellData. False otherwise.
 */
export function isClipboardCellData(obj: any): boolean {
    if (obj === null)
        return true;
    if (typeof obj !== 'object')
        return false;
    
    // Check if text
    if (hasStringProperty(obj, 'text')) {
        return true;
    }

    // Check if numeric
    if (hasNumberProperty(obj, 'numeric')) {
        return true;
    }

    // Check if file
    if ('file' in obj) {
        return hasNumberProperty(obj.file, 'oid') 
            && (hasNullProperty(obj.file, 'label') || hasStringProperty(obj.file, 'label'));
    }

    // Check if object
    if ('object' in obj) {
        return hasNumberProperty(obj.object, 'tableOid')
            && (hasNullProperty(obj.object, 'label') || hasStringProperty(obj.object, 'label'))
            && ('data' in obj.object && Array.isArray(obj.object.data) && obj.object.data.every((item: any) => isDataCellEntry(item)));
    }

    // Check if reference
    if ('reference' in obj) {
        return hasNumberProperty(obj.reference, 'tableOid')
            && (hasNullProperty(obj.reference, 'label') || hasStringProperty(obj.reference, 'label'))
            && ('rowOid' in obj.reference && Array.isArray(obj.reference.rowOid) && obj.reference.rowOid.every((item: any) => typeof item === 'number'));
    }

    // Matches no clipboard type
    return false;
}

function clipboardAsText(data: ClipboardCellData): string | null {
    const value = data.value;
    if (value === null)
        return null;
    else if ('text' in value) 
        return value.text;
    else if ('numeric' in value)
        return value.numeric.toString();
    else if ('file' in value) 
        return value.file.label;
    else if ('object' in value)
        return value.object.label;
    else 
        return value.reference.label;
}

function clipboardAsNumber(data: ClipboardCellData): number | null {
    const value = data.value;
    if (value === null)
        return null;
    else if ('numeric' in value) 
        return value.numeric;
    else {
        let label: string | null;
        if ('text' in value)
            label = value.text;
        else if ('file' in value)
            label = value.file.label;
        else if ('object' in value)
            label = value.object.label;
        else 
            label = value.reference.label;
        const parsedLabel = label ? parseFloat(label) : null;
        return parsedLabel !== null && Number.isFinite(parsedLabel) ? parsedLabel : null;
    }
}

function clipboardAsFile(data: ClipboardCellData): number | null {
    const value = data.value;
    if (value !== null && 'file' in value) 
        return value.file.oid;
    else 
        return null;
}

function clipboardAsObject(data: ClipboardCellData): [number, DataCellEntry[]] | null {
    const value = data.value;
    if (value !== null && 'object' in value) 
        return [value.object.tableOid, value.object.data];
    else
        return null;
}

function clipboardAsReference(data: ClipboardCellData, tableOid: number): number[] {
    const value = data.value;
    if (value !== null && 'reference' in value && value.reference.tableOid == tableOid) 
        return value.reference.rowOid;
    else 
        return [];
}


export class Cell {
    elem: HTMLTableCellElement;
    content: CellContent;
    cellIdentifier: CellIdentifier;

    /**
     * Begins editing the cell.
     */
    #startEditingAsync: () => Promise<void> = async () => {};

    /**
     * A callback function for when the user starts editing the cell.
     */
    #startEditingCallbackFn: (cell: Cell) => Promise<void> = async () => {};

    /**
     * Sets a callback function for when the user starts editing the cell.
     */
    setStartEditingCallback(fn: (cell: Cell) => Promise<void>) {
        this.#startEditingCallbackFn = fn;
    }

    /**
     * Stops editing the cell, saving changes.
     */
    #stopEditingAsync: () => Promise<void> = async () => {};

    /**
     * Stops editing the cell, discarding changes.
     */
    #revertEditAsync: () => Promise<void> = async () => {};

    /**
     * A callback function for when the user stops editing the cell.
     */
    #stopEditingCallbackFn: (cell: Cell) => Promise<void> = async () => {};

    /**
     * Sets a callback function for when the user stops editing the cell.
     * @param fn 
     */
    setStopEditingCallback(fn: (cell: Cell) => Promise<void>) {
        this.#stopEditingCallbackFn = fn;
    }


    /**
     * Gets the contents of the cell, as clipboard data.
     */
    clip: ClipboardCellData;

    /**
     * Sets the contents of the cell, via clipboard data.
     */
    setAsync: (data: ClipboardCellData) => Promise<void>;

    /**
     * Clears the contents of the cell.
     */
    async clearAsync() {
        await this.setAsync({
            columnOid: this.cellIdentifier.columnOid,
            value: null 
        });
    }

    constructor(cwd: Document, content: CellContent) {
        this.content = content;

        // Construct the HTMLElement
        if ('textEntry' in content) {
            this.cellIdentifier = content.textEntry.cellIdentifier;
            this.elem = this.#constructTextEntryCell(cwd, content.textEntry);

            this.clip = {
                columnOid: content.textEntry.cellIdentifier.columnOid,
                value: content.textEntry.label ? { text: content.textEntry.label } : null
            };
            this.setAsync = async (data) => {
                await executeAsync({
                    editCellContents: {
                        tableOid: content.textEntry.dataTableOid,
                        columnOid: content.textEntry.dataColumnOid,
                        rowOid: content.textEntry.dataRowOid,
                        value: {
                            text: clipboardAsText(data)
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };


        } else if ('integerEntry' in content) {
            this.cellIdentifier = content.integerEntry.cellIdentifier;
            this.elem = this.#constructIntegerEntryCell(cwd, content.integerEntry);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: content.integerEntry.value !== null ? { numeric: content.integerEntry.value } : null
            };
            this.setAsync = async (data) => {
                const numericValue: number | null = clipboardAsNumber(data);

                await executeAsync({
                    editCellContents: {
                        tableOid: content.integerEntry.dataTableOid,
                        columnOid: content.integerEntry.dataColumnOid,
                        rowOid: content.integerEntry.dataRowOid,
                        value: {
                            integer: numericValue !== null && !Number.isNaN(numericValue) ? Math.floor(numericValue) : null
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };


        } else if ('numberEntry' in content) {
            this.cellIdentifier = content.numberEntry.cellIdentifier;
            this.elem = this.#constructNumberEntryCell(cwd, content.numberEntry);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: content.numberEntry.value !== null ? { numeric: content.numberEntry.value } : null
            };
            this.setAsync = async (data) => {
                const numericValue: number | null = clipboardAsNumber(data);

                await executeAsync({
                    editCellContents: {
                        tableOid: content.numberEntry.dataTableOid,
                        columnOid: content.numberEntry.dataColumnOid,
                        rowOid: content.numberEntry.dataRowOid,
                        value: {
                            integer: numericValue !== null && !Number.isNaN(numericValue) ? numericValue : null
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };


        } else if ('dateEntry' in content) {
            this.cellIdentifier = content.dateEntry.cellIdentifier;
            this.elem = this.#constructDateEntryCell(cwd, content.dateEntry);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: content.dateEntry.label ? { text: content.dateEntry.label } : null
            };
            this.setAsync = async (data) => {
                await executeAsync({
                    editCellContents: {
                        tableOid: content.dateEntry.dataTableOid,
                        columnOid: content.dateEntry.dataColumnOid,
                        rowOid: content.dateEntry.dataRowOid,
                        value: {
                            text: clipboardAsText(data)
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };


        } else if ('datetimeEntry' in content) {
            this.cellIdentifier = content.datetimeEntry.cellIdentifier;
            this.elem = this.#constructDatetimeEntryCell(cwd, content.datetimeEntry);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: content.datetimeEntry.label ? { text: content.datetimeEntry.label } : null
            };
            this.setAsync = async (data) => {
                await executeAsync({
                    editCellContents: {
                        tableOid: content.datetimeEntry.dataTableOid,
                        columnOid: content.datetimeEntry.dataColumnOid,
                        rowOid: content.datetimeEntry.dataRowOid,
                        value: {
                            text: clipboardAsText(data)
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };


        } else if ('checkboxEntry' in content) {
            this.cellIdentifier = content.checkboxEntry.cellIdentifier;
            this.elem = this.#constructCheckboxEntryCell(cwd, content.checkboxEntry);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: content.checkboxEntry.isChecked !== null ? { numeric: content.checkboxEntry ? 1 : 0 } : null
            };
            this.setAsync = async (data) => {
                const numericValue: number | null = clipboardAsNumber(data);

                await executeAsync({
                    editCellContents: {
                        tableOid: content.checkboxEntry.dataTableOid,
                        columnOid: content.checkboxEntry.dataColumnOid,
                        rowOid: content.checkboxEntry.dataRowOid,
                        value: {
                            boolean: numericValue !== null && !Number.isNaN(numericValue) ? (numericValue != 0) : null
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };


        } else if ('fileEntry' in content) {
            this.cellIdentifier = content.fileEntry.cellIdentifier;
            this.elem = this.#constructFileEntryCell(cwd, content.fileEntry);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: content.fileEntry.fileOid !== null ? {
                    file: {
                        oid: content.fileEntry.fileOid,
                        label: content.fileEntry.label
                    }
                } : null
            };
            this.setAsync = async (data) => {
                await executeAsync({
                    editCellContents: {
                        tableOid: content.fileEntry.dataTableOid,
                        columnOid: content.fileEntry.dataColumnOid,
                        rowOid: content.fileEntry.dataRowOid,
                        value: {
                            file: {
                                fileOid: clipboardAsFile(data)
                            }
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };


        } else if ('imageEntry' in content) {
            this.cellIdentifier = content.imageEntry.cellIdentifier;
            this.elem = this.#constructImageEntryCell(cwd, content.imageEntry);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: content.imageEntry.file !== null ? {
                    file: {
                        oid: 'path' in content.imageEntry.file ? content.imageEntry.file.path.oid : content.imageEntry.file.blob.oid,
                        label: content.imageEntry.label
                    }
                } : null
            };
            this.setAsync = async (data) => {
                await executeAsync({
                    editCellContents: {
                        tableOid: content.imageEntry.dataTableOid,
                        columnOid: content.imageEntry.dataColumnOid,
                        rowOid: content.imageEntry.dataRowOid,
                        value: {
                            file: {
                                fileOid: clipboardAsFile(data)
                            }
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };


        } else if ('schemaLink' in content) {
            this.cellIdentifier = content.schemaLink.cellIdentifier;
            this.elem = this.#constructSchemaLinkCell(cwd, content.schemaLink);

            // Clipboard has no data
            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: null
            };
            // Set and clear functions do nothing
            this.setAsync = async (_) => {};

        } else if ('objectLink' in content) {
            this.cellIdentifier = content.objectLink.cellIdentifier;
            this.elem = this.#constructObjectLinkCell(cwd, content.objectLink);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: content.objectLink.clipboardData ? {
                    object: {
                        tableOid: content.objectLink.clipboardData[0],
                        label: content.objectLink.label,
                        data: content.objectLink.clipboardData[1]
                    }
                } : null
            };
            this.setAsync = async (data) => {
                const obj = clipboardAsObject(data);
                if (obj) {
                    const [objectSubtypeTableOid, objectEntries] = obj;
                    await executeAsync({
                        editCellContents: {
                            tableOid: content.objectLink.dataTableOid,
                            columnOid: content.objectLink.dataColumnOid,
                            rowOid: content.objectLink.dataRowOid,
                            value: {
                                object: {
                                    linkedRowOid: {
                                        copyExisting: {
                                            tableOid: objectSubtypeTableOid,
                                            cells: objectEntries
                                        }
                                    }
                                }
                            }
                        }
                    })
                    .catch(async (e) => {
                        await message(e, {
                            title: 'Error while editing contents of cell.',
                            kind: 'error'
                        });
                    });
                }
            };

        } else if ('singleSelectDropdown' in content) { // Select cell
            this.cellIdentifier = content.singleSelectDropdown.cellIdentifier;
            this.elem = this.#constructSingleSelectDropdownCell(cwd, content.singleSelectDropdown);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: {
                    reference: {
                        tableOid: content.singleSelectDropdown.dropdownTableOid,
                        label: content.singleSelectDropdown.label,
                        rowOid: content.singleSelectDropdown.dropdownRowOid !== null ? [content.singleSelectDropdown.dropdownRowOid] : []
                    }
                }
            };
            this.setAsync = async (data) => {
                // If more than one record, arbitrarily choose the one with the lowest OID
                const recordRowOid: number[] = clipboardAsReference(data, content.singleSelectDropdown.dropdownTableOid).sort();
                await executeAsync({
                    editCellContents: {
                        tableOid: content.singleSelectDropdown.dataTableOid,
                        columnOid: content.singleSelectDropdown.dataColumnOid,
                        rowOid: content.singleSelectDropdown.dataRowOid,
                        value: {
                            select: {
                                linkedRowOid: recordRowOid.length > 0 ? recordRowOid[0] : null
                            }
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };

        } else if ('multiSelectDropdown' in content) { // Multiselect cell
            this.cellIdentifier = content.multiSelectDropdown.cellIdentifier;
            this.elem = this.#constructMultiSelectDropdownCell(cwd, content.multiSelectDropdown);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: {
                    reference: {
                        tableOid: content.multiSelectDropdown.dropdownTableOid,
                        label: content.multiSelectDropdown.label,
                        rowOid: content.multiSelectDropdown.dropdownRowOid
                    }
                }
            };
            this.setAsync = async (data) => {
                await executeAsync({
                    editCellContents: {
                        tableOid: content.multiSelectDropdown.dataTableOid,
                        columnOid: content.multiSelectDropdown.dataColumnOid,
                        rowOid: content.multiSelectDropdown.dataRowOid,
                        value: {
                            multiselect: {
                                linkedRowOid: clipboardAsReference(data, content.multiSelectDropdown.dropdownTableOid)
                            }
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'Error while editing contents of cell.',
                        kind: 'error'
                    });
                });
            };


        } else if ('readonly' in content) { // Readonly cell
            this.cellIdentifier = content.readonly.cellIdentifier;
            this.elem = this.#constructReadonlyCell(cwd, content.readonly);

            this.clip = {
                columnOid: this.cellIdentifier.columnOid,
                value: content.readonly.label ? { text: content.readonly.label } : null
            };
            // Set and clear functions do nothing
            this.setAsync = async (_) => {};

        } else { // Nonconforming cell content
            throw new Error(`Unknown cell content: ${JSON.stringify(content)}`);
        }

        // Add event listener to start editing on double-click
        this.elem.addEventListener('dblclick', async () => {
            await this.startEditingAsync();
        });
    }

    /**
     * Release resources and listeners.
     */
    destroy() {
        if (this.#unlistenForReload)
            this.#unlistenForReload();
    }

    /**
     * The user starts editing the cell.
     */
    async startEditingAsync(): Promise<void> {
        this.elem.classList.add('editing');
        await this.#startEditingCallbackFn(this);
        await this.#startEditingAsync();
    }

    /**
     * The user stops editing the cell, saving changes.
     */
    async stopEditingAsync(): Promise<void> {
        this.elem.classList.remove('editing');
        await this.#stopEditingCallbackFn(this);
        await this.#stopEditingAsync();
    }

    /**
     * The user stops editing the cell, discarding changes.
     */
    async revertEditAsync(): Promise<void> {
        this.elem.classList.remove('editing');
        await this.#stopEditingCallbackFn(this);
        await this.#revertEditAsync();
    }

    /**
     * Hot reloads and returns an updated cell.
     * @returns 
     */
    async getReloadedCellAsync(): Promise<Cell> {
        const content = await getCellAsync(this.cellIdentifier);
        console.debug(content);
        return new Cell(this.elem.ownerDocument, content);
    }

    /**
     * Function that unsets the listener for this cell.
     */
    #unlistenForReload: UnlistenFn | null = null;

    /**
     * Applies callback functions when either the cell needs to be hot reloaded or the entire schema needs to be reloaded.
     */
    async startListeningForReloadAsync({ hotReloadCallbackFn, fullReloadCallbackFn } : { hotReloadCallbackFn?: (newCell: Cell) => Promise<void>, fullReloadCallbackFn?: () => Promise<void> }): Promise<void> {
        this.#unlistenForReload = await listen<CellIdentifier>('cell', async (event) => {
            const cellIdentifier = event.payload;
            if ('tableOid' in cellIdentifier) {
                if ('tableOid' in this.cellIdentifier) {
                    if (cellIdentifier.tableOid == this.cellIdentifier.tableOid
                        && cellIdentifier.columnOid == this.cellIdentifier.columnOid
                        && cellIdentifier.rowOid == this.cellIdentifier.rowOid
                    ) {
                        // Only a hot reload of this cell is required
                        if (hotReloadCallbackFn) {
                            const newCell: Cell = await this.getReloadedCellAsync();
                            this.elem.replaceWith(newCell.elem);
                            this.destroy();
                            await hotReloadCallbackFn(newCell);
                        }
                    }
                } else {
                    if (this.cellIdentifier.fullReloadCellDependencies.some(cellDependency => (
                        cellIdentifier.tableOid == cellDependency.tableOid
                        && cellIdentifier.columnOid == cellDependency.columnOid
                        && cellIdentifier.rowOid == cellDependency.rowOid
                    ))) {
                        // Requires a full reload of the schema
                        if (fullReloadCallbackFn)
                            await fullReloadCallbackFn();
                    } else if (this.cellIdentifier.isolatedCellDependencies.some(cellDependency => (
                        cellIdentifier.tableOid == cellDependency.tableOid
                        && cellIdentifier.columnOid == cellDependency.columnOid
                        && cellIdentifier.rowOid == cellDependency.rowOid
                    ))) {
                        // Only a hot reload of this cell is required
                        if (hotReloadCallbackFn) {
                            const newCell: Cell = await this.getReloadedCellAsync();
                            this.elem.replaceWith(newCell.elem);
                            this.destroy();
                            await hotReloadCallbackFn(newCell);
                        }
                    }
                }
            }
        });
    }



    /**
     * Add a tooltip to an HTML element.
     * @param elem The HTML element.
     * @param tooltip The tooltip to append.
     */
    #addTooltip(elem: HTMLElement, tooltip: string) {
        const existingTooltip: string | null = elem.getAttribute('tooltip');
        elem.setAttribute('tooltip', existingTooltip ? `${existingTooltip} ${tooltip}` : tooltip);
    }

    /**
     * Add tooltips to indicate that there has been a failure in one of the column's validation checks.
     * @param elem The HTMLElement for the cell.
     * @param validationFailures The column's failed validation checks.
     */
    #addValidationFailureTooltips(elem: HTMLElement, validationFailures: ValidationFailures) {
        if (validationFailures.length > 0) {
            elem.classList.add('error');
            this.#addTooltip(elem, validationFailures.map(f => f.message).reduce((acc, m) => `${acc} ${m}`));
        }
    }



    /**
     * Constructs a DIV to display readonly text.
     * For JSON text, color-codes and beautifies the JSON.
     * @param cwd 
     * @param elem The parent HTMLElement.
     * @param label The text to display.
     * @param format The text format.
     * @returns 
     */
    #constructLabel(cwd: Document, elem: HTMLElement, label: string, format: CellContentTextFormat): HTMLDivElement {
        const div: HTMLDivElement = cwd.createElement('div');
        div.classList.add('label');

        if (format == 'plain') {
            elem.classList.add('plain');
            div.innerText = label;
        } else if (format == 'jSON') {
            elem.classList.add('json');
            if (label) {
                try {
                    function constructSpan(parent: HTMLElement, obj: any, level: number) {
                        const span: HTMLSpanElement = cwd.createElement('span');
                        span.setAttribute('level', level.toString());
                        if (Array.isArray(obj)) {
                            span.classList.add('array');

                            const openDelimiter: HTMLSpanElement = cwd.createElement('span');
                            openDelimiter.classList.add('delimiter');
                            openDelimiter.innerText = '[';
                            span.appendChild(openDelimiter);

                            const firstBreak = cwd.createTextNode('\n\t');
                            span.appendChild(firstBreak);

                            for (let k = 0; k < obj.length; ++k) {
                                const value = obj[k];
                                constructSpan(span, value, level + 1);

                                if (k < obj.length - 1) {
                                    const commaBreak = cwd.createTextNode(',\n\t');
                                    span.appendChild(commaBreak);
                                } else {
                                    const noCommaBreak = cwd.createTextNode('\n');
                                    span.appendChild(noCommaBreak);
                                }
                            }

                            const closeDelimiter: HTMLSpanElement = cwd.createElement('span');
                            closeDelimiter.classList.add('delimiter');
                            closeDelimiter.innerText = ']';
                            span.appendChild(closeDelimiter);
                        } else if (typeof obj === 'string' || obj instanceof String) {
                            span.classList.add('string');
                            span.innerText = `"${obj.replace('\\', '\\\\').replace('"', '\\"')}"`;
                        } else if (typeof obj === 'boolean') {
                            span.classList.add('boolean');
                            span.innerText = obj ? 'true' : 'false';
                        } else if (typeof obj === 'number' || typeof obj === 'bigint') {
                            span.classList.add('number');
                            span.innerText = obj.toString();
                        } else {
                            span.classList.add('object');

                            const openDelimiter: HTMLSpanElement = cwd.createElement('span');
                            openDelimiter.classList.add('delimiter');
                            openDelimiter.innerText = '{';
                            span.appendChild(openDelimiter);

                            const firstBreak = cwd.createTextNode('\n\t');
                            span.appendChild(firstBreak);

                            const entries = Object.entries(obj);
                            for (let k = 0; k < entries.length; ++k) {
                                const [key, value] = entries[k];
                                constructSpan(span, key, level + 1);
                                const colonBreak = cwd.createTextNode(': ');
                                span.appendChild(colonBreak);
                                constructSpan(span, value, level + 1);

                                if (k < entries.length - 1) {
                                    const commaBreak = cwd.createTextNode(',\n\t');
                                    span.appendChild(commaBreak);
                                } else {
                                    const noCommaBreak = cwd.createTextNode('\n');
                                    span.appendChild(noCommaBreak);
                                }
                            }

                            const closeDelimiter: HTMLSpanElement = cwd.createElement('span');
                            closeDelimiter.classList.add('delimiter');
                            closeDelimiter.innerText = '}';
                            span.appendChild(closeDelimiter);
                        }
                        parent.appendChild(span);
                    }

                    const parsedObj = JSON.parse(label);
                    constructSpan(div, parsedObj, 1);
                } catch (e) {
                    div.innerText = label;
                    this.#addValidationFailureTooltips(elem, [{ message: `${e}` }]);
                }
            }
        }
        elem.appendChild(div);
        return div;
    }


    /**
     * Construct a cell for free-text entry.
     * @param cwd 
     * @param content The content of a text entry cell.
     */
    #constructTextEntryCell(cwd: Document, content: TextEntryCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', content.format);
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'text';
        input.addEventListener('blur', async () => { await this.stopEditingAsync() });
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        const editingLock: string = JSON.stringify(this.cellIdentifier);
        let editing: boolean = false;

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (!editing) {
                    editing = true;
                    input.value = content.label || '';

                    // Remove the readonly text, insert the input
                    elem.removeChild(readonly);
                    elem.appendChild(input);
                    input.focus();
                    input.select();
                }
            });
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;
                    const rawLabel: string | null = input.value || null;
                    let label: string | null;
                    if (content.format == 'jSON' && rawLabel) {
                        // Automatically attempt to beautify the JSON
                        try {
                            label = JSON.stringify(JSON.parse(rawLabel), null, '\t');
                        } catch (e) {
                            label = rawLabel;
                        }
                    } else {
                        label = rawLabel;
                    }

                    if (label !== content.label) {
                        // Update the cell contents in the database
                        await executeAsync({
                            editCellContents: {
                                tableOid: content.dataTableOid,
                                columnOid: content.dataColumnOid,
                                rowOid: content.dataRowOid,
                                value: {
                                    text: label
                                }
                            }
                        }).catch(async (e) => {
                            await message(e, {
                                title: 'Unable to update cell contents.',
                                kind: 'error'
                            });
                        });
                    }

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#revertEditAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        return elem;
    }

    /**
     * Construct a cell for free-text integer entry.
     * @param cwd 
     * @param content The content of an integer entry cell.
     */
    #constructIntegerEntryCell(cwd: Document, content: IntegerEntryCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.value?.toString() || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.value?.toString() || '', 'plain');
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'number';
        input.step = '1';
        input.addEventListener('blur', async () => { await this.stopEditingAsync() });
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        const editingLock: string = JSON.stringify(this.cellIdentifier);
        let editing: boolean = false;

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (!editing) {
                    editing = true;
                    input.value = content.value?.toString() || '';

                    // Remove the readonly text, insert the input
                    elem.removeChild(readonly);
                    elem.appendChild(input);
                    input.focus();
                    input.select();
                }
            });
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Validate the entered value
                    const parsedValue: number = parseInt(input.value);
                    if (Number.isNaN(parsedValue) && input.value) {
                        await message(`Entered value is not an integer!`, {
                            kind: 'warning'
                        });
                        return;
                    }

                    const value: number | null = Number.isNaN(parsedValue) ? null : parsedValue;
                    if (value !== content.value) {
                        // Update the cell contents in the database
                        await executeAsync({
                            editCellContents: {
                                tableOid: content.dataTableOid,
                                columnOid: content.dataColumnOid,
                                rowOid: content.dataRowOid,
                                value: {
                                    integer: value
                                }
                            }
                        }).catch(async (e) => {
                            await message(e, {
                                title: 'Unable to update cell contents.',
                                kind: 'error'
                            });
                        });
                    }

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#revertEditAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        return elem;
    }

    /**
     * Construct a cell for free-text number entry.
     * @param cwd 
     * @param content The content of a number entry cell.
     */
    #constructNumberEntryCell(cwd: Document, content: NumberEntryCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.value?.toString() || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.value?.toString() || '', 'plain');
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'number';
        input.addEventListener('blur', async () => { await this.stopEditingAsync() });
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        const editingLock: string = JSON.stringify(this.cellIdentifier);
        let editing: boolean = false;

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (!editing) {
                    editing = true;
                    input.value = content.value?.toString() || '';

                    // Remove the readonly text, insert the input
                    elem.removeChild(readonly);
                    elem.appendChild(input);
                    input.focus();
                    input.select();
                }
            });
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Validate the entered value
                    const parsedValue: number = parseFloat(input.value);
                    if (Number.isNaN(parsedValue) && input.value) {
                        await message(`Entered value is not a number!`, {
                            kind: 'warning'
                        });
                        return;
                    }

                    const value: number | null = Number.isNaN(parsedValue) ? null : parsedValue;
                    if (value !== content.value) {
                        // Update the cell contents in the database
                        await executeAsync({
                            editCellContents: {
                                tableOid: content.dataTableOid,
                                columnOid: content.dataColumnOid,
                                rowOid: content.dataRowOid,
                                value: {
                                    number: value
                                }
                            }
                        }).catch(async (e) => {
                            await message(e, {
                                title: 'Unable to update cell contents.',
                                kind: 'error'
                            });
                        });
                    }

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        // When you stop editing, discard changes
        this.#revertEditAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        return elem;
    }



    /**
     * Construct a cell for date entry.
     * @param cwd 
     * @param content The content of a date entry cell.
     */
    #constructDateEntryCell(cwd: Document, content: DateEntryCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'date';
        input.addEventListener('blur', async () => { await this.stopEditingAsync() });
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        const editingLock: string = JSON.stringify(this.cellIdentifier);
        let editing: boolean = false;

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (!editing) {
                    editing = true;
                    input.value = content.label || '';

                    // Remove the readonly text, insert the input
                    elem.removeChild(readonly);
                    elem.appendChild(input);
                    input.focus();
                    input.select();
                }
            });
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    const label: string | null = input.value || null;
                    if (label !== content.label) {
                        // Update the cell contents in the database
                        await executeAsync({
                            editCellContents: {
                                tableOid: content.dataTableOid,
                                columnOid: content.dataColumnOid,
                                rowOid: content.dataRowOid,
                                value: {
                                    date: {
                                        label: label
                                    }
                                }
                            }
                        }).catch(async (e) => {
                            await message(e, {
                                title: 'Unable to update cell contents.',
                                kind: 'error'
                            });
                        });
                    }

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        // When you stop editing, discard changes
        this.#revertEditAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        return elem;
    }

    /**
     * Construct a cell for datetime entry.
     * @param cwd 
     * @param content The content of a datetime entry cell.
     */
    #constructDatetimeEntryCell(cwd: Document, content: DatetimeEntryCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'datetime-local';
        input.addEventListener('blur', async () => { await this.stopEditingAsync() });
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        const editingLock: string = JSON.stringify(this.cellIdentifier);
        let editing: boolean = false;

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (!editing) {
                    editing = true;
                    input.value = content.label || '';

                    // Remove the readonly text, insert the input
                    elem.removeChild(readonly);
                    elem.appendChild(input);
                    input.focus();
                    input.select();
                }
            });
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;
                    
                    const label: string | null = input.value || null;
                    if (label !== content.label) {
                        // Update the cell contents in the database
                        await executeAsync({
                            editCellContents: {
                                tableOid: content.dataTableOid,
                                columnOid: content.dataColumnOid,
                                rowOid: content.dataRowOid,
                                value: {
                                    datetime: {
                                        label: label
                                    }
                                }
                            }
                        }).catch(async (e) => {
                            await message(e, {
                                title: 'Unable to update cell contents.',
                                kind: 'error'
                            });
                        });
                    }

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        // When you stop editing, discard changes
        this.#revertEditAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        return elem;
    }



    /**
     * Construct a cell that contains a checkbox that toggles the boolean value of a cell.
     * @param cwd 
     * @param content The content of a checkbox entry cell.
     * @returns 
     */
    #constructCheckboxEntryCell(cwd: Document, content: CheckboxEntryCellContent): HTMLTableCellElement {
        
        const label: string = content.isChecked === null ? '' : (content.isChecked ? '✔' : '✘');
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.classList.add('checkbox');
        elem.setAttribute('label', label);

        const readonly = this.#constructLabel(cwd, elem, label, 'plain');

        // When you start editing, immediately stop editing
        this.#startEditingAsync = async () => {
            await this.stopEditingAsync();
        };

        // When you stop editing, swap whether the cell is checked, update the database, and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            // Update the cell contents in the database
            await executeAsync({
                editCellContents: {
                    tableOid: content.dataTableOid,
                    columnOid: content.dataColumnOid,
                    rowOid: content.dataRowOid,
                    value: {
                        boolean: !content.isChecked
                    }
                }
            }).catch(async (e) => {
                await message(e, {
                    title: 'Unable to update cell contents.',
                    kind: 'error'
                });
            });
        };

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }



    /**
     * Construct a DIV that allows the user to tab between uploading a reference to a file and uploading the file to the database as a BLOB.
     * @param cwd 
     * @param content The content of a file/image entry cell.
     * @returns 
     */
    #constructFileInput(cwd: Document, content: FileEntryCellContent): { div: HTMLDivElement, getFileOidAsync: () => Promise<number | null> } {
        let fileOid: number | null = content.fileOid;

        // Set up the tabs
        const input: HTMLDivElement = cwd.createElement('div');
        input.style.display = 'grid';
        input.style.gridTemplateColumns = '1fr 1fr';
        

        // Set up tab for relative/absolute path
        const pathTab: HTMLLabelElement = cwd.createElement('label');
        pathTab.innerText = "Path";
        pathTab.classList.add('tab');
        input.appendChild(pathTab);
        const pathRadio: HTMLInputElement = cwd.createElement('input');
        pathRadio.type = 'radio';
        pathRadio.name = `${content.cellIdentifier}`;
        pathRadio.style.visibility = 'hidden';
        pathTab.appendChild(pathRadio);
        const pathTabContent: HTMLDivElement = cwd.createElement('div');
        pathTabContent.style.display = 'grid';
        pathTabContent.style.gridTemplateColumns = '1fr auto';
        input.appendChild(pathTabContent);

        // Text input for a path
        const pathInput: HTMLInputElement = cwd.createElement('input');
        pathInput.innerText = (content.label && !content.label.endsWith(')')) ? content.label : '';
        pathTabContent.appendChild(pathInput);
        
        // Button for selecting a path
        const pathUploadButton: HTMLImageElement = document.createElement('img');
        pathUploadButton.alt = 'Upload';
        pathUploadButton.src = './src-tauri/icons/upload.png';
        pathUploadButton.addEventListener('click', async () => {
            const filepath = await open({
                title: 'Reference File by Path'
            });
            if (filepath) {
                pathInput.value = filepath;
            }
        });
        pathTabContent.appendChild(pathUploadButton);


        // Set up tab for blob upload/download
        const fileTab: HTMLLabelElement = cwd.createElement('label');
        fileTab.innerText = "File";
        fileTab.classList.add('tab');
        input.appendChild(fileTab);
        const fileRadio: HTMLInputElement = cwd.createElement('input');
        fileRadio.type = 'radio';
        fileRadio.name = `${content.cellIdentifier}`;
        fileRadio.style.visibility = 'hidden';
        fileTab.appendChild(fileRadio);
        const fileTabContent: HTMLDivElement = cwd.createElement('div');
        fileTabContent.style.display = 'grid';
        fileTabContent.style.gridTemplateColumns = '1fr auto auto';
        input.appendChild(fileTabContent);

        // Label for filename
        const fileLabel: HTMLDivElement = document.createElement('div');
        fileLabel.innerText = content.label?.endsWith(')') ? content.label : '';
        fileTabContent.appendChild(fileLabel);

        // Button for directly uploading a file to the database
        const fileUploadButton: HTMLImageElement = document.createElement('img');
        fileUploadButton.alt = 'Upload';
        fileUploadButton.src = './src-tauri/icons/upload.png';
        fileUploadButton.addEventListener('click', async () => {
            const filepath = await open({
                title: 'Upload File to DungeonDB'
            });
            if (filepath) {
                let worker: Worker = new Worker('./workers/uploadFile.ts', { type: 'module' });
                worker.onmessage = async function (event) {
                    fileOid = event.data;
                    fileLabel.innerText = filepath;
                };
                worker.onerror = async function (event) {
                    await message(event.error, {
                        title: 'An error occurred while uploading file.',
                        kind: 'error'
                    });
                };
                worker.postMessage({
                    file: {
                        blob: {
                            oid: 0
                        }
                    },
                    uploadFromPath: filepath
                });
            }
        });
        input.appendChild(fileUploadButton);

        // Button for downloading the file that is stored or referenced
        const fileDownloadButton: HTMLImageElement = document.createElement('img');
        fileDownloadButton.classList.add('clickable-text');
        fileDownloadButton.alt = 'Download';
        fileDownloadButton.src = './src-tauri/icons/download.png';
        fileDownloadButton.addEventListener('click', async () => {
            const filepath = await save({
                title: 'Download File from DungeonDB'
            });
            if (filepath) {
                let worker: Worker = new Worker('./workers/downloadBlob.ts', { type: 'module' });
                worker.onmessage = async function () {
                    await message(`The file was successfully downloaded to "${filepath}".`, {
                        title: 'Download completed successfully.',
                        kind: 'info'
                    });
                };
                worker.onerror = async function (event) {
                    await message(event.error, {
                        title: 'An error occurred while downloading file.',
                        kind: 'error'
                    });
                }
                worker.postMessage({ 
                    fileOid: fileOid,
                    downloadToPath: filepath
                });
            }
        });
        input.appendChild(fileDownloadButton);


        // Choose which tab is selected by default, based on whether the label includes file size or not
        if (content.label?.endsWith(')')) {
            fileRadio.checked = true;
        } else {
            pathRadio.checked = true;
        }

        return {
            div: input,
            getFileOidAsync: async () => {
                if (pathRadio.checked && pathInput.value !== content.label) {
                    // If the file is a path and has been changed, upload the path to the database and get the resulting File OID
                    return await uploadFileAsync({
                        file: {
                            path: {
                                oid: 0,
                                path: pathInput.value
                            }
                        },
                        filepath: pathInput.value
                    });
                } else {
                    return fileOid;
                }
            }
        };
    }

    /**
     * Construct a cell that allows the user to select a file.
     * @param cwd 
     * @param content The content of a file entry cell.
     * @returns 
     */
    #constructFileEntryCell(cwd: Document, content: FileEntryCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        let { div: input, getFileOidAsync } = this.#constructFileInput(cwd, content);
        input.addEventListener('blur', async () => { await this.stopEditingAsync() });
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        const editingLock: string = JSON.stringify(this.cellIdentifier);
        let editing: boolean = false;

        this.#startEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (!editing) {
                    editing = true;

                    // Remove the file label, insert the input
                    elem.classList.add('editing');
                    elem.removeChild(readonly);
                    elem.appendChild(input);
                }
            });
        };
        this.#stopEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Get the File OID
                    const fileOid: number | null = await getFileOidAsync();

                    // Update the cell contents in the database
                    await executeAsync({
                        editCellContents: {
                            tableOid: content.dataTableOid,
                            columnOid: content.dataColumnOid,
                            rowOid: content.dataRowOid,
                            value: {
                                file: {
                                    fileOid: fileOid
                                }
                            }
                        }
                    }).catch(async (e) => {
                        await message(e, {
                            title: 'Unable to update cell contents.',
                            kind: 'error'
                        });
                    });

                    // Remove the input, restore the image
                    elem.removeChild(input);
                    elem.appendChild(readonly);

                    // Refresh the input area
                    ({ div: input, getFileOidAsync } = this.#constructFileInput(cwd, content));
                }
            });
        };

        // When you stop editing, discard changes
        this.#revertEditAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(readonly);
                }
            });
        };

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }

    /**
     * Construct a cell that allows the user to select a file, and attempts to display the file as an image.
     * @param cwd 
     * @param content The content of an image entry cell.
     * @returns 
     */
    #constructImageEntryCell(cwd: Document, content: ImageEntryCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const img: HTMLImageElement = cwd.createElement('img');
        if (content.file) {
            // Use worker to load image SRC from database
            // If the file is a path, this will return the path
            // If the file is a blob, this will return a string like "data:image/png;base64,..."
            const imageSrcWorker: Worker = new Worker('./workers/getImageSrc.ts', { type: 'module' });
            imageSrcWorker.onmessage = (event) => {
                img.src = event.data;
            };
            imageSrcWorker.postMessage(content.file);
        }
        img.alt = content.label || '';
        elem.appendChild(img);
        let { div: input, getFileOidAsync } = this.#constructFileInput(cwd, {
            cellIdentifier: content.cellIdentifier,
            dataTableOid: content.dataTableOid,
            dataColumnOid: content.dataColumnOid,
            dataRowOid: content.dataRowOid,
            fileOid: (content.file ? ('path' in content.file ? content.file.path.oid : content.file.blob.oid) : null),
            label: content.label,
            validationFailures: content.validationFailures
        });

        const editingLock: string = JSON.stringify(this.cellIdentifier);
        let editing: boolean = false;

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (!editing) {
                    editing = true;

                    // Remove the image, insert the input
                    elem.classList.add('editing');
                    elem.removeChild(img);
                    elem.appendChild(input);
                }
            });
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Get the File OID
                    const fileOid: number | null = await getFileOidAsync();

                    // Update the cell contents in the database
                    await executeAsync({
                        editCellContents: {
                            tableOid: content.dataTableOid,
                            columnOid: content.dataColumnOid,
                            rowOid: content.dataRowOid,
                            value: {
                                file: {
                                    fileOid: fileOid
                                }
                            }
                        }
                    }).catch(async (e) => {
                        await message(e, {
                            title: 'Unable to update cell contents.',
                            kind: 'error'
                        });
                    });

                    // Remove the input, restore the image
                    elem.classList.remove('editing');
                    elem.removeChild(input);
                    elem.appendChild(img);

                    // Refresh the input area
                    ({ div: input, getFileOidAsync } = this.#constructFileInput(cwd, {
                        cellIdentifier: content.cellIdentifier,
                        dataTableOid: content.dataTableOid,
                        dataColumnOid: content.dataColumnOid,
                        dataRowOid: content.dataRowOid,
                        fileOid: (content.file ? ('path' in content.file ? content.file.path.oid : content.file.blob.oid) : null),
                        label: content.label,
                        validationFailures: content.validationFailures
                    }));
                }
            });
        };

        // When you stop editing, discard changes
        this.#revertEditAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (editing) {
                    editing = false;

                    // Remove the input, restore the readonly text
                    elem.removeChild(input);
                    elem.appendChild(img);
                }
            });
        };

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }



    /**
     * Construct a single- or multi-select dropdown cell.
     * @param cwd 
     * @param content The content of the dropdown cell.
     * @returns 
     */
    #constructDropdownCell(cwd: Document, content: {
        cellIdentifier: CellIdentifier,
        dataTableOid: number,
        dataColumnOid: number,
        dataRowOid: number,
        dropdownTableOid: number,
        dropdownRowOid: number[],
        value: number | number[] | null,
        label: string | null,
        multiple: boolean
    }): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');

        const editingLock: string = JSON.stringify(this.cellIdentifier);
        let editing: boolean = false;

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            navigator.locks.request(editingLock, async () => {
                if (!editing) {
                    editing = true;

                    // Load the dropdown values
                    const input: HTMLDivElement = cwd.createElement('div');

                    // Set up the dropdown
                    const dropdown = jSuites.dropdown(input, {
                        format: 1,
                        autocomplete: true,
                        multiple: content.multiple,
                        lazyLoading: true,
                        opened: true
                    });

                    // Start receiving dropdown items
                    const processid: number = await getProcessidAsync();
                    const unlistenForTableRowLabels = await listen<{processid: number, dropdownValue: {id: number, name: string}}>('table_row_label', async (event) => {
                        console.debug(event.payload);
                        if (event.payload.processid === processid) {
                            dropdown.add(event.payload.dropdownValue.name, event.payload.dropdownValue.id);
                            if (event.payload.dropdownValue.id === content.value || (Array.isArray(content.value) && content.value.indexOf(event.payload.dropdownValue.id) >= 0)) {
                                dropdown.setValue((dropdown.getValue(true) as string[]).concat([event.payload.dropdownValue.id.toString()]));
                            }
                        }
                    });
                    console.debug(`Now listening for processid ${processid}`);
                    await queryAsync({
                        tableRowLabels: {
                            processid: processid, 
                            tableOid: content.dropdownTableOid
                        }
                    });

                    // Set up the stopEditingAsync callback
                    this.#stopEditingAsync = async () => {
                        navigator.locks.request(editingLock, async () => {
                            if (editing) {
                                editing = false;
                                unlistenForTableRowLabels();

                                const selectedRowOid: number[] = (dropdown.getValue(true) as string[])
                                    .map((idStr: string) => parseInt(idStr))
                                    .filter((id: number) => Number.isFinite(id));
                                if (!selectedRowOid.every(o => content.dropdownRowOid.indexOf(o) >= 0) || !content.dropdownRowOid.every(o => selectedRowOid.indexOf(o) >= 0)) {
                                    // There has been some sort of update to which rows are selected
                                    if (content.multiple) {
                                        await executeAsync({
                                            editCellContents: {
                                                tableOid: content.dataTableOid,
                                                columnOid: content.dataColumnOid,
                                                rowOid: content.dataRowOid,
                                                value: {
                                                    multiselect: {
                                                        linkedRowOid: selectedRowOid
                                                    }
                                                }
                                            }
                                        });
                                    } else {
                                        await executeAsync({
                                            editCellContents: {
                                                tableOid: content.dataTableOid,
                                                columnOid: content.dataColumnOid,
                                                rowOid: content.dataRowOid,
                                                value: {
                                                    select: {
                                                        linkedRowOid: selectedRowOid.length > 0 ? selectedRowOid[0] : null
                                                    }
                                                }
                                            }
                                        });
                                    }
                                }

                                // Swap the dropdown for the readonly label
                                elem.removeChild(input);
                                elem.appendChild(readonly);
                            }
                        });
                    };
                    
                    // When you stop editing, discard changes
                    this.#revertEditAsync = async () => {
                        navigator.locks.request(editingLock, async () => {
                            if (editing) {
                                editing = false;

                                // Remove the input, restore the readonly text
                                elem.removeChild(input);
                                elem.appendChild(readonly);
                            }
                        });
                    };

                    // Remove the readonly text
                    elem.removeChild(readonly);
                    elem.appendChild(input);
                    dropdown.open();
                    
                }
            });
        };

        return elem;
    }

    /**
     * Construct a single-select dropdown cell.
     * @param cwd 
     * @param content The content of the single-select dropdown cell.
     * @returns 
     */
    #constructSingleSelectDropdownCell(cwd: Document, content: SingleSelectDropdownCellContent): HTMLTableCellElement {
        return this.#constructDropdownCell(cwd, {
            cellIdentifier: content.cellIdentifier,
            dataTableOid: content.dataTableOid,
            dataColumnOid: content.dataColumnOid,
            dataRowOid: content.dataRowOid,
            dropdownTableOid: content.dropdownTableOid,
            dropdownRowOid: content.dropdownRowOid ? [content.dropdownRowOid] : [],
            value: content.dropdownRowOid,
            label: content.label,
            multiple: false
        });
    }

    /**
     * Construct a multi-select dropdown cell.
     * @param cwd 
     * @param content The content of the multi-select dropdown cell.
     * @returns 
     */
    #constructMultiSelectDropdownCell(cwd: Document, content: MultiSelectDropdownCellContent): HTMLTableCellElement {
        return this.#constructDropdownCell(cwd, {
            cellIdentifier: content.cellIdentifier,
            dataTableOid: content.dataTableOid,
            dataColumnOid: content.dataColumnOid,
            dataRowOid: content.dataRowOid,
            dropdownTableOid: content.dropdownTableOid,
            dropdownRowOid: content.dropdownRowOid,
            value: content.dropdownRowOid,
            label: content.label,
            multiple: true
        });
    }



    /**
     * Construct a cell that contains a readonly link to another schema.
     * @param cwd 
     * @param content The content of the link to another schema.
     * @returns 
     */
    #constructSchemaLinkCell(cwd: Document, content: SchemaLinkCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');
        
        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        readonly.classList.add('clickable');
        readonly.addEventListener('click', async () => {
            // Open schema using the provided query string
            await openDialogAsync({
                schema: {
                    title: content.label || '',
                    queryString: `schema_oid=${content.linkSchemaOid}&${content.linkQueryFilter}`
                }
            });
        });

        // Cell cannot be edited, so immediately stop editing
        this.#startEditingAsync = async () => {
            await this.stopEditingAsync();
        }

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }

    /**
     * Construct a cell that contains a readonly link to an object.
     * @param cwd 
     * @param content The content of the link to an object.
     * @returns 
     */
    #constructObjectLinkCell(cwd: Document, content: ObjectLinkCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');
        
        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        readonly.classList.add('clickable');
        readonly.addEventListener('click', async () => {
            // Open schema using the provided query string
            await openDialogAsync({
                object: {
                    title: content.label || '',
                    queryString: `schema_oid=${content.linkSchemaOid}&${content.linkQueryFilter}`
                }
            });
        });

        // Cell cannot be edited, so immediately stop editing
        this.#startEditingAsync = async () => {
            await this.stopEditingAsync();
        }

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }

    /**
     * Construct a cell with a readonly label.
     * @param cwd 
     * @param content The content of the readonly cell.
     * @returns 
     */
    #constructReadonlyCell(cwd: Document, content: ReadonlyCellContent): HTMLTableCellElement {
        
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.classList.add('readonly');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', content.format);

        // Cell cannot be edited, so immediately stop editing
        this.#startEditingAsync = async () => {
            await this.stopEditingAsync();
        };

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }
}
