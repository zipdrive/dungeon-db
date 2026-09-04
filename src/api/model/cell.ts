import { FullMetadata as ColumnFullMetadata } from "./column";


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

export type CellDependency = {
    tableOid: number,
    columnOid: number,
    rowOid: number | null
};

export type CellIdentifier = {
    tableOid: number,
    columnOid: number,
    rowOid: number
} | {
    columnOid: number,
    oidFilters: [string, number][]
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
    tableRowIdentifier: {
        tableOid: number,
        rowOid: number
    } | null,
    oidFilters: [string, number][],
    index: number,
    validationFailures: ValidationFailures
};
export type AddNewRowButton = {
    tableOid: number,
    fixedParentDatasource: [number, number, ColumnFullMetadata] | null,
    columnSpan: number
};


type CellContentTextFormat = 'plain' | 'jSON';

export type ReadonlyCellContent = {
    cellIdentifier: CellIdentifier,
    label: string | null,
    format: CellContentTextFormat,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures
};
export type TextEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    format: CellContentTextFormat,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures 
};
export type IntegerEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    value: number | null,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures 
};
export type NumberEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    value: number | null,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures 
};
export type DateEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures 
};
export type DatetimeEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures 
};
export type CheckboxEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    isChecked: boolean | null,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures 
};
export type FileEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    fileOid: number | null,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures
};
export type ImageEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    file: File | null,
    label: string | null,
    fileSrc: string | null,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures
};
export type SchemaLinkCellContent = {
    cellIdentifier: CellIdentifier,
    label: string | null,
    linkSchemaOid: number,
    linkOidFilters: [string, number][],
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures
};
export type ObjectLinkCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    linkSchemaOid: number,
    linkRowOid: number | null,
    linkQueryFilter: string | null,
    clipboardData: [number, DataCellEntry[]] | null,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures
};
export type SingleSelectDropdownCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    dropdownTableOid: number,
    dropdownRowOid: number | null,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
    validationFailures: ValidationFailures
};
export type MultiSelectDropdownCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    dropdownTableOid: number,
    dropdownRowOid: number[],
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[],
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
