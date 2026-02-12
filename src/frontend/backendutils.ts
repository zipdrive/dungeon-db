import { invoke, Channel } from "@tauri-apps/api/core";
import { message } from "@tauri-apps/plugin-dialog";

export type BasicMetadata = {
    oid: number,
    name: string 
};
export type BasicHierarchicalMetadata = BasicMetadata & {
    hierarchyLevel: number
};
export type ToggledHierarchicalMetadata = BasicHierarchicalMetadata & {
    isDisabled: boolean
};

export type ColumnType = { primitive: 'Any' | 'Boolean' | 'Integer' | 'Number' | 'Date' | 'Timestamp' | 'Text' | 'JSON' | 'File' | 'Image' } 
    | { singleSelectDropdown: number }
    | { multiSelectDropdown: number }
    | { reference: number } 
    | { childObject: number } 
    | { childTable: number };

export type TableColumnMetadata = {
    oid: number, 
    name: string,
    columnOrdering: number,
    columnStyle: string,
    columnType: ColumnType,
    isNullable: boolean,
    isUnique: boolean,
    isPrimaryKey: boolean,
};

export type DropdownValue = {
    trueValue: string | null,
    displayValue: string | null
};

export type TableColumnCell = {
    tableOid: number,
    rowOid: number,
    columnOid: number, 
    columnName: string,
    columnType: ColumnType, 
    trueValue: string | null,
    displayValue: string | null,
    failedValidations: { description: string }[]
};

export type TableCellChannelPacket = {
    rowOid: number,
    rowIndex: number
} | TableColumnCell;

export type TableRowCellChannelPacket = {
    rowExists: boolean,
    tableOid: number
} | (TableColumnCell & { columnOrdering: number });


export type Query = {
    invokeAction: 'get_table_list',
    invokeParams: {
        tableChannel: Channel<BasicMetadata>
    }
} | {
    invokeAction: 'get_table_metadata',
    invokeParams: {
        tableOid: number
    }
} | {
    invokeAction: 'get_report_list',
    invokeParams: {
        reportChannel: Channel<BasicMetadata>
    }
} | {
    invokeAction: 'get_object_type_list',
    invokeParams: {
        objectTypeChannel: Channel<BasicHierarchicalMetadata>
    }
} | {
    invokeAction: 'get_master_list_option_dropdown_values',
    invokeParams: {
        tableOid: number | null,
        allowInheritanceFromTables: boolean,
        optionChannel: Channel<ToggledHierarchicalMetadata>
    }
} | {
    invokeAction: 'get_subtype_list',
    invokeParams: {
        tableOid: number,
        objectTypeChannel: Channel<BasicHierarchicalMetadata>
    }
} | {
    invokeAction: 'get_table_column',
    invokeParams: {
        columnOid: number
    }
} | {
    invokeAction: 'get_table_column_dropdown_values',
    invokeParams: {
        columnOid: number,
        dropdownValueChannel: Channel<DropdownValue>
    }
} | {
    invokeAction: 'get_table_column_list',
    invokeParams: {
        tableOid: number,
        columnChannel: Channel<TableColumnMetadata>
    }
} | {
    invokeAction: 'get_table_column_reference_values',
    invokeParams: {
        referenceTypeChannel: Channel<BasicMetadata>
    }
} | {
    invokeAction: 'get_table_column_object_values',
    invokeParams: {
        objectTypeChannel: Channel<BasicMetadata>
    }
} | {
    invokeAction: 'get_table_data',
    invokeParams: {
        tableOid: number,
        parentRowOid: number | null,
        pageNum: number,
        pageSize: number,
        cellChannel: Channel<TableCellChannelPacket>
    }
} | {
    invokeAction: 'get_table_row',
    invokeParams: {
        tableOid: number,
        rowOid: number,
        cellChannel: Channel<TableRowCellChannelPacket>
    }
} | {
    invokeAction: 'get_object_data',
    invokeParams: {
        objTypeOid: number,
        objRowOid: number,
        objDataChannel: Channel<TableRowCellChannelPacket>
    }
} | {
    invokeAction: 'get_blob_value',
    invokeParams: {
        tableOid: number,
        rowOid: number,
        columnOid: number
    }
} | {
    invokeAction: 'download_blob_value',
    invokeParams: {
        tableOid: number,
        rowOid: number,
        columnOid: number,
        filePath: string
    }
};

export type Dialog = {
    invokeAction: 'dialog_create_table',
    invokeParams: {}
} | {
    invokeAction: 'dialog_edit_table',
    invokeParams: {
        tableOid: number
    }
} | {
    invokeAction: 'dialog_create_object_type',
    invokeParams: {}
} | {
    invokeAction: 'dialog_edit_object_type',
    invokeParams: {
        objTypeOid: number
    }
} | {
    invokeAction: 'dialog_create_table_column',
    invokeParams: {
        tableOid: number,
        columnOrdering: number | null
    }
} | {
    invokeAction: 'dialog_edit_table_column',
    invokeParams: {
        tableOid: number,
        columnOid: number
    }
} | {
    invokeAction: 'dialog_table_data',
    invokeParams: {
        tableOid: number,
        tableName: string
    }
} | {
    invokeAction: 'dialog_child_table_data',
    invokeParams: {
        tableOid: number,
        parentRowOid: number,
        tableName: string
    }
} | {
    invokeAction: 'dialog_object_data',
    invokeParams: {
        tableOid: number,
        rowOid: number,
        title: string
    }
} | {
    invokeAction: 'dialog_close',
    invokeParams: {}
};

export type Action = {
    createTable: {
        tableName: string,
        masterTableOidList: number[]
    }
} | {
    editTableMetadata: {
        tableOid: number,
        tableName: string,
        masterTableOidList: number[]
    }
} | {
    deleteTable: {
        tableOid: number
    }
} | {
    createObjectType: {
        objTypeName: string,
        masterTableOidList: number[]
    }
} | {
    editObjectTypeMetadata: {
        objTypeOid: number,
        objTypeName: string,
        masterTableOidList: number[]
    }
} | {
    deleteObjectType: {
        objTypeOid: number
    }
} | {
    createTableColumn: {
        tableOid: number,
        columnOrdering: number | null,
        columnName: string,
        columnType: ColumnType,
        columnStyle: string,
        isNullable: boolean,
        isUnique: boolean,
        isPrimaryKey: boolean
    }
} | {
    editTableColumnMetadata: {
        tableOid: number,
        columnOid: number,
        columnName: string,
        columnType: ColumnType,
        columnStyle: string,
        isNullable: boolean,
        isUnique: boolean,
        isPrimaryKey: boolean
    }
} | {
    editTableColumnWidth: {
        tableOid: number,
        columnOid: number,
        columnWidth: number
    }
} | {
    editTableColumnDropdownValues: {
        tableOid: number,
        columnOid: number,
        dropdownValues: DropdownValue[]
    }
} | {
    reorderTableColumn: {
        tableOid: number,
        columnOid: number,
        oldColumnOrdering: number,
        newColumnOrdering: number | null
    }
} | {
    deleteTableColumn: {
        tableOid: number,
        columnOid: number
    }
} | {
    pushTableRow: {
        tableOid: number,
        parentRowOid: number | null 
    }
} | {
    insertTableRow: {
        tableOid: number,
        parentRowOid: number | null,
        rowOid: number
    }
} | {
    retypeTableRow: {
        baseTypeOid: number,
        baseRowOid: number,
        newSubtypeOid: number
    }
} | {
    deleteTableRow: {
        tableOid: number,
        rowOid: number
    }
} | {
    updateTableCellStoredAsPrimitiveValue: {
        tableOid: number,
        rowOid: number,
        columnOid: number,
        value: string | null
    }
} | {
    updateTableCellStoredAsBlob: {
        tableOid: number,
        rowOid: number,
        columnOid: number,
        filePath: string
    }
} | {
    setTableObjectCell: {
        tableOid: number,
        rowOid: number,
        columnOid: number,
        objTypeOid: number | null,
        objRowOid: number | null
    }
};



/**
 * Runs a query and returns the result or passes the result through one or more channels.
 * @param query The query to run.
 * @returns The result of the query, if singular. Otherwise, returns void.
 */
export async function queryAsync(query: Query): Promise<any> {
    return await invoke(query.invokeAction, query.invokeParams)
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while reading database.",
            kind: 'error'
        });
    });
}

/**
 * Opens a dialog window.
 * @param dialog The dialog window to open.
 */
export async function openDialogAsync(dialog: Dialog): Promise<void> {
    await invoke(dialog.invokeAction, dialog.invokeParams)
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while opening dialog box.",
            kind: 'error'
        });
    });
}

/**
 * Closes the current dialog window.
 */
export async function closeDialogAsync(): Promise<void> {
    await invoke('dialog_close', {})
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while closing dialog box.",
            kind: 'error'
        });
    });
}



/**
 * Does an action with an impact on the state of the database.
 * @param action The action to perform.
 * @returns May return the OID of the object created. Usually returns void.
 */
export async function executeAsync(action: Action): Promise<void> {
    return await invoke('execute', { action: action });
}

/**
 * Undoes the last action performed.
 */
export async function undoAsync() {
    await invoke('undo', {});
}

/**
 * Redoes the last action that was undone.
 */
export async function redoAsync() {
    await invoke('redo', {});
}