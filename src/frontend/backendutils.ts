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



export type ReportVirtualParameter = {
    column: {
        columnOid: number,
        sourceName: string,
        columnName: string,
        linkedTableOid: number,
        isManyToOne: boolean
    }
} | {
    masterList: {
        masterTableOid: number,
        masterTableName: string
    }
} | {
    reference: {
        columnOid: number,
        sourceName: string,
        columnName: string,
        linkedTableOid: number
    }
} | {
    inheritance: {
        inheritorTableOid: number,
        inheritorTableName: string
    }
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


export type ReportColumnMetadata = {
    formula: {
        oid: number, 
        name: string,
        columnOrdering: number,
        columnStyle: string,
        formula: string
    }
} | {
    subreport: {
        oid: number, 
        name: string,
        columnOrdering: number,
        columnStyle: string,
        subreportOid: number,
        subreportBaseParameterOid: number
    }
};

export type ReportCellChannelPacket = {
    rowStart: {
        rowOid: number,
        rowIndex: number
    }
} | {
    columnValue: TableColumnCell
} | {
    readOnlyValue: {
        displayValue: string | null,
        failedValidations: { description: string }[]
    }
} | {
    subreport: {
        subreportOid: number
    }
};

export type ReportRowCellChannelPacket = {
    rowExists: boolean
} | {
    columnValue: TableColumnCell
} | {
    readOnlyValue: {
        displayValue: string | null,
        failedValidations: { description: string }[]
    }
} | {
    subreport: {
        subreportOid: number
    }
};


export type Query = {
    invokeAction: 'get_table_metadata',
    invokeParams: {
        tableOid: number
    }
} | {
    invokeAction: 'get_report_metadata',
    invokeParams: {
        reportOid: number
    }
} | {
    invokeAction: 'get_table_column',
    invokeParams: {
        columnOid: number
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



export type QueryStream = {
    tables: {
        channel: Channel<BasicMetadata>
    }
}
| {
    reports: {
        channel: Channel<BasicMetadata>
    }
}
| {
    objectTypes: {
        channel: Channel<BasicHierarchicalMetadata>
    }
} 
| {
    masterLists: {
        tableOid: number | null,
        allowInheritanceFromTables: boolean,
        channel: Channel<ToggledHierarchicalMetadata>
    }
}
| {
    referenceColumnTypes: {
        channel: Channel<BasicMetadata>
    }
}
| {
    objectColumnTypes: {
        channel: Channel<BasicMetadata>
    }
}
| {
    objectSubtypes: {
        tableOid: number,
        channel: Channel<BasicHierarchicalMetadata>
    }
} 
| {
    reportParameters: {
        baseTableOid: number,
        channel: Channel<ReportVirtualParameter>
    }
}
| {
    tableColumns: {
        tableOid: number,
        channel: Channel<TableColumnMetadata>
    }
}
| {
    tableColumnDropdownValues: {
        columnOid: number,
        channel: Channel<DropdownValue>
    }
}
| {
    reportColumns: {
        reportOid: number,
        channel: Channel<ReportColumnMetadata>
    }
}
| {
    tablePageCells: {
        tableOid: number,
        parentRowOid: number | null,
        pageNum: number,
        pageSize: number,
        channel: Channel<TableCellChannelPacket>
    }
}
| {
    tableRowCells: {
        tableOid: number,
        rowOid: number,
        channel: Channel<TableRowCellChannelPacket>
    }
}
| {
    tableObjectCells: {
        tableOid: number,
        rowOid: number,
        channel: Channel<TableRowCellChannelPacket>
    }
}
| {
    reportPageCells: {
        reportOid: number,
        parentRowOid: number | null,
        pageNum: number,
        pageSize: number,
        channel: Channel<ReportCellChannelPacket>
    }
}
| {
    reportRowCells: {
        reportOid: number,
        baseTableRowOid: number,
        channel: Channel<ReportRowCellChannelPacket>
    }
};

/**
 * Receives data through a channel from the backend.
 * @param queryStream 
 */
export async function queryStreamAsync(queryStream: QueryStream): Promise<void> {
    await invoke('query', {
        query: queryStream
    })
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while reading database.",
            kind: 'error'
        });
    });
}



export type Dialog = {
    createTable: null
} | {
    editTableMetadata: {
        tableOid: number
    }
} | {
    createReport: null
} | {
    editReportMetadata: {
        reportOid: number
    }
} | {
    createObjectType: null
} | {
    editObjectTypeMetadata: {
        objTypeOid: number
    }
} | {
    createTableColumn: {
        tableOid: number,
        columnOrdering: number | null
    }
} | {
    editTableColumnMetadata: {
        tableOid: number,
        columnOid: number
    }
} | {
    createReportColumn: {
        reportOid: number,
        columnOrdering: number | null
    }
} | {
    editReportColumnMetadata: {
        reportOid: number,
        columnOid: number
    }
} | {
    table: {
        tableOid: number,
        tableName: string
    }
} | {
    childTable: {
        tableOid: number,
        parentRowOid: number,
        tableName: string
    }
} | {
    object: {
        tableOid: number,
        rowOid: number,
        objectName: string
    }
} | {
    report: {
        reportOid: number,
        reportName: string
    }
};

/**
 * Opens a dialog window.
 * @param dialog The dialog window to open.
 */
export async function openDialogAsync(dialog: Dialog): Promise<void> {
    await invoke('dialog_open', { dialog: dialog })
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
    createReport: {
        reportName: string,
        baseTableOid: number
    }
} | {
    editReportMetadata: {
        reportOid: number,
        reportName: string
    }
} | {
    deleteReport: {
        reportOid: number
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
        isPrimaryKey: boolean,
        dropdownValues: DropdownValue[] | null
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
        isPrimaryKey: boolean,
        dropdownValues: DropdownValue[] | null
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
    createReportFormulaColumn: {
        reportOid: number,
        columnOrdering: number | null,
        columnName: string,
        columnStyle: string,
        formula: string
    }
} | {
    createReportSubreportColumn: {
        reportOid: number,
        columnOrdering: number | null,
        columnName: string,
        columnStyle: string,
        baseParameterOid: number
    }
} | {
    editReportFormulaColumnMetadata: {
        report_oid: number,
        column_oid: number,
        column_name: string,
        column_style: string,
        formula: string
    }
} | {
    editReportSubreportColumnMetadata: {
        report_oid: number,
        column_oid: number,
        column_name: string,
        column_style: string,
    }
} | {
    editReportColumnWidth: {
        report_oid: number,
        column_oid: number,
        column_width: number,
    }
} | {
    reorderReportColumn: {
        report_oid: number,
        column_oid: number,
        old_column_ordering: number,
        new_column_ordering: number | null
    }
} | {
    deleteReportColumn: {
        reportOid: number,
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
    updateTableCellStoredAsMultiselectValue: {
        tableOid: number,
        rowOid: number,
        columnOid: number,
        columnTypeOid: number,
        valueOidList: number[]
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
 * Does an action with an impact on the state of the database.
 * @param action The action to perform.
 * @returns May return the OID of the object created. Usually returns void.
 */
export async function executeAsync(action: Action): Promise<void> {
    return await invoke('execute', { action: action });
}
