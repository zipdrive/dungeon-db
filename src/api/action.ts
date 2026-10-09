import { invoke } from "@tauri-apps/api/core";
import { TableMetadata } from "./model/table";
import { ReportMetadata } from "./model/report";
import { TableColumnMetadata } from "./model/tableColumn";
import { TableCell } from "./model/tableRow";
import { ReportColumnMetadata } from "./model/reportColumn";

export type Action = {
    createTable: TableMetadata
} | {
    editTable: TableMetadata
} | {
    trashTable: number
} | {
    createReport: ReportMetadata
} | {
    editReport: ReportMetadata
} | {
    trashReport: number
} | {
    createTableColumn: {
        tableOid: number,
        metadata: TableColumnMetadata,
        ordering: number | null
    }
} | {
    replaceTableColumn: {
        oldMetadata: TableColumnMetadata,
        newMetadata: TableColumnMetadata
    }
} | {
    editTableColumnMetadata: {
        metadata: TableColumnMetadata
    }
} | {
    editTableColumnOrdering: {
        metadata: TableColumnMetadata,
        ordering: number | null
    }
} | {
    trashTableColumn: {
        columnOid: number
    }
} | {
    createReportColumn: {
        tableOid: number,
        metadata: ReportColumnMetadata,
        ordering: number | null
    }
} | {
    replaceReportColumn: {
        oldMetadata: ReportColumnMetadata,
        newMetadata: ReportColumnMetadata
    }
} | {
    editReportColumnMetadata: {
        metadata: ReportColumnMetadata
    }
} | {
    editReportColumnOrdering: {
        metadata: ReportColumnMetadata,
        ordering: number | null
    }
} | {
    trashReportColumn: {
        reportOid: number,
        columnOid: number
    }
} | {
    createTableRow: {
        tableOid: number,
        rowOid: number | null,
        fixedParentDatasource: [number, number, TableColumnMetadata] | null
    }
} | {
    editTableRowOid: {
        tableOid: number,
        rowOid: number,
        newRowOid: number | null
    }
} | {
    trashTableRow: {
        tableOid: number,
        rowOid: number
    }
} | {
    editTableRowSubtype: {
        tableOid: number,
        rowOid: number,
        inheritorTableOid: number
    }
} | {
    editTableCellContents: TableCell
} | {
    createObject: {
        tableOid: number,
        columnOid: number,
        rowOid: number,
        objectTableOid: number
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