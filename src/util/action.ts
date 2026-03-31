import { invoke } from "@tauri-apps/api/core";
import { FullMetadata as TableFullMetadata } from "./table";
import { FullMetadata as ReportFullMetadata } from "./report";
import { FullMetadata as ColumnFullMetadata } from "./column";
import { Cell } from "./cell";

export type Action = {
    createTable: TableFullMetadata
} | {
    editTable: TableFullMetadata
} | {
    createReport: ReportFullMetadata
} | {
    editReport: ReportFullMetadata
} | {
    trashSchema: number
} | {
    createColumn: ColumnFullMetadata
} | {
    editColumn: ColumnFullMetadata
} | {
    editColumnStyle: {
        metadata: ColumnFullMetadata,
        newColumnStyle: string
    }
} | {
    editColumnOrdering: {
        metadata: ColumnFullMetadata,
        newColumnOrdering: number | null
    }
} | {
    trashColumn: number
} | {
    createRow: {
        tableOid: number,
        rowOid: number | null,
        fixedParentDatasource: [number, number, ColumnFullMetadata] | null
    }
} | {
    editRowOid: {
        tableOid: number,
        rowOid: number,
        newRowOid: number | null
    }
} | {
    trashRow: {
        tableOid: number,
        rowOid: number
    }
} | {
    editRowSubtype: {
        tableOid: number,
        rowOid: number,
        inheritorTableOid: number
    }
} | {
    editCellContents: Cell
};

/**
 * Does an action with an impact on the state of the database.
 * @param action The action to perform.
 * @returns May return the OID of the object created. Usually returns void.
 */
export async function executeAsync(action: Action): Promise<void> {
    return await invoke('execute', { action: action });
}