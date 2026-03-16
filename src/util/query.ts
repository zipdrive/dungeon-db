import { Channel, invoke } from "@tauri-apps/api/core";
import { FullMetadata as TableFullMetadata } from "./table";
import { FullMetadata as ReportFullMetadata } from "./report";
import { FullMetadata as ColumnFullMetadata } from "./column";
import { Blob, Cell } from "./cell";
import { message } from "@tauri-apps/plugin-dialog";

export type FlatListItemMetadata = {
    oid: number,
    name: string
};
export type HierarchicalListItemMetadata = FlatListItemMetadata & {
    masterOid: number | null,
    level: number
};
export type ToggledHierarchicalListItemMetadata = HierarchicalListItemMetadata & { disabled: boolean };

export type DropdownValue = {
    value: number,
    label: string
};

export type Limit = {
    page: {
        num: number,
        size: number
    }
} | {
    singleRow: null
};

export type Query = {
    tables: {
        channel: Channel<HierarchicalListItemMetadata>
    }
} | {
    reports: {
        channel: Channel<HierarchicalListItemMetadata>
    }
} | {
    inheritorTables: {
        tableOid: number,
        channel: Channel<HierarchicalListItemMetadata>
    }
} | {
    masterSchemas: {
        schemaOid: number | null,
        isTable: boolean,
        channel: Channel<ToggledHierarchicalListItemMetadata>
    }
} | {
    columnReferences: {
        channel: Channel<DropdownValue>
    }
} | {
    columnValues: {
        schemaOid: number,
        channel: Channel<DropdownValue>
    }
} | {
    cells: {
        schemaOid: number,
        filters: [string, number][],
        limit: Limit,
        columnChannel: Channel<ColumnFullMetadata>,
        cellChannel: Channel<Cell & { maxIndex: number }>
    }
};

export async function queryAsync(query: Query): Promise<void> {
    await invoke('query', { query: query })
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while querying database.',
                kind: 'error'
            })
        });
}

export async function getTableMetadataAsync(oid: number): Promise<TableFullMetadata> {
    return await invoke('get_table_metadata', { tableOid: oid });
}

export async function getReportMetadataAsync(oid: number): Promise<ReportFullMetadata> {
    return await invoke('get_report_metadata', { reportOid: oid });
}

export async function getColumnAsync(oid: number): Promise<ColumnFullMetadata> {
    return await invoke('get_column', { columnOid: oid });
}

export async function getBlobBase64Async(blob: Blob): Promise<string> {
    return await invoke('get_blob', { blob: blob });
}

export async function downloadBlobAsync(blob: Blob, filepath: string): Promise<void> {
    await invoke('download_blob', { blob: blob, filepath: filepath });
}