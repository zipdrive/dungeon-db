import { Channel, invoke } from "@tauri-apps/api/core";
import { FullMetadata as TableFullMetadata } from "./table";
import { FullMetadata as ReportFullMetadata } from "./report";
import { FullMetadata as ColumnFullMetadata } from "./column";
import { Cell, ValueOid, File, CellOid } from "./cell";
import { message } from "@tauri-apps/plugin-dialog";
import { Datasource } from "./datasource";
import { Schema } from "./schema";

export type FlatListItemMetadata = {
    oid: number,
    name: string
};
export type HierarchicalListItemMetadata = FlatListItemMetadata & {
    masterOid: number | null,
    level: number
};
export type SelectedHierarchicalListItemMetadata = HierarchicalListItemMetadata & { selected: boolean };
export type ToggledHierarchicalListItemMetadata = HierarchicalListItemMetadata & { disabled: boolean };

export type DropdownValue = {
    value: number,
    label: string
};

export type DatasourceDropdownValue = {
    value: Datasource,
    label: string 
};
export type ParameterDropdownValue = {
    value: string,
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
        rowOid: number,
        channel: Channel<SelectedHierarchicalListItemMetadata>
    }
} | {
    masterSchemas: {
        schemaOid: number | null,
        isTable: boolean,
        channel: Channel<ToggledHierarchicalListItemMetadata>
    }
} | {
    columns: {
        schemaOid: number,
        channel: Channel<ColumnFullMetadata>
    }
} | {
    rootDatasources: {
        channel: Channel<DatasourceDropdownValue>
    }
} | {
    linkedDatasources: {
        parentDatasource: Datasource,
        channel: Channel<DatasourceDropdownValue>
    }
} | {
    parameters: {
        parentDatasource: Datasource,
        channel: Channel<ParameterDropdownValue>
    }
} | {
    columnAssociatedTables: {
        channel: Channel<DropdownValue>
    }
} | {
    columnAssociatedReports: {
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
            await message(`Query: ${JSON.stringify(query)}\n\n${e}`, {
                title: `An error occurred while querying database.`,
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

export async function getSchemaMetadataAsync(oid: number): Promise<Schema> {
    return await invoke('get_schema_metadata', { schemaOid: oid });
}

export async function getColumnAsync(oid: number): Promise<ColumnFullMetadata> {
    return await invoke('get_column', { columnOid: oid });
}

export async function getCellAsync(cellOid: CellOid): Promise<Cell> {
    return await invoke('get_cell', { cellOid: cellOid });
}

export async function getFileBase64Async(data: { fileOid: number }): Promise<string> {
    return await invoke('get_file_base64', data);
}

export async function downloadFileAsync(data: { fileOid: number, filepath: string }): Promise<void> {
    await invoke('download_file', data);
}

export async function uploadFileAsync(data: { file: File, filepath: string }): Promise<number> {
    return await invoke('upload_file', data);
}