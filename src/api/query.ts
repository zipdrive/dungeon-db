import { Channel, invoke } from "@tauri-apps/api/core";
import { TableListItem, TableMetadata } from "./model/table";
import { ReportListItem, ReportMetadata } from "./model/report";
import { TableColumnMetadata } from "./model/tableColumn";
import { message } from "@tauri-apps/plugin-dialog";
import { ReportColumnMetadata } from "./model/reportColumn";
import { TableRow, TableRowLabel } from "./model/tableRow";
import { ReportRow } from "./model/reportRow";

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
        channel: Channel<TableListItem>
    }
} | {
    reports: {
        channel: Channel<ReportListItem>
    }
} | {
    tableMasters: {
        tableOid: number | null,
        channel: Channel<TableListItem>
    }
} | {
    tableColumns: {
        tableOid: number,
        channel: Channel<TableColumnMetadata>
    }
} | {
    reportColumns: {
        reportOid: number,
        channel: Channel<ReportColumnMetadata>
    }
} | {
    tableCells: {
        tableOid: number,
        columnChannel: Channel<TableColumnMetadata>,
        rowChannel: Channel<TableRow>
    }
} | {
    reportCells: {
        reportOid: number,
        columnChannel: Channel<ReportColumnMetadata>,
        rowChannel: Channel<ReportRow>
    }
} | {
    tableRowLabels: {
        tableOid: number,
        channel: Channel<TableRowLabel>
    }
};

export async function queryAsync(query: Query): Promise<void> {
    await invoke('query', { query: query });
}

export async function getTableMetadataAsync(oid: number): Promise<TableMetadata> {
    return await invoke('get_table_metadata', { tableOid: oid });
}

export async function getReportMetadataAsync(oid: number): Promise<ReportMetadata> {
    return await invoke('get_report_metadata', { reportOid: oid });
}

export async function getTableColumnMetadataAsync(oid: number): Promise<TableColumnMetadata> {
    return await invoke('get_table_column_metadata', { columnOid: oid });
}

export async function getObjectLabelAsync(tableOid: number, rowOid: number): Promise<string> {
    return await invoke('get_object_label', { tableOid, rowOid });
}

export async function getTableRow(tableOid: number, rowOid: number): Promise<TableRow> {
    return await invoke('get_row', { tableOid, rowOid });
}

export async function getObjectRow(tableOid: number, rowOid: number): Promise<TableRow> {
    return await invoke('get_object_row', { tableOid, rowOid });
}

export async function getSrcAsync(data: { file: File }): Promise<string> {
    return await invoke('get_src', data);
}

export async function downloadFileAsync(data: { fileOid: number, downloadToPath: string }): Promise<void> {
    await invoke('download_file', data);
}

export async function uploadFileAsync(data: { file: File, uploadFromPath: string }): Promise<number> {
    return await invoke('upload_file', data);
}