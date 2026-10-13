import { invoke } from "@tauri-apps/api/core";

export type ExportJsonSchema = {
    table: {
        tableOid: number,
        tableName: string,
        oidColumn: string | null,
        indexColumn: string | null,
        polymorphism: 'yes' | 'no'
    }
} | {
    report: {
        reportOid: number,
        reportName: string,
        indexColumn: string | null 
    }
};

/**
 * Exports tables and reports in JSON format.
 */
export async function exportJsonAsync(schemas: ExportJsonSchema[], typeColumn: string | null) {
    return await invoke('export_json', { schemas, typeColumn });
}