import { FullMetadata as TableFullMetadata } from "./table";
import { FullMetadata as ReportFullMetadata } from "./report";

export type Schema = {
    table: TableFullMetadata
} | {
    report: ReportFullMetadata
};

export type FullMetadata = {
    oid: number,
    name: string,
    masterSchemas: Schema[]
};