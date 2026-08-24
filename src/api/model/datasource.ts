import { FullMetadata as ColumnFullMetadata } from "./column"

export type Datasource = {
    table: {
        oid: number,
        tableOid: number
    }
} | {
    masterTable: {
        parentDatasource: Datasource,
        tableOid: number 
    }
} | {
    inheritorTable: {
        parentDatasource: Datasource,
        tableOid: number
    }
} | {
    column: {
        parentDatasource: Datasource,
        column: ColumnFullMetadata
    }
};