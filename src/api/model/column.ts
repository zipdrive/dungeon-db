import { FullMetadata as SchemaFullMetadata } from "./schema";

export type Primitive = 
    'plainText' 
    | 'markdownText' 
    | 'jsonText' 
    | 'xmlText' 
    | 'integer' 
    | 'number' 
    | 'boolean' 
    | 'date' 
    | 'datetime' 
    | 'file' 
    | 'image'
;

export type ColumnType = {
    primitive: Primitive
} | {
    object: {
        oid: number,
        tableOid: number 
    }
} | {
    select: {
        oid: number,
        tableOid: number 
    }
} | {
    multiselect: {
        oid: number,
        tableOid: number 
    }
} | {
    formula: {
        oid: number,
        formula: string 
    }
} | {
    subreport: {
        oid: number,
        reportOid: number
    }
};

export type FullMetadata = {
    oid: number,
    schema: SchemaFullMetadata,
    name: string,
    columnType: ColumnType,
    style: string,
    ordering: number,
    defaultValue: string | null,
    isPrimaryKey: boolean
};
