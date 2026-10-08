export type Primitive = 
    'text' 
    | 'textMarkdown' 
    | 'textBBCode'
    | 'textJson' 
    | 'textXml' 
    | 'integer' 
    | 'number' 
    | 'boolean' 
    | 'date' 
    | 'datetime' 
;



export type TableColumnType = {
    primitive: {
        oid: number,
        primitive: Primitive,
        defaultValue: string | null
    }
} | {
    file: {
        oid: number
    }
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
    subreport: {
        oid: number,
        reportOid: number
    }
};

export type TableColumnBaseType = Primitive
    | 'file'
    | 'object'
    | 'select'
    | 'multiselect'
    | 'subreport';



export type TableColumnMetadata = {
    oid: number,
    name: string,
    columnType: TableColumnType,
    size: number,
    style: string,
    isPrimaryKey: boolean
};
