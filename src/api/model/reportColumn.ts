type ReportColumnType = {
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

export type ReportColumnMetadata = {
    oid: number,
    name: string,
    columnType: ReportColumnType,
    size: number,
    style: string,
    isPrimaryKey: boolean
};
