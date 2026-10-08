export type ReportListItem = {
    oid: number,
    name: string
};

export type ReportMetadata = {
    oid: number,
    name: string,
    filterFormula: string | null,
    groupByColumnOids: number[],
    orderByColumnOids: number[]
}