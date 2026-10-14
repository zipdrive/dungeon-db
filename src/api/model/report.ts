export type ReportListItem = {
    oid: number,
    name: string
};

export type ReportMetadata = {
    oid: number,
    name: string,

    // The baseline table.
    baseline: {
        // The formula for the baseline table. Should return a record.
        formula: string,

        // The OID of the baseline table.
        tableOid: number,

        // The name of the baseline table's OID in the oidFilters of a TableRow.
        oidName: string,

        // The name of the baseline table's parent table OID in the oidFilters that are passed into a Report page.
        parentOidName: string | null
    } | null,

    // A formula used to filter rows of the report.
    filterFormula: string | null,

    // The column OIDs used to group the report.
    groupByColumnOids: number[],

    // The first item is the column OID used to order the report.
    // The second item is true if the column is sorted in ascending order, and false if sorted in descending order.
    orderByColumnOids: [number, boolean][]
}