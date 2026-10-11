export type TablePageBreadcrumb = {
    key: 'table',
    name: string,
    tableOid: number 
};

export type ReportPageBreadcrumb = {
    key: 'report',
    name: string,
    reportOid: number,
    oidFilters: [string, number[]][]
};

export type ObjectPageBreadcrumb = {
    key: 'object',
    name: string,
    tableOid: number,
    rowOid: number
};

export type PageBreadcrumb = TablePageBreadcrumb | ReportPageBreadcrumb | ObjectPageBreadcrumb;