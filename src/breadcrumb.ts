export type TablePageBreadcrumb = {
    page: 'table',
    name: string,
    tableOid: number 
};

export type ReportPageBreadcrumb = {
    page: 'report',
    name: string,
    reportOid: number,
    oidFilters: [string, number[]][]
};

export type ObjectPageBreadcrumb = {
    page: 'object',
    name: string,
    tableOid: number,
    rowOid: number
};

export type PageBreadcrumb = TablePageBreadcrumb | ReportPageBreadcrumb | ObjectPageBreadcrumb;