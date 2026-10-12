import { TableCellContent } from "./tableRow"

export type TableCellReference = {
    tableOid: number,
    columnOid: number,
    rowOid: number
};

export type ReportCell<Content extends TableCellContent = TableCellContent> = {
    columnOid: number,
    reference: TableCellReference | null,
    content: Content
};

export type ReportRow = {
    oidFilters: [string, number[]][],
    index: number,
    cells: ReportCell[]
};