import { TableCellContent } from "./tableRow"

export type TableCellReference = {
    tableOid: number,
    columnOid: number,
    rowOid: number
};

export type ReportCell = {
    columnOid: number,
    reference: TableCellReference | null,
    content: TableCellContent
}

export type ReportRow = {
    oidFilters: [string, number[]][],
    index: number,
    cells: ReportCell[]
}