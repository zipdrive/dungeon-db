import { File } from "./file"

export type ObjectTableCellContent = {
    tableOid: number,
    value: number | null,
    label: string | null
};
export type SingleSelectDropdownTableCellContent = {
    tableOid: number,
    value: number | null 
};
export type MultiSelectDropdownTableCellContent = {
    tableOid: number,
    value: number[]
};
export type SubreportTableCellContent = {
    reportOid: number,
    oidFilters: [string, number[]][],
    label: string | null
};

export type TableCellContent = {
    boolean: {
        value: boolean 
    }
} | {
    integer: {
        value: number | null 
    }
} | {
    number: {
        value: number | null 
    }
} | {
    date: {
        value: number | null,
        label: string | null 
    }
} | {
    datetime: {
        value: number | null,
        label: string | null 
    }
} | {
    text: {
        value: string | null,
        format: 'Plain' | 'Json' | 'Xml' | 'Markdown' | 'BBCode'
    }
} | {
    file: {
        value: File | null
    }
} | {
    object: ObjectTableCellContent
} | {
    singleSelectDropdown: SingleSelectDropdownTableCellContent
} | {
    multiSelectDropdown: MultiSelectDropdownTableCellContent
} | {
    subreport: SubreportTableCellContent
};

export type TableCell<Content extends TableCellContent = TableCellContent> = {
    tableOid: number,
    columnOid: number,
    rowOid: number,
    content: Content
};

export type TableRow = {
    oid: number,
    index: number,
    cells: TableCell[]
};


export type TableRowLabel = {
    oid: number,
    label: string
};