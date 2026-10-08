import { File } from "./file"

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
    object: {
        tableOid: number,
        value: number | null 
    }
} | {
    singleSelectDropdown: {
        tableOid: number,
        value: number | null 
    }
} | {
    multiSelectDropdown: {
        tableOid: number,
        value: number[]
    }
} | {
    subreport: {
        reportOid: number,
        oidFilters: [string, number[]][]
    }
};

export type TableCell = {
    tableOid: number,
    columnOid: number,
    rowOid: number,
    content: TableCellContent
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