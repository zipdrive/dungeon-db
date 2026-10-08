export type TableListItem = {
    oid: number,
    name: string,
    disabled: boolean
};

export type TableMetadata = {
    oid: number,
    name: string,
    masterOids: number[]
}