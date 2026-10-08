export type File = {
    path: {
        oid: number,
        name: string,
        path: string 
    }
} | {
    blob: {
        oid: number,
        name: string,
        size: number
    }
};