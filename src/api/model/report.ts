import { FullMetadata as SchemaFullMetadata } from "./schema";

export type FullMetadata = {
    schema: SchemaFullMetadata,
    filterFormula: string | null,
    groupByColumnOids: number[]
}