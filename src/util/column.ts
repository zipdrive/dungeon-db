import { FullMetadata as SchemaFullMetadata } from "./schema";

export type Primitive = 'text' | 'integer' | 'number' | 'checkbox' | 'date' | 'datetime' | 'file' | 'image' | 'jSON';

export type ColumnType = {
    primitive: Primitive
};

export type FullMetadata = {
    oid: number,
    hidden: boolean,
    schema: SchemaFullMetadata,
    name: string,
    columnType: ColumnType,
    style: string,
    ordering: number,
    defaultValue: string | null,
    isNullable: boolean,
    isUnique: boolean,
    isPrimaryKey: boolean
};