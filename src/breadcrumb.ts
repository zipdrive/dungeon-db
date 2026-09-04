import { Schema } from "./api/model/schema";

export type SchemaPageBreadcrumb = {
    name: string,
    schema: Schema,
    oidFilters: [string, number][],
    customFilters: string[],
};

export type ObjectPageBreadcrumb = {
    name: string,
    schema: Schema,
    oidFilters: [string, number][],
};

export type PageBreadcrumb = {
    schema: SchemaPageBreadcrumb
} | {
    object: ObjectPageBreadcrumb
};