import React from "react";
import { TableColumnMetadata } from "../api/model/tableColumn";
import { ObjectPageBreadcrumb, ReportPageBreadcrumb, TablePageBreadcrumb } from "../breadcrumb"

type TablePageProps = Omit<TablePageBreadcrumb, 'key'> & {
    onRequestCreateTableColumn: (tableOid: number, ordering: number | null) => void,
    onRequestEditTableColumn: (columnMetadata: TableColumnMetadata) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

export function TablePage(props: TablePageProps): React.JSX.Element {

}