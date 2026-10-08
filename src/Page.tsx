import { useEffect, useState } from "react";
import { PageBreadcrumb, TablePageBreadcrumb, ReportPageBreadcrumb, ObjectPageBreadcrumb } from "./breadcrumb";
import { 
    Typography,
    Breadcrumb,
} from "@material-tailwind/react";
import { SchemaPage } from "./Schema";
import { ObjectPage } from "./page/Object";
import { AgGridProvider } from "ag-grid-react";
import { AllCommunityModule, enableDevValidations } from 'ag-grid-community';
import { TableColumnMetadata } from "./api/model/tableColumn";
import { getTableMetadataAsync } from "./api/query";

type PageProps = {
    schema: ['table' | 'report', number, string] | null,
    onRequestCreateTableColumn: (tableOid: number, ordering: number | null) => void,
    onRequestEditTableColumn: (columnMetadata: TableColumnMetadata) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onError: (e: unknown) => void,
};

export function Page(props: PageProps): React.JSX.Element {
    useEffect(() => {
        enableDevValidations();
    }, []);

    const [pageBreadcrumbs, setPageBreadcrumbs] = useState<PageBreadcrumb[]>([]);

    /**
     * Opens a drill-down report page.
     */
    function openDrillDownReportPage(report: ReportPageBreadcrumb) {
        setPageBreadcrumbs((oldPageBreadcrumbs) => oldPageBreadcrumbs.concat([report]));
    }

    /**
     * Opens an object page.
     */
    function openObjectPage(object: ObjectPageBreadcrumb) {
        setPageBreadcrumbs((oldPageBreadcrumbs) => oldPageBreadcrumbs.concat([object]));
    }

    useEffect(() => {
        if (props.schema !== null) {
            if (props.schema[0] === 'table') {
                setPageBreadcrumbs([{
                    key: 'table',
                    tableOid: props.schema[1],
                    name: props.schema[2]
                }]);
            } else {
                setPageBreadcrumbs([{
                    key: 'report',
                    reportOid: props.schema[1],
                    name: props.schema[2]
                }]);
            }
        } else {
            setPageBreadcrumbs([]);
        }
    }, [props.schema]);

    if (props.schema !== null && pageBreadcrumbs.length > 0) {
        const lastPageBreadcrumb: PageBreadcrumb = pageBreadcrumbs[pageBreadcrumbs.length - 1];
        return (<AgGridProvider modules={[AllCommunityModule]}>
            <div className="grid grid-col grid-rows-[calc(var(--spacing)*10)_calc(100vh-(var(--spacing)*10))] bg-[rgb(var(--color-surface-light)/1)] text-[rgb(var(--color-surface-foreground)/1)] h-screen">
                <Breadcrumb className="px-4 rounded-none w-full border-b-1 border-b-[rgb(var(--color-surface-dark)/1)] bg-[rgb(var(--color-surface)/1)]">
                    {pageBreadcrumbs.map((breadcrumb, idx) => {
                        return (<>
                            {idx > 0 && (<Breadcrumb.Separator />)}
                            <Breadcrumb.Link
                                as="a"
                                className="cursor-pointer text-[rgb(var(--color-primary)/1)]"
                                onClick={() => {
                                    const newPageBreadcrumbs: PageBreadcrumb[] = pageBreadcrumbs.slice(0, idx + 1);
                                    setPageBreadcrumbs(newPageBreadcrumbs);
                                }}
                            >
                                {breadcrumb.name}
                            </Breadcrumb.Link>
                        </>);
                    })}
                </Breadcrumb>
                {lastPageBreadcrumb.key === 'table'}
                {lastPageBreadcrumb.key === 'report'}
                {lastPageBreadcrumb.key === 'object' && (<ObjectPage 
                    {...lastPageBreadcrumb}
                    onRequestEditTableColumn={props.onRequestEditTableColumn}
                    onRequestUploadFile={props.onRequestUploadFile}
                    onRequestOpenDrillDownReport={openDrillDownReportPage}
                    onRequestOpenObject={openObjectPage}
                    onError={props.onError}
                />)}
            </div>
        </AgGridProvider>);
    }
    return (<div className="grow flex flex-col justify-center content-center">
        <div className="grow" />
        <Typography variant="small" className="shrink text-center">Select a table or report from the sidebar.</Typography>
        <div className="grow" />
    </div>);
}