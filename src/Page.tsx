import { useCallback, useEffect, useState } from "react";
import { PageBreadcrumb, ReportPageBreadcrumb, ObjectPageBreadcrumb } from "./breadcrumb";
import { 
    Typography,
    Breadcrumb,
    Chip,
} from "@material-tailwind/react";
import { ObjectPage } from "./page/Object";
import { AgGridProvider } from "ag-grid-react";
import { AllCommunityModule, enableDevValidations } from 'ag-grid-community';
import { TableColumnMetadata } from "./api/model/tableColumn";
import { TablePage } from "./page/Table";
import { ReportPage } from "./page/Report";
import { ReportColumnMetadata } from "./api/model/reportColumn";

type PageProps = {
    schema: ['table' | 'report', number, string] | null,
    onRequestCreateTableColumn: (tableOid: number, columnOid: number | null) => void,
    onRequestEditTableColumn: (columnMetadata: TableColumnMetadata) => void,
    onRequestCreateReportColumn: (reportOid: number, columnOid: number | null) => void,
    onRequestEditReportColumn: (columnMetadata: ReportColumnMetadata) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onError: (e: unknown) => void,
};

export function Page(props: PageProps): React.JSX.Element {
    /* 
    useEffect(() => {
        // This enables warnings, etc. for AgGrid
        // Unnecessary now that grid debugging is done
        enableDevValidations();
    }, []);
    */

    const [pageBreadcrumbs, setPageBreadcrumbs] = useState<PageBreadcrumb[]>([]);

    /**
     * Opens a drill-down report page.
     */
    const openDrillDownReportPage = useCallback((report: ReportPageBreadcrumb) => {
        setPageBreadcrumbs((oldPageBreadcrumbs) => oldPageBreadcrumbs.concat([report]));
    }, []);

    /**
     * Opens an object page.
     */
    const openObjectPage = useCallback((object: ObjectPageBreadcrumb) => {
        setPageBreadcrumbs((oldPageBreadcrumbs) => oldPageBreadcrumbs.concat([object]));
    }, []);

    useEffect(() => {
        if (props.schema !== null) {
            if (props.schema[0] === 'table') {
                setPageBreadcrumbs([{
                    page: 'table',
                    tableOid: props.schema[1],
                    name: props.schema[2]
                }]);
            } else {
                setPageBreadcrumbs([{
                    page: 'report',
                    reportOid: props.schema[1],
                    name: props.schema[2],
                    oidFilters: []
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
                {lastPageBreadcrumb.page === 'table' && <TablePage
                    {...lastPageBreadcrumb}
                    onRequestCreateTableColumn={props.onRequestCreateTableColumn}
                    onRequestEditTableColumn={props.onRequestEditTableColumn}
                    onRequestUploadFile={props.onRequestUploadFile}
                    onRequestOpenDrillDownReport={openDrillDownReportPage}
                    onRequestOpenObject={openObjectPage}
                    onError={props.onError}
                />}
                {lastPageBreadcrumb.page === 'report' && <ReportPage
                    {...lastPageBreadcrumb}
                    onRequestCreateReportColumn={props.onRequestCreateReportColumn}
                    onRequestEditReportColumn={props.onRequestEditReportColumn}
                    onRequestUploadFile={props.onRequestUploadFile}
                    onRequestOpenDrillDownReport={openDrillDownReportPage}
                    onRequestOpenObject={openObjectPage}
                    onError={props.onError}
                />}
                {lastPageBreadcrumb.page === 'object' && (<ObjectPage 
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