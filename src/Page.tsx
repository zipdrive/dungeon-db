import { useEffect, useState } from "react";
import { PageBreadcrumb, SchemaPageBreadcrumb, ObjectPageBreadcrumb } from "./breadcrumb";
import { 
    Typography,
    Breadcrumb,
} from "@material-tailwind/react";
import { SchemaPage } from "./Schema";
import { ObjectPage } from "./Object";
import { FullMetadata as ColumnFullMetadata } from "./api/model/column";
import { FullMetadata as SchemaFullMetadata } from "./api/model/schema";
import { getSchemaMetadataAsync } from "./api/query";
import { AgGridProvider } from "ag-grid-react";
import { AllCommunityModule, enableDevValidations } from 'ag-grid-community';

type PageProps = {
    schema: { oid: number, name: string } | null,
    onRequestCreateColumn: (schema: SchemaFullMetadata, isTableColumn: boolean, ordering: number | null) => void,
    onRequestEditColumn: (columnMetadata: ColumnFullMetadata, isTableColumn: boolean) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onError: (e: unknown) => void,
};

export function Page(props: PageProps): React.JSX.Element {
    useEffect(() => {
        enableDevValidations();
    }, []);

    const [pageBreadcrumbs, setPageBreadcrumbs] = useState<PageBreadcrumb[]>([]);

    /**
     * Opens a schema page.
     */
    function openSchemaPage(schema: SchemaPageBreadcrumb) {
        setPageBreadcrumbs(pageBreadcrumbs.concat([{ schema }]));
    }

    /**
     * Opens an object page.
     */
    function openObjectPage(object: ObjectPageBreadcrumb) {
        setPageBreadcrumbs(pageBreadcrumbs.concat([{ object }]));
    }

    useEffect(() => {
        if (props.schema !== null) {
            getSchemaMetadataAsync(props.schema.oid)
                .then((schema) => {
                    setPageBreadcrumbs([{
                        schema: {
                            name: 'table' in schema ? schema.table.schema.name : schema.report.schema.name,
                            schema,
                            oidFilters: [],
                            customFilters: [],
                        }
                    }]);
                });
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
                                {('schema' in breadcrumb ? breadcrumb.schema.name : breadcrumb.object.name)}
                            </Breadcrumb.Link>
                        </>);
                    })}
                </Breadcrumb>
                {'schema' in lastPageBreadcrumb ?
                    <SchemaPage 
                        {...lastPageBreadcrumb.schema}
                        onChangeCustomFilters={(newCustomFilters) => {
                            setPageBreadcrumbs([...pageBreadcrumbs.slice(0, pageBreadcrumbs.length - 1), {
                                schema: {
                                    schema: lastPageBreadcrumb.schema.schema,
                                    name: lastPageBreadcrumb.schema.name,
                                    oidFilters: lastPageBreadcrumb.schema.oidFilters,
                                    customFilters: newCustomFilters
                                }
                            }]);
                        }}
                        onRequestCreateColumn={props.onRequestCreateColumn}
                        onRequestEditColumn={props.onRequestEditColumn}
                        onRequestUploadFile={props.onRequestUploadFile}
                        onRequestOpenSchema={openSchemaPage}
                        onRequestOpenObject={openObjectPage}
                        onError={props.onError}
                    /> :
                    <ObjectPage 
                        {...lastPageBreadcrumb.object}
                        onRequestEditColumn={props.onRequestEditColumn}
                        onRequestUploadFile={props.onRequestUploadFile}
                        onRequestOpenSchema={openSchemaPage}
                        onRequestOpenObject={openObjectPage}
                        onError={props.onError}
                    />
                }
            </div>
        </AgGridProvider>);
    }
    return (<div className="grow flex flex-col justify-center content-center">
        <div className="grow" />
        <Typography variant="small" className="shrink text-center">Select a table or report from the sidebar.</Typography>
        <div className="grow" />
    </div>);
}