import { useEffect, useState } from "react";
import { PageBreadcrumb, SchemaPageBreadcrumb, ObjectPageBreadcrumb } from "./breadcrumb";
import { 
    Typography,
    Breadcrumb,
} from "@material-tailwind/react";
import { Schema } from "./Schema";
import { Object } from "./Object";
import { FullMetadata as ColumnFullMetadata } from "./api/model/column";
import { FullMetadata as SchemaFullMetadata } from "./api/model/schema";
import { getSchemaMetadataAsync } from "./api/query";

type PageProps = {
    schema: { oid: number, name: string } | null,
    onRequestCreateColumn: (schema: SchemaFullMetadata, isTableColumn: boolean, ordering: number | null) => void,
    onRequestEditColumn: (columnMetadata: ColumnFullMetadata, isTableColumn: boolean) => void,
};

export function Page(props: PageProps): React.JSX.Element {
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
        return (<div className="flex flex-col w-full">
            <Breadcrumb className="rounded-none h-10 border-b-1 border-b-blue-gray-100" fullWidth>
                {pageBreadcrumbs.map((breadcrumb, idx) => {
                    return (<>
                        {idx > 0 && (<Breadcrumb.Separator />)}
                        <Breadcrumb.Link
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
                <Schema 
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
                    onRequestOpenSchema={openSchemaPage}
                    onRequestOpenObject={openObjectPage}
                /> :
                <Object 
                    {...lastPageBreadcrumb.object}
                    onRequestOpenSchema={openSchemaPage}
                    onRequestOpenObject={openObjectPage}
                />
            }
        </div>);
    }
    return (<div className="grow flex flex-col justify-center content-center">
        <div className="grow" />
        <Typography variant="small" className="shrink text-center">Select a table or report from the sidebar.</Typography>
        <div className="grow" />
    </div>);
}