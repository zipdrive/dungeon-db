import { SchemaPageBreadcrumb, ObjectPageBreadcrumb } from "./breadcrumb";

type ObjectProps = {
    schemaOid: number,
    oidFilters: [string, number][],
    onRequestOpenSchema: (schema: SchemaPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
};

export function Object(props: ObjectProps): React.JSX.Element {
    return (<div></div>);
}