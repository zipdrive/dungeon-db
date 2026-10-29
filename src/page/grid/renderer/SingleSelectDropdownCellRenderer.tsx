import React from "react";
import { TableRowLabel } from "../../../api/model/tableRow";
import { Spinner } from "@material-tailwind/react";

export type SingleSelectDropdownCellRendererProps = {
    value: number | null,
    items: Promise<TableRowLabel[]>,
};

export function SingleSelectDropdownCellRenderer(props: SingleSelectDropdownCellRendererProps): React.JSX.Element {
    const [label, setLabel] = React.useState<string | null>(null);
    React.useEffect(() => {
        props.items.then((items) => {
            setLabel(items.find(({ oid }) => oid == props.value)?.label ?? '');
        });
    }, [props.value, props.items]);
    
    return <>{label !== null ? label : <Spinner />}</>;
}