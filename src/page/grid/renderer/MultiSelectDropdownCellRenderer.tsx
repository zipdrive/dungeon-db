import React from "react";
import { TableRowLabel } from "../../../api/model/tableRow";
import { Chip, Spinner } from "@material-tailwind/react";

export type MultiSelectDropdownCellRendererProps = {
    value: number[],
    items: Promise<TableRowLabel[]>,
};

export function MultiSelectDropdownCellRenderer(props: MultiSelectDropdownCellRendererProps): React.JSX.Element {
    const [labels, setLabels] = React.useState<TableRowLabel[] | null>(null);
    React.useEffect(() => {
        props.items.then((items) => {
            setLabels(items.filter(({ oid }) => props.value.indexOf(oid) >= 0));
        });
    }, [props.value, props.items]);
    
    if (labels !== null) {
        return <div>
            {labels.map((item) => <Chip key={`multiselect-${item.oid}`} className="px-2">{item.label}</Chip>)}
        </div>;
    } else {
        return <Spinner />
    }
}