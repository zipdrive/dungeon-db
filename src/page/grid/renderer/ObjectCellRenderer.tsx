import React from "react";
import { getObjectLabelAsync } from "../../../api/query";
import { Spinner } from "@material-tailwind/react";

export type ObjectCellRendererProps = {
    tableOid: number,
    value: number | null,
    onError: (e: unknown) => void
};

export function ObjectCellRenderer(props: ObjectCellRendererProps): React.JSX.Element {
    const [label, setLabel] = React.useState<string | null>(null);
    const [isLabelPending, startLabelTransition] = React.useTransition();
    React.useEffect(() => {
        startLabelTransition(async () => {
            let objectLabel: string = '';
            try {
                objectLabel = props.value !== null ? await getObjectLabelAsync(props.tableOid, props.value) : '';
            } catch (e) {
                props.onError(e);
            }

            startLabelTransition(() => {
                setLabel(objectLabel);
            });
        });
    }, [props.tableOid, props.value, props.onError]);

    return isLabelPending ? <Spinner /> : <>{label}</>;
}