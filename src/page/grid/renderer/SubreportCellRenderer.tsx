import React from "react";
import { getDrillDownReportLabelAsync, getObjectLabelAsync } from "../../../api/query";
import { Spinner } from "@material-tailwind/react";

export type SubreportCellRendererProps = {
    reportOid: number,
    oidFilters: [string, number[]][],
    onError: (e: unknown) => void,
};

export function SubreportCellRenderer(props: SubreportCellRendererProps): React.JSX.Element {
    const [label, setLabel] = React.useState<string | null>(null);
    const [isLabelPending, startLabelTransition] = React.useTransition();
    React.useEffect(() => {
        startLabelTransition(async () => {
            let subreportLabel: string = '';
            try {
                subreportLabel = await getDrillDownReportLabelAsync(props.reportOid, props.oidFilters);
            } catch (e) {
                props.onError(e);
            }

            startLabelTransition(() => {
                setLabel(subreportLabel);
            });
        });
    }, [props.reportOid, props.oidFilters, props.onError]);

    return isLabelPending ? <Spinner /> : <>{label}</>;
}