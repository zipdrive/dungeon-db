import { useEffect, useState } from "react";
import { executeAsync } from "../api/action";
import {
    Alert,
  Button,
  Dialog,
} from "@material-tailwind/react";
import Form from "./form/Form";

export type CreateReportPopupProps = {
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function CreateReportPopup(props: CreateReportPopupProps): React.JSX.Element {
    const [reportName, setReportName] = useState<string>('');
    const [baselineFormula, setBaselineFormula] = useState<string>('');

    const [confirmAlert, setConfirmAlert] = useState<string | null>(null);

    /**
     * Creates a new schema from the inputted information.
     */
    async function createReportAsync(): Promise<boolean> {
        try {
            await executeAsync({
                createReport: {
                    oid: 0,
                    name: reportName,
                    baseline: baselineFormula ? {
                        formula: baselineFormula,
                        tableOid: 0,
                        oidName: '',
                        parentOidName: null
                    } : null,
                    filterFormula: null,
                    groupByColumnOids: [],
                    orderByColumnOids: []
                }
            });
            return true;
        } catch (e) {
            props.onError(e);
            return false;
        }
    }

    return (<div className="flex flex-col gap-y-6">
        <Form title="Create New Report">
            <Form.TextField label="Report Name" value={reportName} onSetValue={setReportName} />
            <Form.FormulaField 
                label="Baseline Table" 
                tooltip="A formula that returns a baseline table. Specifying a baseline table will add controls to the report to add and remove rows from that table. Aggregation is disabled when a baseline table is specified."
                value={baselineFormula} 
                onSetValue={setBaselineFormula} 
            />
        </Form>
        {confirmAlert && (<Alert color="error">{confirmAlert}</Alert>)}
        <div className="flex flex-row gap-y-2 justify-end">
            <Dialog.DismissTrigger
                as={Button}
                variant="ghost"
                color="error"
                onClick={() => {
                    props.onClosePopup();
                }}
                className="mr-1"
            >
                <span>Cancel</span>
            </Dialog.DismissTrigger>
            <Button
                variant="gradient" 
                onClick={async () => {
                    if (await createReportAsync()) {
                        props.onClosePopup();
                    }
                }}
            >
                <span>Confirm</span>
            </Button>
        </div>
    </div>);
}