import { useEffect, useState } from "react";
import { executeAsync } from "../api/action";
import {
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

    /**
     * Creates a new schema from the inputted information.
     */
    async function createReportAsync(): Promise<boolean> {
        try {
            await executeAsync({
                createReport: {
                    oid: 0,
                    name: reportName,
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
        </Form>
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