import { useEffect, useState, useTransition } from "react";
import { executeAsync } from "../api/action";
import {
  Button,
  Typography,
  Dialog,
  Spinner,
} from "@material-tailwind/react";
import { queryAsync, ToggledHierarchicalListItemMetadata } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import Form from "./form/Form";

export type EditSchemaPopupProps = {
    schemaOid: number,
    schemaName: string,
    schemaType: 'table' | 'report',
    masterSchemaOids: number[],
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function EditSchemaPopup(props: EditSchemaPopupProps): React.JSX.Element {
    const [schemaName, setSchemaName] = useState<string>('');
    const [schemaType, setSchemaType] = useState<'table' | 'report'>('table');
    const [masterSchemas, setMasterSchemas] = useState<ToggledHierarchicalListItemMetadata[]>([]);
    const [isMasterSchemaListPending, startMasterSchemaListTransition] = useTransition();
    const [selectedMasterSchemas, setSelectedMasterSchemas] = useState<string[]>([]);

    useEffect(() => {
        setSchemaName(props.schemaName);
        setSelectedMasterSchemas(props.masterSchemaOids.map((masterSchemaOid) => masterSchemaOid.toString()));

        startMasterSchemaListTransition(async () => {
            const temp: ToggledHierarchicalListItemMetadata[] = [];
            await queryAsync({
                masterSchemas: {
                    schemaOid: props.schemaOid,
                    isTable: props.schemaType === 'table',
                    channel: new Channel<ToggledHierarchicalListItemMetadata>((item) => {
                        temp.push(item);
                    })
                }
            });
            startMasterSchemaListTransition(() => {
                setMasterSchemas(temp);
                setSelectedMasterSchemas(selectedMasterSchemas.filter((selectedMasterSchema) => {
                    const selectedMasterSchemaOid: number = parseInt(selectedMasterSchema);
                    return temp.findIndex((masterSchema) => masterSchema.oid == selectedMasterSchemaOid) >= 0;
                }));
            });
        });
    }, [props.schemaOid, props.schemaName, props.schemaType, props.masterSchemaOids]);

    /**
     * Edits the schema from the inputted information.
     */
    async function editSchemaAsync(): Promise<boolean> {
        try {
            if (props.schemaType === 'table') {
                await executeAsync({
                    editTable: {
                        schema: {
                            oid: props.schemaOid,
                            name: schemaName,
                            masterSchemaOids: selectedMasterSchemas.map((selectedMasterSchema) => parseInt(selectedMasterSchema)),
                            orderByColumnOids: [],
                        }
                    }
                });
            } else {
                await executeAsync({
                    editReport: {
                        schema: {
                            oid: props.schemaOid,
                            name: schemaName,
                            masterSchemaOids: selectedMasterSchemas.map((selectedMasterSchema) => parseInt(selectedMasterSchema)),
                            orderByColumnOids: [],
                        },
                        filterFormula: null,
                        groupByColumnOids: []
                    }
                });
            }
            return true;
        } catch (e) {
            props.onError(e);
            return false;
        }
    }
    
        return (<div className="flex flex-col gap-y-6">
            <Form title="Edit Schema">
                <Form.TextField label="Schema Name" value={schemaName} onSetValue={setSchemaName} />
                <Form.RadioField label="Schema Type" value={props.schemaType} 
                    possibleValues={[
                        { value: 'table', label: "Table", tooltip: "A table is a data schema with columns and rows.", disabled: props.schemaType !== 'table' }, 
                        { value: 'report', label: "Report", tooltip: "A report is a virtual schema that uses formulas to reference the data from one or more tables.", disabled: props.schemaType !== 'report' },
                    ]} 
                />
                {isMasterSchemaListPending ? (
                    <Form.CustomField label="Inherit Columns From"><Spinner /></Form.CustomField>
                ) : (masterSchemas.length == 0 ? (
                    <Form.CustomField label="Inherit Columns From">
                        <Typography variant="small" className="text-center">
                            No schemas to inherit columns from.
                        </Typography>
                    </Form.CustomField>
                ) : (<Form.MultiselectField
                    label="Inherit Columns From"
                    value={selectedMasterSchemas}
                    possibleValues={masterSchemas.map((masterSchema) => { return { value: masterSchema.oid.toString(), label: masterSchema.name }; })}
                    onSetValue={setSelectedMasterSchemas}
                />))}
            </Form>
            <div className="flex flex-row justify-end gap-y-2">
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
                        if (await editSchemaAsync()) {
                            props.onClosePopup();
                        }
                    }}
                >
                    <span>Confirm</span>
                </Button>
            </div>
        </div>
    );
}