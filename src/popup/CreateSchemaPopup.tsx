import { useEffect, useState, useTransition } from "react";
import { executeAsync } from "../api/action";
import {
  Button,
  Radio,
  Input,
  Typography,
  Tooltip,
  Dialog,
  List,
  ListItem,
  Spinner,
} from "@material-tailwind/react";
import { queryAsync, ToggledHierarchicalListItemMetadata } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import Form, { FormCustomField } from "./form/Form";

export type CreateSchemaPopupProps = {
    defaultSchemaType: 'table' | 'report',
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function CreateSchemaPopup(props: CreateSchemaPopupProps & { isOpen: boolean }): React.JSX.Element {
    const [schemaName, setSchemaName] = useState<string>('');
    const [schemaType, setSchemaType] = useState<'table' | 'report'>('table');
    const [masterSchemas, setMasterSchemas] = useState<ToggledHierarchicalListItemMetadata[]>([]);
    const [isMasterSchemaListPending, startMasterSchemaListTransition] = useTransition();
    const [selectedMasterSchemas, setSelectedMasterSchemas] = useState<string[]>([]);

    useEffect(() => {
        const unlistenMasterSchemas = listen<number[]>('schema', (_updatedSchemas) => {
            loadMasterSchemas();
        });

        return () => {
            unlistenMasterSchemas.then(f => f());
        };
    }, []);

    useEffect(() => {
        loadMasterSchemas();
    }, [schemaType]);

    useEffect(() => {
        setSchemaName('');
        console.log('defaultSchemaType', props.defaultSchemaType);
        setSchemaType(props.defaultSchemaType);
        setSelectedMasterSchemas([]);
    }, [props.isOpen, props.defaultSchemaType]);

    /**
     * Loads the list of master schemas.
     */
    async function loadMasterSchemas() {
        startMasterSchemaListTransition(async () => {
            const temp: ToggledHierarchicalListItemMetadata[] = [];
            await queryAsync({
                masterSchemas: {
                    schemaOid: null,
                    isTable: schemaType === 'table',
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
    }

    /**
     * Creates a new schema from the inputted information.
     */
    async function createSchemaAsync(): Promise<boolean> {
        try {
            if (schemaType === 'table') {
                await executeAsync({
                    createTable: {
                        schema: {
                            oid: 0,
                            name: schemaName,
                            masterSchemaOids: selectedMasterSchemas.map((selectedMasterSchema) => parseInt(selectedMasterSchema)),
                            orderByColumnOids: [],
                        }
                    }
                });
            } else {
                await executeAsync({
                    createReport: {
                        schema: {
                            oid: 0,
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
        <Form title="Create New Schema">
            <Form.TextField label="Schema Name" value={schemaName} onSetValue={setSchemaName} />
            <Form.RadioField label="Schema Type" value={schemaType} 
                possibleValues={[
                    { value: 'table', label: "Table", tooltip: "A table is a data schema with columns and rows." }, 
                    { value: 'report', label: "Report", tooltip: "A report is a virtual schema that uses formulas to reference the data from one or more tables." },
                ]} 
                onSetValue={setSchemaType} 
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
                    if (await createSchemaAsync()) {
                        props.onClosePopup();
                    }
                }}
            >
                <span>Confirm</span>
            </Button>
        </div>
    </div>);
}