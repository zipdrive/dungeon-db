import { useEffect, useState, useTransition } from "react";
import { executeAsync } from "../api/action";
import {
  Button,
  Radio,
  Input,
  Typography,
  Tooltip,
  Dialog,
  DialogHeader,
  DialogBody,
  DialogFooter,
  List,
  ListItem,
  Spinner,
} from "@material-tailwind/react";
import { queryAsync, ToggledHierarchicalListItemMetadata } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

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
    const [selectedMasterSchemas, setSelectedMasterSchemas] = useState<number[]>([]);

    useEffect(() => {
        loadMasterSchemas();

        const unlistenMasterSchemas = listen<number[]>('schema', (_updatedSchemas) => {
            loadMasterSchemas();
        });

        return () => {
            unlistenMasterSchemas.then(f => f());
        };
    }, []);

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
                setSelectedMasterSchemas(selectedMasterSchemas.filter((masterSchemaOid) => temp.findIndex((masterSchema) => masterSchema.oid == masterSchemaOid) >= 0));
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
                            masterSchemaOids: selectedMasterSchemas,
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
                            masterSchemaOids: selectedMasterSchemas,
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

    return (
        <>
            <DialogHeader>Create New Schema</DialogHeader>
            <DialogBody className="flex flex-col gap-4">
                <Typography variant="h6">Schema Name:</Typography>
                <Input value={schemaName} onChange={(e) => { setSchemaName(e.target.value); }} label="Schema Name" />
                <Typography variant="h6">Schema Type:</Typography>
                <List className="flex flex-row py-0">
                    <ListItem>
                        <Tooltip content="A table is a data schema with columns and rows.">
                            <Radio name="schemaType" label="Table" value="table" className="py-0" ripple={false} checked={schemaType === 'table'} onChange={(e) => { setSchemaType(e.target.value as 'table' | 'report'); }} />
                        </Tooltip>
                    </ListItem>
                    <ListItem>
                        <Tooltip content="A report is a virtual schema that references the data from one or more tables.">
                            <Radio name="schemaType" label="Report" value="report" className="py-0" ripple={false} checked={schemaType === 'report'} onChange={(e) => { setSchemaType(e.target.value as 'table' | 'report'); }} />
                        </Tooltip>
                    </ListItem>
                </List>
                <Typography variant="h6">Inherit Columns From:</Typography>
                {
                    isMasterSchemaListPending ? (<Spinner />) : (masterSchemas.length == 0 ? 
                        (
                            <Typography variant="small" className="text-center">
                                No schemas to inherit columns from.
                            </Typography>
                        ) : (
                            <List className="flex flex-col">
                                {masterSchemas.map((masterSchema) => {
                                    return (
                                        <ListItem 
                                            selected={selectedMasterSchemas.indexOf(masterSchema.oid) >= 0} 
                                            disabled={masterSchema.disabled} 
                                            onClick={() => {
                                                const newSelectedMasterSchemas = [...selectedMasterSchemas];
                                                const idx: number = newSelectedMasterSchemas.indexOf(masterSchema.oid);
                                                if (idx < 0) {
                                                    newSelectedMasterSchemas.push(masterSchema.oid);
                                                } else {
                                                    newSelectedMasterSchemas.splice(idx, 1);
                                                }
                                                setSelectedMasterSchemas(newSelectedMasterSchemas);
                                                console.log(newSelectedMasterSchemas);
                                            }}
                                        >
                                            {'&nbsp;'.repeat(2 * masterSchema.level)}{masterSchema.name}
                                        </ListItem>
                                    )
                                })}
                            </List>
                        )
                    )
                }
            </DialogBody>
            <DialogFooter>
                <Button
                    variant="text"
                    color="red"
                    onClick={() => {
                        props.onClosePopup();
                    }}
                    className="mr-1"
                >
                    <span>Cancel</span>
                </Button>
                <Button
                    variant="gradient" 
                    color="green" 
                    onClick={async () => {
                        if (await createSchemaAsync()) {
                            props.onClosePopup();
                        }
                    }}
                >
                    <span>Confirm</span>
                </Button>
            </DialogFooter>
        </>
    );
}