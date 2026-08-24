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
  Tabs,
  TabsHeader,
  TabsBody,
  TabPanel,
  Tab,
} from "@material-tailwind/react";
import { queryAsync, ToggledHierarchicalListItemMetadata } from "../api/query";
import { Channel } from "@tauri-apps/api/core";

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
    const [masterSchemas, setMasterSchemas] = useState<ToggledHierarchicalListItemMetadata[]>([]);
    const [isMasterSchemaListPending, startMasterSchemaListTransition] = useTransition();
    const [selectedMasterSchemas, setSelectedMasterSchemas] = useState<number[]>([]);

    useEffect(() => {
        setSchemaName(props.schemaName);
        setSelectedMasterSchemas(props.masterSchemaOids);

        if (props.isOpen) {
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
                    setSelectedMasterSchemas(selectedMasterSchemas.filter((masterSchemaOid) => temp.findIndex((masterSchema) => masterSchema.oid == masterSchemaOid) >= 0));
                });
            });
        }
    }, [props.schemaOid, props.schemaName, props.schemaType, props.masterSchemaOids, props.isOpen]);

    /**
     * Resets the form.
     */
    function resetForm() {
        setSchemaName('');
        setSelectedMasterSchemas([]);
    }

    /**
     * Edits the schema from the inputted information.
     */
    async function editSchemaAsync() {
        if (props.schemaType === 'table') {
            await executeAsync({
                editTable: {
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
                editReport: {
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
    }

    return (
        <Dialog open={props.isOpen} handler={props.onClosePopup}>
            <DialogHeader>Edit Schema</DialogHeader>
            <DialogBody>
                <Tabs value="general">
                    <TabsHeader>
                        <Tab key="general" value="general">
                            General
                        </Tab>
                        <Tab key="columns" value="columns">
                            Columns 
                        </Tab>
                    </TabsHeader>
                    <TabsBody>
                        <TabPanel key="general" value="general" className="flex flex-col gap-4">
                            <Typography variant="h6">Schema Name:</Typography>
                            <Input value={schemaName} onChange={(e) => { setSchemaName(e.target.value); }} label="Schema Name" />
                            <Typography variant="h6">Schema Type:</Typography>
                            <List className="flex flex-row py-0">
                                <ListItem>
                                    <Tooltip content="A table is a data schema with columns and rows.">
                                        <Radio name="schemaType" label="Table" value="table" className="py-0" ripple={false} checked={props.schemaType === 'table'} />
                                    </Tooltip>
                                </ListItem>
                                <ListItem>
                                    <Tooltip content="A report is a virtual schema that references the data from one or more tables.">
                                        <Radio name="schemaType" label="Report" value="report" className="py-0" ripple={false} checked={props.schemaType === 'report'} />
                                    </Tooltip>
                                </ListItem>
                            </List>
                            <Typography variant="h6">Inherit Columns From:</Typography>
                            {
                                isMasterSchemaListPending ? (<Spinner />) : (
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
                            }
                        </TabPanel>
                        <TabPanel key="columns" value="columns">
                            <div />
                        </TabPanel>
                    </TabsBody>
                </Tabs>
            </DialogBody>
            <DialogFooter>
                <Button
                    variant="text"
                    color="red"
                    onClick={() => {
                        props.onClosePopup();
                        resetForm();
                    }}
                    className="mr-1"
                >
                    <span>Cancel</span>
                </Button>
                <Button
                    variant="gradient" 
                    color="green" 
                    onClick={async () => {
                        await editSchemaAsync();
                        props.onClosePopup();
                        resetForm();
                    }}
                >
                    <span>Confirm</span>
                </Button>
            </DialogFooter>
        </Dialog>
    );
}