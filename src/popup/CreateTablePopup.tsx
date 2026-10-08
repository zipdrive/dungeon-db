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
import { queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import Form from "./form/Form";
import { TableListItem } from "../api/model/table";

export type CreateTablePopupProps = {
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function CreateTablePopup(props: CreateTablePopupProps): React.JSX.Element {
    const [tableName, setTableName] = useState<string>('');
    const [tableMastersList, setTableMastersList] = useState<TableListItem[]>([]);
    const [isTableMastersListPending, startTableMastersListTransition] = useTransition();
    const [selectedTableMasters, setSelectedTableMasters] = useState<string[]>([]);

    useEffect(() => {
        const unlistenTableMasters = listen<number[]>('table', () => {
            loadTableMasters();
        });

        return () => {
            unlistenTableMasters.then(f => f());
        };
    }, []);

    /**
     * Loads the list of master schemas.
     */
    function loadTableMasters() {
        startTableMastersListTransition(async () => {
            const temp: TableListItem[] = [];
            try {
                await queryAsync({
                    tableMasters: {
                        tableOid: null,
                        channel: new Channel<TableListItem>((item) => {
                            temp.push(item);
                        })
                    }
                });
            } catch (e) {
                props.onError(e);
            }
            startTableMastersListTransition(() => {
                setTableMastersList(temp);
                setSelectedTableMasters(selectedTableMasters.filter((selectedTableMaster) => {
                    const selectedTableMasterOid: number = parseInt(selectedTableMaster);
                    return temp.findIndex((tableMaster) => tableMaster.oid == selectedTableMasterOid) >= 0;
                }));
            });
        });
    }

    /**
     * Creates a new schema from the inputted information.
     */
    async function createTableAsync(): Promise<boolean> {
        try {
            await executeAsync({
                createTable: {
                    oid: 0,
                    name: tableName,
                    masterOids: selectedTableMasters.map((selectedMasterSchema) => parseInt(selectedMasterSchema))
                }
            });
            return true;
        } catch (e) {
            props.onError(e);
            return false;
        }
    }

    return (<div className="flex flex-col gap-y-6">
        <Form title="Create New Table">
            <Form.TextField label="Table Name" value={tableName} onSetValue={setTableName} />
            {isTableMastersListPending ? (
                <Form.CustomField label="Inherit Columns From"><Spinner /></Form.CustomField>
            ) : (tableMastersList.length == 0 ? (
                <Form.CustomField label="Inherit Columns From">
                    <Typography variant="small" className="text-center">
                        No schemas to inherit columns from.
                    </Typography>
                </Form.CustomField>
            ) : (<Form.MultiselectField
                label="Inherit Columns From"
                value={selectedTableMasters}
                possibleValues={tableMastersList.map((masterSchema) => { return { value: masterSchema.oid.toString(), label: masterSchema.name }; })}
                onSetValue={setSelectedTableMasters}
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
                    if (await createTableAsync()) {
                        props.onClosePopup();
                    }
                }}
            >
                <span>Confirm</span>
            </Button>
        </div>
    </div>);
}