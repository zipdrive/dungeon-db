import { Alert, Button, Spinner, Typography } from "@material-tailwind/react";
import { ReportMetadata } from "../api/model/report";
import Form from "./form/Form";
import { startTransition, useEffect, useState, useTransition } from "react";
import { executeAsync } from "../api/action";
import { ReportColumnMetadata } from "../api/model/reportColumn";
import { queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { TableListItem, TableMetadata } from "../api/model/table";
import { TableColumnMetadata } from "../api/model/tableColumn";

export type EditTablePopupProps = {
    metadata: TableMetadata,
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function EditTablePopup(props: EditTablePopupProps): React.JSX.Element {
    const [tableName, setTableName] = useState<string>('');
    const [tableMastersList, setTableMastersList] = useState<TableListItem[]>([]);
    const [isTableMastersListPending, startTableMastersListTransition] = useTransition();
    const [selectedTableMasters, setSelectedTableMasters] = useState<string[]>([]);

    const [tableColumnList, setTableColumnList] = useState<TableColumnMetadata[]>([]);
    const [isTableColumnListPending, startTableColumnListTransition] = useTransition();
    
    useEffect(() => {
        setTableName(props.metadata.name);
        
        const unlistenTableMasters = listen<number[]>('table', () => {
            loadTableMasters();
        });

        loadTableColumns();
        const unlistenForTableColumns = listen<number[]>('table', (event) => {
            if (event.payload.indexOf(props.metadata.oid) >= 0) {
                loadTableColumns();
            }
        });

        return () => {
            unlistenTableMasters.then(f => f());
            unlistenForTableColumns.then(f => f());
        };
    }, [props.metadata.oid]);

    /**
     * Loads the list of master schemas.
     */
    function loadTableMasters() {
        startTableMastersListTransition(async () => {
            const temp: TableListItem[] = [];
            try {
                await queryAsync({
                    tableMasters: {
                        tableOid: props.metadata.oid,
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

    function loadTableColumns() {
        startTableColumnListTransition(async () => {
            const temp: TableColumnMetadata[] = [];
            try {
                await queryAsync({
                    tableColumns: {
                        tableOid: props.metadata.oid,
                        channel: new Channel<TableColumnMetadata>((item) => {
                            temp.push(item);
                        })
                    }
                });
            } catch (e) {
                props.onError(e);
            }
            startTableColumnListTransition(() => {
                setTableColumnList(temp);
            });
        });
    }

    /**
     * Overwrites the report metadata with the inputted information.
     */
    async function editReportAsync(): Promise<boolean> {
        try {
            await executeAsync({
                editTable: {
                    oid: props.metadata.oid,
                    name: tableName,
                    masterOids: selectedTableMasters.map((oid) => parseInt(oid))
                }
            });
            return true;
        } catch (e) {
            props.onError(e);
            return false;
        }
    }

    return (<div className="flex flex-col gap-y-6">
        <Form 
            title="Edit Table"
            tabs={[
                {
                    value: 'general',
                    label: "General",
                    fields: (<>
                        <Form.TextField 
                            label="Table Name" 
                            value={tableName} 
                            onSetValue={setTableName} 
                        />
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
                    </>)
                },
                {
                    value: 'columns',
                    label: "Columns",
                    fields: (<Form.CustomField
                        label="Columns"
                    >
                        {isTableColumnListPending ? (<Spinner />) : (<div className="w-full grid grid-row grid-cols-[1fr_auto_auto]">
                            {tableColumnList.map((col) => {
                                return (<>
                                    <Typography>{col.name}</Typography>
                                    <Button 
                                        variant="gradient"
                                    >
                                        Edit
                                    </Button>
                                    <Button 
                                        variant="ghost"
                                        color="error"
                                    >
                                        Delete
                                    </Button>
                                </>);
                            })}
                        </div>)}
                    </Form.CustomField>)
                }
            ]}
        />
        <div className="flex flex-row justify-end gap-y-2">
            <Button
                variant="ghost"
                color="error"
                onClick={() => {
                    props.onClosePopup();
                }}
                className="mr-1"
            >
                <span>Cancel</span>
            </Button>
            <Button
                variant="gradient" 
                onClick={async () => {
                    if (await editReportAsync()) {
                        props.onClosePopup();
                    }
                }}
            >
                <span>Confirm</span>
            </Button>
        </div>
    </div>);
}