import { 
    Dialog,
    Button,
    Alert,
    Tabs,
} from "@material-tailwind/react";
import { useState, useEffect, useTransition } from "react";
import { executeAsync } from "../api/action";
import { TableColumnBaseType, TableColumnType } from "../api/model/tableColumn";
import { listen } from "@tauri-apps/api/event";
import { queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import Form from "./form/Form";
import { TableListItem } from "../api/model/table";
import { ReportListItem } from "../api/model/report";

export type CreateTableColumnPopupProps = {
    tableOid: number,
    ordering: number | null,
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function CreateTableColumnPopup(props: CreateTableColumnPopupProps): React.JSX.Element {
    const [columnName, setColumnName] = useState<string>('');
    const [columnBaseType, setColumnBaseType] = useState<TableColumnBaseType>('text');
    const [isPrimaryKey, setPrimaryKey] = useState<boolean>(false);
    const [defaultValue, setDefaultValue] = useState<string>('');
    const [columnSize, setColumnSize] = useState<number>(150);
    const [columnStyle, setColumnStyle] = useState<string>('');

    const [refTableList, setRefTableList] = useState<TableListItem[]>([]);
    const [refTable, setRefTable] = useState<string | undefined>(undefined);
    const [isRefTableListPending, startRefTableListTransition] = useTransition();
    const [refReportList, setRefReportList] = useState<ReportListItem[]>([]);
    const [refReport, setRefReport] = useState<string | undefined>(undefined);
    const [isRefReportListPending, startRefReportListTransition] = useTransition();

    const [confirmAlert, setConfirmAlert] = useState<string | null>(null);

    useEffect(() => {
        function updateRefTableList() {
            startRefTableListTransition(async () => {
                const queriedRefTableList: TableListItem[] = [];
                await queryAsync({
                    tables: {
                        channel: new Channel<TableListItem>((item) => {
                            queriedRefTableList.push(item);
                        })
                    }
                });
                startRefTableListTransition(() => {
                    setRefTableList(queriedRefTableList);
                });
            });
        }

        function updateRefReportList() {
            startRefReportListTransition(async () => {
                const queriedRefReportList: ReportListItem[] = [];
                await queryAsync({
                    reports: {
                        channel: new Channel<ReportListItem>((item) => {
                            queriedRefReportList.push(item);
                        })
                    }
                });
                startRefReportListTransition(() => {
                    setRefReportList(queriedRefReportList);
                });
            });
        }

        updateRefTableList();
        updateRefReportList();

        const unlistenTables = listen<number[]>('table', async () => {
            updateRefTableList();
        });
        const unlistenReports = listen<number>('report', async () => {
            updateRefReportList();
        });

        return () => {
            unlistenTables.then(f => f());
            unlistenReports.then(f => f());
        }
    }, []);

    /**
     * Creates the column.
     */
    async function createTableColumnAsync(): Promise<boolean> {
        setConfirmAlert(null);

        let columnType: TableColumnType;
        switch (columnBaseType) {
            case 'text':
            case 'textJson':
            case 'textXml':
            case 'textMarkdown':
            case 'textBBCode':
            case 'boolean':
            case 'integer':
            case 'number':
            case 'date':
            case 'datetime':
                columnType = { 
                    primitive: {
                        oid: 0,
                        primitive: columnBaseType,
                        defaultValue: defaultValue === '' ? null : defaultValue
                    } 
                };
                break;
            case 'file':
                columnType = {
                    file: {
                        oid: 0
                    }
                };
                break;
            case 'object':
                if (refTable !== undefined) {
                    columnType = { object: { oid: 0, tableOid: parseInt(refTable) }};
                } else {
                    setConfirmAlert('"Table" is a required field if column type "Object" is selected!');
                    return false;
                }
                break;
            case 'select':
                if (refTable !== undefined) {
                    columnType = { select: { oid: 0, tableOid: parseInt(refTable) }};
                } else {
                    setConfirmAlert('"Table" is a required field if column type "Single-Select Dropdown" is selected!');
                    return false;
                }
                break;
            case 'multiselect':
                if (refTable !== undefined) {
                    columnType = { multiselect: { oid: 0, tableOid: parseInt(refTable) }};
                } else {
                    setConfirmAlert('"Table" is a required field if column type "Multi-Select Dropdown" is selected!');
                    return false;
                }
                break;
            case 'subreport':
                if (refReport !== undefined) {
                    columnType = { subreport: { oid: 0, reportOid: parseInt(refReport) }};
                } else {
                    setConfirmAlert('"Report" is a required field if column type "Drill-Down Report" is selected!');
                    return false;
                }
                break;
        }

        try {
            await executeAsync({
                createTableColumn: {
                    tableOid: props.tableOid,
                    metadata: {
                        oid: 0,
                        name: columnName,
                        columnType,
                        isPrimaryKey,
                        size: columnSize,
                        style: columnStyle,
                    },
                    ordering: props.ordering
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
            title="Create New Column"
            tabs={[
                {
                    value: 'details',
                    label: "Details",
                    fields: (<>
                        <Form.TextField label="Column Name" value={columnName} onSetValue={setColumnName} />
                        <Form.SelectField 
                            label="Column Type"
                            value={columnBaseType}
                            possibleValues={[
                                { value: 'text', label: "Plain Text", disabled: false },
                                { value: 'integer', label: "Integer", disabled: false },
                                { value: 'number', label: "Number", disabled: false },
                                { value: 'boolean', label: "Checkbox", disabled: false },
                                { value: 'date', label: "Date", disabled: false },
                                { value: 'datetime', label: "Datetime", disabled: false },
                                { value: 'object', label: "Object", disabled: refTableList.length == 0 },
                                { value: 'select', label: "Single-Select Dropdown", disabled: refTableList.length == 0 },
                                { value: 'multiselect', label: "Multi-Select Dropdown", disabled: refTableList.length == 0 },
                                { value: 'file', label: "File", disabled: false },
                                { value: 'textJson', label: "JSON", disabled: false },
                                { value: 'subreport', label: "Drill-Down Report", disabled: refReportList.length == 0 },
                            ]}
                            onSetValue={setColumnBaseType}
                        />
                        <Form.CheckboxField label="Is Primary Key?" value={isPrimaryKey} onSetValue={setPrimaryKey} />
                        {(columnBaseType === 'text' 
                            || columnBaseType === 'textJson' 
                            || columnBaseType === 'textXml' 
                            || columnBaseType === 'textMarkdown' 
                            || columnBaseType === 'textBBCode'
                            || columnBaseType === 'integer' 
                            || columnBaseType === 'number' 
                            || columnBaseType === 'date' 
                            || columnBaseType === 'datetime' 
                            || columnBaseType === 'boolean'
                        ) && (<Form.TextField 
                            label="Default Value" 
                            value={defaultValue} 
                            onSetValue={setDefaultValue} 
                        />)}
                        {(columnBaseType === 'object'
                            || columnBaseType === 'select'
                            || columnBaseType === 'multiselect'
                        ) && (<Form.SelectField 
                            label="Table" 
                            value={refTable} 
                            possibleValues={refTableList.map(({ oid, name }) => { return { value: oid.toString(), label: name }; })} 
                            onSetValue={setRefTable} 
                        />)}
                        {columnBaseType === 'subreport' && (<Form.SelectField 
                            label="Report" 
                            value={refReport} 
                            possibleValues={refReportList.map(({ oid, name }) => { return { value: oid.toString(), label: name }; })} 
                            onSetValue={setRefReport} 
                        />)}
                    </>)
                },
                {
                    value: 'css',
                    label: 'CSS',
                    fields: (<>
                        <Form.IntegerField
                            label="Size"
                            value={columnSize}
                            onSetValue={setColumnSize}
                        />
                        <Form.TextField 
                            multiline 
                            label="CSS Style"
                            value={columnStyle}
                            onSetValue={setColumnStyle}
                        />
                    </>)
                }
            ]}
        />
        <div className="flex flex-row justify-end gap-y-2">
            {confirmAlert && (<Alert color="error">{confirmAlert}</Alert>)}
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
                    if (await createTableColumnAsync()) {
                        props.onClosePopup();
                    }
                }}
            >
                <span>Confirm</span>
            </Button>
        </div>
    </div>);
}