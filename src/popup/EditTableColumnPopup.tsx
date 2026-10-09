import { 
    Button,
    Alert,
    Dialog,
} from "@material-tailwind/react";
import { useEffect, useState, useTransition } from "react";
import { TableColumnBaseType, TableColumnType, TableColumnMetadata } from "../api/model/tableColumn";
import { queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { executeAsync } from "../api/action";
import Form from "./form/Form";
import { TableListItem } from "../api/model/table";
import { ReportListItem } from "../api/model/report";

export type EditTableColumnPopupProps = {
    columnMetadata: TableColumnMetadata,
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function EditTableColumnPopup(props: EditTableColumnPopupProps): React.JSX.Element {
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

    useEffect(() => {
        setColumnName(props.columnMetadata.name);
        setPrimaryKey(props.columnMetadata.isPrimaryKey);
        setColumnSize(props.columnMetadata.size);
        setColumnStyle(props.columnMetadata.style);

        if ('primitive' in props.columnMetadata.columnType) {
            setColumnBaseType(props.columnMetadata.columnType.primitive.primitive);
            setDefaultValue(props.columnMetadata.columnType.primitive.defaultValue ?? '');
            setRefTable(undefined);
            setRefReport(undefined);
        } else if ('file' in props.columnMetadata.columnType) {
            setColumnBaseType('file');
            setDefaultValue('');
            setRefTable(undefined);
            setRefReport(undefined);
        } else if ('object' in props.columnMetadata.columnType) {
            setColumnBaseType('object');
            setDefaultValue('');
            setRefTable(props.columnMetadata.columnType.object.tableOid.toString());
            setRefReport(undefined);
        } else if ('select' in props.columnMetadata.columnType) {
            setColumnBaseType('select');
            setDefaultValue('');
            setRefTable(props.columnMetadata.columnType.singleSelect.tableOid.toString());
            setRefReport(undefined);
        } else if ('multiselect' in props.columnMetadata.columnType) {
            setColumnBaseType('multiselect');
            setDefaultValue('');
            setRefTable(props.columnMetadata.columnType.multiSelect.tableOid.toString());
            setRefReport(undefined);
        } else if ('subreport' in props.columnMetadata.columnType) {
            setColumnBaseType('subreport');
            setDefaultValue('');
            setRefTable(undefined);
            setRefReport(props.columnMetadata.columnType.subreport.reportOid.toString());
        }
        setConfirmAlert(null);
    }, [props.columnMetadata]);

    /**
     * Creates the column.
     */
    async function editTableColumnAsync(): Promise<boolean> {
        setConfirmAlert(null);

        let columnType: TableColumnType;
        let hasColumnTypeChanged: boolean;
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
                hasColumnTypeChanged = !('primitive' in props.columnMetadata.columnType) 
                    || props.columnMetadata.columnType.primitive.primitive !== columnType.primitive.primitive
                    || props.columnMetadata.columnType.primitive.defaultValue !== columnType.primitive.defaultValue;
                break;
            case 'file':
                columnType = {
                    file: {
                        oid: 0
                    }
                };
                hasColumnTypeChanged = !('file' in props.columnMetadata.columnType);
                break;
            case 'object':
                if (refTable !== undefined) {
                    columnType = { object: { oid: 0, tableOid: parseInt(refTable) }};
                    hasColumnTypeChanged = !('object' in props.columnMetadata.columnType) 
                        || props.columnMetadata.columnType.object.tableOid !== columnType.object.tableOid;
                } else {
                    setConfirmAlert('"Table" is a required field if column type "Object" is selected!');
                    return false;
                }
                break;
            case 'singleSelect':
                if (refTable !== undefined) {
                    columnType = { singleSelect: { oid: 0, tableOid: parseInt(refTable) }};
                    hasColumnTypeChanged = !('select' in props.columnMetadata.columnType)
                        || props.columnMetadata.columnType.singleSelect.tableOid !== columnType.singleSelect.tableOid;
                } else {
                    setConfirmAlert('"Table" is a required field if column type "Single-Select Dropdown" is selected!');
                    return false;
                }
                break;
            case 'multiSelect':
                if (refTable !== undefined) {
                    columnType = { multiSelect: { oid: 0, tableOid: parseInt(refTable) }};
                    hasColumnTypeChanged = !('multiselect' in props.columnMetadata.columnType)
                        || props.columnMetadata.columnType.multiSelect.tableOid !== columnType.multiSelect.tableOid;
                } else {
                    setConfirmAlert('"Table" is a required field if column type "Multi-Select Dropdown" is selected!');
                    return false;
                }
                break;
            case 'subreport':
                if (refReport !== undefined) {
                    columnType = { subreport: { oid: 0, reportOid: parseInt(refReport) }};
                    hasColumnTypeChanged = !('subreport' in props.columnMetadata.columnType)
                        || props.columnMetadata.columnType.subreport.reportOid !== columnType.subreport.reportOid;
                } else {
                    setConfirmAlert('"Report" is a required field if column type "Drill-Down Report" is selected!');
                    return false;
                }
                break;
        }

        try {
            if (hasColumnTypeChanged) {
                // Replace the old column, if the column type has been changed
                await executeAsync({
                    replaceTableColumn: {
                        oldMetadata: props.columnMetadata,
                        newMetadata: {
                            oid: 0,
                            name: columnName,
                            columnType,
                            isPrimaryKey,
                            size: columnSize,
                            style: columnStyle
                        }
                    }
                });
            } else {
                // Edit the metadata of the column
                await executeAsync({
                    editTableColumnMetadata: {
                        metadata: {
                            oid: props.columnMetadata.oid,
                            name: columnName,
                            columnType: props.columnMetadata.columnType,
                            isPrimaryKey,
                            size: columnSize,
                            style: columnStyle
                        }
                    }
                });
            }
            return true;
        } catch (e) {
            props.onError(e);
            return false;
        }
    }

    console.log(columnName, columnBaseType, isPrimaryKey);
    return (<div className="flex flex-col gap-y-6">
        <Form 
            title="Edit Column"
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
                    label: "CSS",
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
                    if (await editTableColumnAsync()) {
                        props.onClosePopup();
                    }
                }}
            >
                <span>Confirm</span>
            </Button>
        </div>
    </div>);
}