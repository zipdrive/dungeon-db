import { 
    Button,
    Alert,
    Dialog,
} from "@material-tailwind/react";
import { useEffect, useState } from "react";
import { FullMetadata as ColumnFullMetadata, ColumnType } from "../api/model/column";
import { DropdownValue, queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { executeAsync } from "../api/action";
import Form from "./form/Form";

export type EditColumnPopupProps = {
    columnMetadata: ColumnFullMetadata,
    isTableColumn: boolean,
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

type ColumnBaseType = 'plainText'
    | 'jsonText'
    | 'xmlText'
    | 'markdownText'
    | 'integer'
    | 'number'
    | 'boolean'
    | 'date'
    | 'datetime'
    | 'file'
    | 'image'
    | 'object'
    | 'select'
    | 'multiselect'
    | 'formula'
    | 'subreport';

export function EditColumnPopup(props: EditColumnPopupProps): React.JSX.Element {
    const [columnName, setColumnName] = useState<string>('');
    const [columnBaseType, setColumnBaseType] = useState<ColumnBaseType>(props.isTableColumn ? 'plainText' : 'formula');
    const [isPrimaryKey, setPrimaryKey] = useState<boolean>(false);
    const [defaultValue, setDefaultValue] = useState<string>('');
    const [columnStyle, setColumnStyle] = useState<string>('');

    const [formula, setFormula] = useState<string>('');
    const [refTableList, setRefTableList] = useState<{ oid: number, name: string }[]>([]);
    const [refTable, setRefTable] = useState<string | undefined>(undefined);
    const [refReportList, setRefReportList] = useState<{ oid: number, name: string }[]>([]);
    const [refReport, setRefReport] = useState<string | undefined>(undefined);

    const [confirmAlert, setConfirmAlert] = useState<string | null>(null);

    useEffect(() => {
        async function updateRefTableList() {
            const queriedRefTableList: { oid: number, name: string }[] = [];
            await queryAsync({
                columnAssociatedTables: {
                    channel: new Channel<DropdownValue>(({ value, label }) => {
                        queriedRefTableList.push({ oid: value, name: label });
                    })
                }
            });
            setRefTableList(queriedRefTableList);
        }

        async function updateRefReportList() {
            const queriedRefReportList: { oid: number, name: string }[] = [];
            await queryAsync({
                columnAssociatedReports: {
                    channel: new Channel<DropdownValue>(({ value, label }) => {
                        queriedRefReportList.push({ oid: value, name: label });
                    })
                }
            });
            setRefReportList(queriedRefReportList);
        }

        updateRefTableList();
        updateRefReportList();

        const unlistenSchemas = listen<number[]>('schema', async (_updatedSchemas) => {
            await Promise.all([
                updateRefTableList(),
                updateRefReportList()
            ]);
        });

        return () => {
            unlistenSchemas.then(f => f());
        }
    }, []);

    useEffect(() => {
        setColumnName(props.columnMetadata.name);
        setPrimaryKey(props.columnMetadata.isPrimaryKey);
        setDefaultValue(props.columnMetadata.defaultValue ?? '');
        setColumnStyle(props.columnMetadata.style);

        if ('primitive' in props.columnMetadata.columnType) {
            setColumnBaseType(props.columnMetadata.columnType.primitive);
        } else if ('object' in props.columnMetadata.columnType) {
            setColumnBaseType('object');
            setRefTable(props.columnMetadata.columnType.object.tableOid.toString());
            setRefReport(undefined);
            setFormula('');
        } else if ('select' in props.columnMetadata.columnType) {
            setColumnBaseType('select');
            setRefTable(props.columnMetadata.columnType.select.tableOid.toString());
            setRefReport(undefined);
            setFormula('');
        } else if ('multiselect' in props.columnMetadata.columnType) {
            setColumnBaseType('multiselect');
            setRefTable(props.columnMetadata.columnType.multiselect.tableOid.toString());
            setRefReport(undefined);
            setFormula('');
        } else if ('formula' in props.columnMetadata.columnType) {
            setColumnBaseType('formula');
            setRefTable(undefined);
            setRefReport(undefined);
            setFormula(props.columnMetadata.columnType.formula.formula);
        } else if ('subreport' in props.columnMetadata.columnType) {
            setColumnBaseType('subreport');
            setRefTable(undefined);
            setRefReport(props.columnMetadata.columnType.subreport.reportOid.toString());
            setFormula('');
        }
        setConfirmAlert(null);
    }, [props.columnMetadata]);

    /**
     * Creates the column.
     */
    async function editColumnAsync(): Promise<boolean> {
        setConfirmAlert(null);

        let columnType: ColumnType;
        switch (columnBaseType) {
            case 'plainText':
            case 'jsonText':
            case 'xmlText':
            case 'markdownText':
            case 'boolean':
            case 'integer':
            case 'number':
            case 'date':
            case 'datetime':
            case 'file':
            case 'image':
                columnType = { primitive: columnBaseType };
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
            case 'formula':
                columnType = { formula: { oid: 0, formula }};
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
            if (columnName === props.columnMetadata.name 
                && JSON.stringify(columnType) == JSON.stringify(props.columnMetadata.columnType)
                && isPrimaryKey === props.columnMetadata.isPrimaryKey
                && (defaultValue === '' ? null : defaultValue) === props.columnMetadata.defaultValue
            ) {
                if (columnStyle !== props.columnMetadata.style) {
                    await executeAsync({
                        editColumnStyle: {
                            metadata: props.columnMetadata,
                            newColumnStyle: columnStyle
                        }
                    });
                }
            } else {
                await executeAsync({
                    editColumn: {
                        oid: 0,
                        name: columnName,
                        columnType,
                        schema: props.columnMetadata.schema,
                        isPrimaryKey,
                        defaultValue: defaultValue === '' ? null : defaultValue,
                        style: columnStyle,
                        ordering: props.columnMetadata.ordering,
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
        <Form title="Edit Column">
            <Form.TextField label="Column Name" value={columnName} onSetValue={setColumnName} />
            <Form.SelectField 
                label="Column Type"
                value={columnBaseType}
                possibleValues={[
                    { value: 'plainText', label: "Plain Text", disabled: !props.isTableColumn },
                    { value: 'integer', label: "Integer", disabled: !props.isTableColumn },
                    { value: 'number', label: "Number", disabled: !props.isTableColumn },
                    { value: 'boolean', label: "Checkbox", disabled: !props.isTableColumn },
                    { value: 'date', label: "Date", disabled: !props.isTableColumn },
                    { value: 'datetime', label: "Datetime", disabled: !props.isTableColumn },
                    { value: 'object', label: "Object", disabled: !props.isTableColumn || refTableList.length == 0 },
                    { value: 'select', label: "Single-Select Dropdown", disabled: !props.isTableColumn || refTableList.length == 0 },
                    { value: 'multiselect', label: "Multi-Select Dropdown", disabled: !props.isTableColumn || refTableList.length == 0 },
                    { value: 'file', label: "File", disabled: !props.isTableColumn },
                    { value: 'image', label: "Image", disabled: !props.isTableColumn },
                    { value: 'jsonText', label: "JSON", disabled: !props.isTableColumn },
                    { value: 'formula', label: "Formula" },
                    { value: 'subreport', label: "Drill-Down Report", disabled: refReportList.length == 0 },
                ]}
                onSetValue={setColumnBaseType}
            />
            <Form.CheckboxField label="Is Primary Key?" value={isPrimaryKey} onSetValue={setPrimaryKey} />
            {(columnBaseType === 'plainText' 
                || columnBaseType === 'jsonText' 
                || columnBaseType === 'xmlText' 
                || columnBaseType === 'markdownText' 
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
            {columnBaseType === 'formula' && (<Form.FormulaField
                label="Formula"
                value={formula}
                onSetValue={setFormula}
            />)}
            <Form.TextField
                multiline 
                label="CSS Style"
                value={columnStyle}
                onSetValue={setColumnStyle}
            />
        </Form>
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
                    if (await editColumnAsync()) {
                        props.onClosePopup();
                    }
                }}
            >
                <span>Confirm</span>
            </Button>
        </div>
    </div>);
}