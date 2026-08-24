import { 
    DialogBody, 
    DialogHeader,
    DialogFooter,
    Typography,
    Input,
    Button,
    Checkbox,
    Select,
    Option,
    Alert,
    Textarea,
} from "@material-tailwind/react";
import { useState, useEffect } from "react";
import { executeAsync } from "../api/action";
import { FullMetadata as SchemaFullMetadata } from "../api/model/schema";
import { ColumnType } from "../api/model/column";
import { listen } from "@tauri-apps/api/event";
import { DropdownValue, HierarchicalListItemMetadata, queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";

export type CreateColumnPopupProps = {
    schema: SchemaFullMetadata,
    isTableColumn: boolean,
    ordering: number | null,
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

export function CreateColumnPopup(props: CreateColumnPopupProps & { isOpen: boolean }): React.JSX.Element {
    const [columnName, setColumnName] = useState<string>('');
    const [columnBaseType, setColumnBaseType] = useState<ColumnBaseType>(props.isTableColumn ? 'plainText' : 'formula');
    const [isPrimaryKey, setPrimaryKey] = useState<boolean>(false);
    const [isHiddenColumn, setHiddenColumn] = useState<boolean>(false);
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
        if (props.isOpen) {
            setColumnName('');
            setColumnBaseType(props.isTableColumn ? 'plainText' : 'formula');
            setPrimaryKey(false);
            setHiddenColumn(false);
            setDefaultValue('');
            setColumnStyle('');
            setFormula('');
            setRefTable(undefined);
            setRefReport(undefined);
            setConfirmAlert(null);
        }
    }, [props.isOpen]);

    /**
     * Creates the column.
     */
    async function createColumnAsync(): Promise<boolean> {
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
            await executeAsync({
                createColumn: {
                    oid: 0,
                    name: columnName,
                    columnType,
                    schema: props.schema,
                    isPrimaryKey,
                    hidden: isHiddenColumn,
                    defaultValue: defaultValue === '' ? null : defaultValue,
                    style: columnStyle,
                    ordering: props.ordering ?? 0,
                }
            });
            return true;
        } catch (e) {
            props.onError(e);
            return false;
        }
    }

    return (<>
        <DialogHeader>Create New Column</DialogHeader>
        <DialogBody className="flex flex-col gap-4">
            <Typography variant="h6">Column Name:</Typography>
            <Input value={columnName} onChange={(e) => { setColumnName(e.target.value); }} label="Column Name" />
            <Typography variant="h6">Column Type:</Typography>
            {props.isTableColumn ? (<Select
                label="Column Type" 
                value={columnBaseType} 
                onChange={(e) => { if (e) { setColumnBaseType(e as ColumnBaseType); } }}
            >
                <Option value="plainText">Plain Text</Option>
                <Option value="integer">Integer</Option>
                <Option value="number">Number</Option>
                <Option value="boolean">Checkbox</Option>
                <Option value="date">Date</Option>
                <Option value="datetime">Datetime</Option>
                <Option value="file">File</Option>
                <Option value="image">Image</Option>
                <Option value="object" disabled={refTableList.length == 0}>Object</Option>
                <Option value="select" disabled={refTableList.length == 0}>Single-Select Dropdown</Option>
                <Option value="multiselect" disabled={refTableList.length == 0}>Multi-Select Dropdown</Option>
                <Option value="jsonText">JSON</Option>
                <Option value="xmlText">XML</Option>
                <Option value="markdownText">Markdown Text</Option>
                <Option value="formula">Formula</Option>
                <Option value="subreport" disabled={refReportList.length == 0}>Drill-Down Report</Option>
            </Select>) : (<Select
                label="Column Type" 
                value={columnBaseType} 
                onChange={(e) => { if (e) { setColumnBaseType(e as ColumnBaseType); } }}
            >
                <Option value="formula">Formula</Option>
                <Option value="subreport" disabled={refReportList.length == 0}>Drill-Down Report</Option>
            </Select>)}
            <Typography variant="h6">Is Primary Key?</Typography>
            <Checkbox checked={isPrimaryKey} onChange={(e) => { setPrimaryKey(e.target.checked); }} label="Is Primary Key?" />
            <Typography variant="h6">Is Hidden?</Typography>
            <Checkbox checked={isHiddenColumn} onChange={(e) => { setHiddenColumn(e.target.checked); }} label="Is Hidden?" />
            {(columnBaseType === 'plainText' 
                || columnBaseType === 'jsonText' 
                || columnBaseType === 'xmlText' 
                || columnBaseType === 'markdownText' 
                || columnBaseType === 'integer' 
                || columnBaseType === 'number' 
                || columnBaseType === 'date' 
                || columnBaseType === 'datetime' 
                || columnBaseType === 'boolean'
            ) && (<>
                <Typography variant="h6">Default Value:</Typography>
                <Input value={defaultValue} onChange={(e) => { setDefaultValue(e.target.value); }} label="Default Value" />
            </>)}
            {(columnBaseType === 'object'
                || columnBaseType === 'select'
                || columnBaseType === 'multiselect'
            ) && (<>
                <Typography variant="h6">Table:</Typography>
                <Select label="Table" value={refTable} onChange={(e) => { if (e) { setRefTable(e); }}}>
                    {refTableList.map(({ oid, name }) => (<Option value={oid.toString()}>{name}</Option>))}
                </Select>
            </>)}
            {columnBaseType === 'subreport' && (<>
                <Typography variant="h6">Report:</Typography>
                <Select label="Report" value={refTable} onChange={(e) => { if (e) { setRefReport(e); }}}>
                    {refReportList.map(({ oid, name }) => (<Option value={oid.toString()}>{name}</Option>))}
                </Select>
            </>)}
            {columnBaseType === 'formula' && (<>
                <Typography variant="h6">Formula:</Typography>
                <Textarea label="Formula" value={formula} onChange={(e) => { setFormula(e.target.value); }} />
            </>)}
        </DialogBody>
        <DialogFooter className="gap-y-2">
            {confirmAlert && (<Alert color="red">{confirmAlert}</Alert>)}
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
                    if (await createColumnAsync()) {
                        props.onClosePopup();
                    }
                }}
            >
                <span>Confirm</span>
            </Button>
        </DialogFooter>
    </>);
}