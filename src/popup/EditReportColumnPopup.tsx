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
import { ReportColumnMetadata, ReportColumnType } from "../api/model/reportColumn";

export type EditReportColumnPopupProps = {
    columnMetadata: ReportColumnMetadata,
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function EditReportColumnPopup(props: EditReportColumnPopupProps): React.JSX.Element {
    const [columnName, setColumnName] = useState<string>('');
    const [columnBaseType, setColumnBaseType] = useState<'formula' | 'subreport'>('formula');
    const [isPrimaryKey, setPrimaryKey] = useState<boolean>(false);
    const [columnSize, setColumnSize] = useState<number>(150);
    const [columnStyle, setColumnStyle] = useState<string>('');

    const [formula, setFormula] = useState<string>('');
    const [refReportList, setRefReportList] = useState<ReportListItem[]>([]);
    const [refReport, setRefReport] = useState<string | undefined>(undefined);
    const [isRefReportListPending, startRefReportListTransition] = useTransition();

    const [confirmAlert, setConfirmAlert] = useState<string | null>(null);

    useEffect(() => {
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

        updateRefReportList();

        const unlistenReports = listen<number>('report', async () => {
            updateRefReportList();
        });

        return () => {
            unlistenReports.then(f => f());
        }
    }, []);

    useEffect(() => {
        setColumnName(props.columnMetadata.name);
        setPrimaryKey(props.columnMetadata.isPrimaryKey);
        setColumnSize(props.columnMetadata.size);
        setColumnStyle(props.columnMetadata.style);

        if ('formula' in props.columnMetadata.columnType) {
            setColumnBaseType('formula');
            setFormula(props.columnMetadata.columnType.formula.formula);
            setRefReport(undefined);
        } else if ('subreport' in props.columnMetadata.columnType) {
            setColumnBaseType('subreport');
            setFormula('');
            setRefReport(props.columnMetadata.columnType.subreport.reportOid.toString());
        }
        setConfirmAlert(null);
    }, [props.columnMetadata]);

    /**
     * Creates the column.
     */
    async function editTableColumnAsync(): Promise<boolean> {
        setConfirmAlert(null);

        let columnType: ReportColumnType;
        let hasColumnTypeChanged: boolean;
        switch (columnBaseType) {
            case 'formula':
                if (formula) {
                    columnType = { formula: { oid: 0, formula }};
                    hasColumnTypeChanged = !('formula' in props.columnMetadata.columnType)
                        || props.columnMetadata.columnType.formula.formula !== formula;
                } else {
                    setConfirmAlert('"Formula" is a required field if column type "Formula" is selected!');
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
                    replaceReportColumn: {
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
                    editReportColumnMetadata: {
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
                                { value: 'formula', label: "Formula", disabled: false },
                                { value: 'subreport', label: "Drill-Down Report", disabled: refReportList.length == 0 },
                            ]}
                            onSetValue={setColumnBaseType}
                        />
                        <Form.CheckboxField label="Is Primary Key?" value={isPrimaryKey} onSetValue={setPrimaryKey} />
                        {columnBaseType === 'formula' && (<Form.FormulaField
                            label="Formula"
                            value={formula}
                            onSetValue={setFormula}
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