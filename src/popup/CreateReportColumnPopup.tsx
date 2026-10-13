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
import { checkFormula, queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import Form from "./form/Form";
import { TableListItem } from "../api/model/table";
import { ReportListItem } from "../api/model/report";
import { ReportColumnType } from "../api/model/reportColumn";

export type CreateReportColumnPopupProps = {
    reportOid: number,
    ordering: number | null,
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function CreateReportColumnPopup(props: CreateReportColumnPopupProps): React.JSX.Element {
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

    /**
     * Creates the column.
     */
    async function createTableColumnAsync(): Promise<boolean> {
        setConfirmAlert(null);

        let columnType: ReportColumnType;
        switch (columnBaseType) {
            case 'formula':
                if (formula) {
                    const result = await checkFormula(formula);
                    if (result.success) {
                        columnType = { formula: { oid: 0, formula }};
                    } else {
                        setConfirmAlert(result.msg);
                        return false;
                    }
                } else {
                    setConfirmAlert('"Formula" is a required field if column type "Formula" is selected!');
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
                createReportColumn: {
                    reportOid: props.reportOid,
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