import { Alert, Button, Spinner, Typography } from "@material-tailwind/react";
import { ReportMetadata } from "../api/model/report";
import Form from "./form/Form";
import { startTransition, useEffect, useState, useTransition } from "react";
import { executeAsync } from "../api/action";
import { ReportColumnMetadata } from "../api/model/reportColumn";
import { queryAsync } from "../api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

export type EditReportPopupProps = {
    metadata: ReportMetadata,
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function EditReportPopup(props: EditReportPopupProps): React.JSX.Element {
    const [reportName, setReportName] = useState<string>('');
    const [filterFormula, setFilterFormula] = useState<string>('');
    const [groupByColumnOids, setGroupByColumnOids] = useState<string[]>([]);
    const [orderByColumnOids, setOrderByColumnOids] = useState<string[]>([]);

    const [reportColumnList, setReportColumnList] = useState<ReportColumnMetadata[]>([]);
    const [isReportColumnListPending, startReportColumnListTransition] = useTransition();
    
    useEffect(() => {
        setReportName(props.metadata.name);
        setFilterFormula(props.metadata.filterFormula ?? '');
        setGroupByColumnOids(props.metadata.groupByColumnOids.map((oid) => oid.toString()));
        setOrderByColumnOids(props.metadata.orderByColumnOids.map((oid) => oid.toString()));

        loadReportColumns();
        const unlistenForReportColumns = listen<number>('report', (event) => {
            if (event.payload == props.metadata.oid) {
                loadReportColumns();
            }
        });

        return () => {
            unlistenForReportColumns.then(f => f());
        };
    }, [props.metadata.oid]);

    function loadReportColumns() {
        startReportColumnListTransition(async () => {
            const temp: ReportColumnMetadata[] = [];
            try {
                await queryAsync({
                    reportColumns: {
                        reportOid: props.metadata.oid,
                        channel: new Channel<ReportColumnMetadata>((item) => {
                            temp.push(item);
                        })
                    }
                });
            } catch (e) {
                props.onError(e);
            }
            startReportColumnListTransition(() => {
                setReportColumnList(temp);
            });
        });
    }

    /**
     * Overwrites the report metadata with the inputted information.
     */
    async function editReportAsync(): Promise<boolean> {
        try {
            await executeAsync({
                editReport: {
                    oid: props.metadata.oid,
                    name: reportName,
                    filterFormula: filterFormula ?? null,
                    groupByColumnOids: groupByColumnOids.map((oid) => parseInt(oid)),
                    orderByColumnOids: orderByColumnOids.map((oid) => parseInt(oid))
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
            title="Edit Report"
            tabs={[
                {
                    value: 'general',
                    label: "General",
                    fields: (<>
                        <Form.TextField 
                            label="Report Name" 
                            value={reportName} 
                            onSetValue={setReportName} 
                        />
                    </>)
                },
                {
                    value: 'columns',
                    label: "Columns",
                    fields: (<Form.CustomField
                        label="Columns"
                    >
                        {isReportColumnListPending ? (<Spinner />) : (<div className="w-full grid grid-row grid-cols-[1fr_auto_auto]">
                            {reportColumnList.map((col) => {
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
                },
                {
                    value: 'filtering',
                    label: "Filtering",
                    fields: (<>
                        <Form.FormulaField
                            label="Filter"
                            tooltip="Only data where this formula is true will be included in the report. This filter is applied before aggregation."
                            value={filterFormula}
                            onSetValue={setFilterFormula}
                        />
                    </>)
                },
                {
                    value: 'aggregation',
                    label: "Aggregation",
                    fields: (<>
                        <Form.MultiselectField
                            label="Group By"
                            tooltip="The report will be aggregated by distinct values in these columns."
                            value={groupByColumnOids}
                            onSetValue={setGroupByColumnOids}
                            possibleValues={reportColumnList.map((col) => {
                                return { value: col.oid.toString(), label: col.name, disabled: false };
                            })}
                        />
                    </>)
                },
                {
                    value: 'sorting',
                    label: "Sorting",
                    fields: (<>
                        {orderByColumnOids.map((oid, index) => {
                            <Form.SelectField 
                                key={`report${props.metadata.oid}-orderby${index}`}
                                label={`${index}.`}
                                tooltip={index == 0 ? "The rows of the report will first be sorted by this column." : "After the rows of the report are sorted by the above columns, they will next be sorted by this column."}
                                value={oid}
                                onSetValue={(newOid) => {
                                    if (newOid) {
                                        setOrderByColumnOids((oldOrderByColumnOids) => oldOrderByColumnOids.splice(index, 1, newOid));
                                    } else {
                                        setOrderByColumnOids((oldOrderByColumnOids) => oldOrderByColumnOids.splice(index, 1));
                                    }
                                }}
                                possibleValues={reportColumnList.map((col) => {
                                    const colOid: string = col.oid.toString();
                                    return {
                                        value: colOid,
                                        label: col.name,
                                        disabled: colOid !== oid && orderByColumnOids.indexOf(colOid) >= 0
                                    };
                                })}
                            />
                        })}
                        <Form.SelectField
                            key={`report${props.metadata.oid}-orderby${orderByColumnOids.length}`}
                            label={`${orderByColumnOids.length}.`}
                            value=""
                            onSetValue={(newOid) => {
                                if (newOid) {
                                    setOrderByColumnOids((oldOrderByColumnOids) => oldOrderByColumnOids.concat([newOid]));
                                }
                            }}
                            possibleValues={reportColumnList.map((col) => {
                                const colOid: string = col.oid.toString();
                                return {
                                    value: colOid,
                                    label: col.name,
                                    disabled: orderByColumnOids.indexOf(colOid) >= 0
                                };
                            })}
                        />
                    </>)
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