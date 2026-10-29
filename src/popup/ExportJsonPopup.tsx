import { useCallback, useEffect, useState, useTransition } from "react";
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
import { ReportListItem } from "../api/model/report";
import { exportJsonAsync } from "../api/export";

export type ExportJsonPopupProps = {
    onClosePopup: () => void,
    onError: (e: unknown) => void,
};

export function ExportJsonPopup(props: ExportJsonPopupProps): React.JSX.Element {
    const [tableList, setTableList] = useState<{ value: string, label: string, disabled?: boolean }[]>([]);
    const [isTableListPending, startTableListTransition] = useTransition();
    const [reportList, setReportList] = useState<{ value: string, label: string, disabled?: boolean }[]>([]);
    const [isReportListPending, startReportListTransition] = useTransition();

    const [selectedTables, setSelectedTables] = useState<string[]>([]);
    const [selectedReports, setSelectedReports] = useState<string[]>([]);
    const [typeColumn, setTypeColumn] = useState<string>('$type');
    const [tableConfigs, setTableConfigs] = useState<{[tableOid: string]: { oidColumn: string, indexColumn: string, isPolymorphic: boolean }}>({});
    const [reportConfigs, setReportConfigs] = useState<{[reportOid: string]: { indexColumn: string }}>({});

    /**
     * Loads a basic list of tables.
     */
    const loadTables = useCallback(() => {
        startTableListTransition(async () => {
            const temp: TableListItem[] = [];
            try {
                await queryAsync({
                    tables: {
                        channel: new Channel<TableListItem>((item) => {
                            temp.push(item);
                        })
                    }
                });
            } catch (e) {
                props.onError(e);
            }
            startTableListTransition(() => {
                setTableList(temp.map(({ oid, name }) => {
                    return {
                        value: oid.toString(),
                        label: name,
                        disabled: false
                    };
                }));
            });
        });
    }, []);

    /**
     * Loads a basic list of reports.
     */
    const loadReports = useCallback(() => {
        startReportListTransition(async () => {
            const temp: ReportListItem[] = [];
            try {
                await queryAsync({
                    reports: {
                        channel: new Channel<ReportListItem>((item) => {
                            temp.push(item);
                        })
                    }
                });
            } catch (e) {
                props.onError(e);
            }
            startReportListTransition(() => {
                setReportList(temp.map(({ oid, name }) => {
                    return {
                        value: oid.toString(),
                        label: name,
                        disabled: false 
                    };
                }));
            });
        });
    }, []);

    // Load tables on boot and on subsequent updates to any table
    useEffect(() => {
        loadTables();
        
        const unlistenForTables = listen<TableListItem>('table', () => {
            loadTables();
        });

        return () => {
            unlistenForTables.then(f => f());
        };
    }, [loadTables]);

    // Load reports on boot and on subsequent updates to any report
    useEffect(() => {
        loadReports();

        const unlistenForReports = listen<number>('report', () => {
            loadReports();
        });

        return () => {
            unlistenForReports.then(f => f());
        };
    }, [loadReports]);

    // Reset tables, reports, and selection when reset signal is received
    useEffect(() => {
        const unlistenForReset = listen<null>('reset', () => {
            loadTables();
            loadReports();
        });

        return () => {
            unlistenForReset.then(f => f());
        };
    }, [loadTables, loadReports]);

    /**
     * Begins the export operation in the background.
     */
    const beginExport = useCallback(() => {
        exportJsonAsync(
            [
                ...selectedTables.map((tableOid) => {
                    const config = tableOid in tableConfigs ? tableConfigs[tableOid] : {
                        oidColumn: '',
                        indexColumn: '',
                        isPolymorphic: false 
                    };
                    return {
                        table: {
                            tableOid: parseInt(tableOid),
                            tableName: tableList.find(({ value }) => value === tableOid)?.label ?? '',
                            oidColumn: config.oidColumn || null,
                            indexColumn: config.indexColumn || null,
                            polymorphism: (config.isPolymorphic ? 'yes' : 'no') as 'yes' | 'no'
                        }
                    };
                }),
                ...selectedReports.map((reportOid) => {
                    return {
                        report: {
                            reportOid: parseInt(reportOid),
                            reportName: reportList.find(({ value }) => value === reportOid)?.label ?? '',
                            indexColumn: (reportOid in reportConfigs ? reportConfigs[reportOid].indexColumn : null) || null
                        }
                    };
                })
            ],
            typeColumn
        );
        props.onClosePopup();
    }, [props.onClosePopup, selectedTables, selectedReports, typeColumn, tableConfigs, reportConfigs]);

    return (<div className="flex flex-col gap-y-6">
        <Form 
            title="Export to JSON"
            tabs={[
                {
                    value: 'schemas',
                    label: 'Export Schemas',
                    fields: (<>
                        {isTableListPending ? (
                            <Form.CustomField label="Tables" tooltip="Exports all rows of these tables."><Spinner /></Form.CustomField>
                        ) : (tableList.length == 0 ? (
                            <Form.CustomField label="Tables" tooltip="Exports all rows of these tables.">
                                <Typography variant="small" className="text-center">
                                    No tables available to export.
                                </Typography>
                            </Form.CustomField>
                        ) : (<Form.MultiselectField 
                            label="Tables" 
                            tooltip="Exports all rows of these tables."
                            possibleValues={tableList}
                            value={selectedTables} 
                            onSetValue={setSelectedTables} 
                        />))}
                        {isReportListPending ? (
                            <Form.CustomField label="Reports" tooltip="Exports all rows of these reports."><Spinner /></Form.CustomField>
                        ) : (reportList.length == 0 ? (
                            <Form.CustomField label="Reports" tooltip="Exports all rows of these reports.">
                                <Typography variant="small" className="text-center">
                                    No reports available to export.
                                </Typography>
                            </Form.CustomField>
                        ) : (<Form.MultiselectField 
                            label="Reports" 
                            tooltip="Exports all rows of these reports."
                            possibleValues={reportList}
                            value={selectedReports} 
                            onSetValue={setSelectedReports} 
                        />))}
                        <Form.TextField
                            label="Subtype Column Name"
                            tooltip="If specified, adds a column to each exported Object containing the name of that object's subtype."
                            value={typeColumn}
                            onSetValue={setTypeColumn}
                        />
                    </>)
                }
            ]
            .concat(selectedTables.map((tableOid) => {
                const { label } = tableList.find(({ value }) => value === tableOid) ?? { label: '' };
                return {
                    value: `table${tableOid}`,
                    label,
                    fields: (<>
                        <Form.TextField
                            label="Row ID Column"
                            tooltip="If specified, exports the unique ID for each row. This ID is how Single-Select Dropdowns and Multi-Select Dropdowns reference tables."
                            value={(tableOid in tableConfigs ? tableConfigs[tableOid].oidColumn : null) ?? ''}
                            onSetValue={(value) => {
                                setTableConfigs((oldTableConfigs) => {
                                    return {
                                        ...oldTableConfigs,
                                        [tableOid]: tableOid in oldTableConfigs ? {
                                            ...oldTableConfigs[tableOid],
                                            oidColumn: value
                                        } : {
                                            oidColumn: value,
                                            indexColumn: '',
                                            isPolymorphic: false
                                        }
                                    };
                                });
                            }}
                        />
                        <Form.TextField
                            label="Index Column"
                            tooltip="If specified, exports a unique index for each row."
                            value={(tableOid in tableConfigs ? tableConfigs[tableOid].indexColumn : null) ?? ''}
                            onSetValue={(value) => {
                                setTableConfigs((oldTableConfigs) => {
                                    return {
                                        ...oldTableConfigs,
                                        [tableOid]: tableOid in oldTableConfigs ? {
                                            ...oldTableConfigs[tableOid],
                                            indexColumn: value
                                        } : {
                                            oidColumn: '',
                                            indexColumn: value,
                                            isPolymorphic: false
                                        }
                                    };
                                });
                            }}
                        />
                        <Form.CheckboxField
                            label="Export as Object?"
                            tooltip="For each row of the table, the export also includes any additional columns from subtypes."
                            value={(tableOid in tableConfigs ? tableConfigs[tableOid].isPolymorphic : null) ?? false}
                            onSetValue={(value) => {
                                setTableConfigs((oldTableConfigs) => {
                                    return {
                                        ...oldTableConfigs,
                                        [tableOid]: tableOid in oldTableConfigs ? {
                                            ...oldTableConfigs[tableOid],
                                            isPolymorphic: value
                                        } : {
                                            oidColumn: '',
                                            indexColumn: '',
                                            isPolymorphic: value
                                        }
                                    };
                                });
                            }}
                        />
                    </>)
                };
            }))
            .concat(selectedReports.map((reportOid) => {
                const { label } = reportList.find(({ value }) => value === reportOid) ?? { label: '' };
                return {
                    value: `report${reportOid}`,
                    label,
                    fields: (<>
                        <Form.TextField
                            label="Index Column"
                            tooltip="If specified, exports a unique index for each row."
                            value={(reportOid in reportConfigs ? reportConfigs[reportOid].indexColumn : null) ?? ''}
                            onSetValue={(value) => {
                                setTableConfigs((oldReportConfigs) => {
                                    return {
                                        ...oldReportConfigs,
                                        [reportOid]: reportOid in oldReportConfigs ? {
                                            ...oldReportConfigs[reportOid],
                                            indexColumn: value
                                        } : {
                                            oidColumn: '',
                                            indexColumn: value,
                                            isPolymorphic: false
                                        }
                                    };
                                });
                            }}
                        />
                    </>)
                };
            }))
            }
        />
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
                onClick={beginExport}
            >
                <span>Export</span>
            </Button>
        </div>
    </div>);
}