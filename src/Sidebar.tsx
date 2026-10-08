import { useEffect, useState, useTransition } from "react";
import {
  Accordion,
  Button,
  Spinner,
  Typography
} from '@material-tailwind/react';
import { getReportMetadataAsync, getTableMetadataAsync, queryAsync } from "./api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import expandImageSrc from './assets/expand_down.png';
import { Menu } from "@tauri-apps/api/menu";
import { executeAsync } from "./api/action";
import { TableListItem, TableMetadata } from "./api/model/table";
import { ReportListItem, ReportMetadata } from "./api/model/report";

type SidebarProps = {
    selectedSchema: ['table' | 'report', number] | null,
    onSelectTable: (tableOid: number, tableName: string) => void,
    onSelectReport: (reportOid: number, reportName: string) => void,
    onRequestCreateTable: () => void,
    onRequestEditTable: (table: TableMetadata) => void,
    onRequestCreateReport: () => void,
    onRequestEditReport: (report: ReportMetadata) => void,
    onError: (e: unknown) => void,
};

export function Sidebar(props: SidebarProps): React.JSX.Element {
    const [isTableSidebarOpen, setIsTableSidebarOpen] = useState<boolean>(true);
    const [isReportSidebarOpen, setIsReportSidebarOpen] = useState<boolean>(false);
    const [tableList, setTableList] = useState<TableListItem[]>([]);
    const [isTableListPending, startTableListTransition] = useTransition();
    const [reportList, setReportList] = useState<ReportListItem[]>([]);
    const [isReportListPending, startReportListTransition] = useTransition();

    useEffect(() => {
        loadTables();
        loadReports();

        const unlistenForTables = listen<number[]>('table', () => {
            loadTables();
        });
        const unlistenForReports = listen<number>('report', () => {
            loadReports();
        });

        return () => {
            unlistenForTables.then(f => f());
            unlistenForReports.then(f => f());
        };
    }, []);

    /**
     * Loads a basic list of tables.
     */
    function loadTables() {
        startTableListTransition(async () => {
            const temp: TableListItem[] = [];
            await queryAsync({
                tables: {
                    channel: new Channel<TableListItem>((item) => {
                        temp.push(item);
                    })
                }
            });
            startTableListTransition(() => {
                setTableList(temp);
            });
        });
    }

    /**
     * Loads a basic list of reports.
     */
    function loadReports() {
        startReportListTransition(async () => {
            const temp: ReportListItem[] = [];
            await queryAsync({
                reports: {
                    channel: new Channel<ReportListItem>((item) => {
                        temp.push(item);
                    })
                }
            });
            startReportListTransition(() => {
                setReportList(temp);
            });
        });
    }

    return (
        <div className="w-sm py-4 border-r-2 border-r-[rgb(var(--color-primary)/1)] bg-[rgb(var(--color-secondary-light)/1)] flex flex-col gap-y-6 overflow-y-auto">
            <div>
                <div onClick={() => { setIsTableSidebarOpen(!isTableSidebarOpen); }} className="grid grid-cols-[1fr_40px] items-center w-full h-10 border-b-1 border-b-[rgb(var(--color-secondary-dark)/1)]">
                    <Typography type="h6" className="px-2 text-[rgb(var(--color-secondary-foreground)/1)]">Tables</Typography>
                    <img src={expandImageSrc} className={"px-2 transition-transform" + (isTableSidebarOpen ? ' rotate-180' : '')} />
                </div>
                {isTableSidebarOpen && <div>
                    {isTableListPending ? (<div className="flex flex-row justify-center py-2"><Spinner /></div>) :
                        (<div className="flex flex-col justify-center gap-2 py-2">
                            {tableList.length > 0 && <div className="flex flex-col py-1">
                                {tableList.map((item) => (<Typography
                                    key={`table-${item.oid}`}
                                    className={`px-4 py-1 ${(props.selectedSchema !== null && props.selectedSchema[0] === 'table' && props.selectedSchema[1] == item.oid ? 'bg-[rgb(var(--color-secondary)/1)] border-y-1 border-y-[rgb(var(--color-secondary-dark)/1)]' : '')} text-[rgb(var(--color-secondary-foreground)/1)]`}
                                    onClick={() => {
                                        props.onSelectTable(item.oid, item.name);
                                    }}
                                    onContextMenu={async () => {
                                        const menu = await Menu.new({
                                            items: [
                                                {
                                                    text: "Edit",
                                                    action: async () => {
                                                        const table = await getTableMetadataAsync(item.oid);
                                                        props.onRequestEditTable(table);
                                                    }
                                                },
                                                {
                                                    text: "Delete",
                                                    action: async () => {
                                                        try {
                                                            await executeAsync({
                                                                trashTable: item.oid
                                                            });
                                                        } catch (e) {
                                                            props.onError(e);
                                                        }
                                                    }
                                                }
                                            ]
                                        });
                                        menu.popup();
                                    }}
                                >
                                    {item.name}
                                </Typography>))}
                            </div>}
                            <Button 
                                variant="gradient" 
                                className="text-center w-fit mx-auto cursor-pointer" 
                                onClick={() => { props.onRequestCreateTable(); }}
                            >
                                New Table
                            </Button>
                        </div>)
                    }
                </div>}
            </div>
            <div>
                <div onClick={() => { setIsReportSidebarOpen(!isReportSidebarOpen); }} className="grid grid-cols-[1fr_40px] items-center w-full h-10 border-b-1 border-b-[rgb(var(--color-secondary-dark)/1)]">
                    <Typography type="h6" className="px-2 text-[rgb(var(--color-secondary-foreground)/1)]">Reports</Typography>
                    <img src={expandImageSrc} className={"px-2 transition-transform" + (isReportSidebarOpen ? ' rotate-180' : '')} />
                </div>
                {isReportSidebarOpen && <div>
                    {isReportListPending ? (<div className="flex flex-row justify-center py-2"><Spinner /></div>) :
                        (<div className="flex flex-col justify-center gap-2 py-2">
                            {reportList.length > 0 && (<div className="flex flex-col">
                                {reportList.map((item) => (<Typography
                                    key={`table-${item.oid}`}
                                    className={`px-4 py-1 ${(props.selectedSchema !== null && props.selectedSchema[0] === 'report' && props.selectedSchema[1] == item.oid ? 'bg-[rgb(var(--color-secondary)/1)] border-y-1 border-y-[rgb(var(--color-secondary-dark)/1)]' : '')} text-[rgb(var(--color-secondary-foreground)/1)]`}
                                    onClick={() => {
                                        props.onSelectReport(item.oid, item.name);
                                    }}
                                    onContextMenu={async () => {
                                        const menu = await Menu.new({
                                            items: [
                                                {
                                                    text: "Edit",
                                                    action: async () => {
                                                        const report = await getReportMetadataAsync(item.oid);
                                                        props.onRequestEditReport(report);
                                                    }
                                                },
                                                {
                                                    text: "Delete",
                                                    action: async () => {
                                                        try {
                                                            await executeAsync({
                                                                trashReport: item.oid
                                                            });
                                                        } catch (e) {
                                                            props.onError(e);
                                                        }
                                                    }
                                                }
                                            ]
                                        });
                                        menu.popup();
                                    }}
                                >
                                    {item.name}
                                </Typography>))}
                            </div>)}
                            <Button 
                                variant="gradient" 
                                className="text-center w-fit mx-auto cursor-pointer" 
                                onClick={() => { props.onRequestCreateReport(); }}
                            >
                                New Report
                            </Button>
                        </div>)
                    }
                </div>}
            </div>
        </div>
    );
}