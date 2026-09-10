import { useEffect, useState, useTransition } from "react";
import {
  Accordion,
  Button,
  Spinner,
  Typography
} from '@material-tailwind/react';
import { getSchemaMetadataAsync, HierarchicalListItemMetadata, queryAsync } from "./api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import expandImageSrc from './assets/expand_down.png';
import { Menu } from "@tauri-apps/api/menu";
import { Schema } from "./api/model/schema";
import { executeAsync } from "./api/action";

type SchemaHierarchyItem = [HierarchicalListItemMetadata, SchemaHierarchyItem[]];

type SidebarProps = {
    selectedSchemaOid: number | null,
    onSelectSchema: (schemaOid: number, schemaName: string) => void,
    onRequestCreateSchema: (defaultType: 'table' | 'report') => void,
    onRequestEditSchema: (schema: Schema) => void,
    onError: (e: unknown) => void,
};

export function Sidebar(props: SidebarProps): React.JSX.Element {
    const [isTableSidebarOpen, setIsTableSidebarOpen] = useState<boolean>(true);
    const [isReportSidebarOpen, setIsReportSidebarOpen] = useState<boolean>(false);
    const [expandedSchemaHierarchies, setExpandedSchemaHierarchies] = useState<string[]>([]);
    const [tableList, setTableList] = useState<SchemaHierarchyItem[]>([]);
    const [isTableListPending, startTableListTransition] = useTransition();
    const [reportList, setReportList] = useState<SchemaHierarchyItem[]>([]);
    const [isReportListPending, startReportListTransition] = useTransition();

    useEffect(() => {
        loadTables();
        loadReports();

        const unlistenForSchemas = listen<number>('schema', () => {
            loadTables();
            loadReports();
        });

        return () => {
            unlistenForSchemas.then(f => f());
        };
    }, []);

    /**
     * Loads a basic list of tables.
     */
    function loadTables() {
        startTableListTransition(async () => {
            console.log('Reloading tables...');
            const temp: SchemaHierarchyItem[] = [];
            await queryAsync({
                tables: {
                    channel: new Channel<HierarchicalListItemMetadata>((item) => {
                        console.log(item);
                        let prevItem: SchemaHierarchyItem | undefined = temp.length > 0 ? temp[temp.length - 1] : undefined;
                        while (prevItem) {
                            if (prevItem[0].oid == item.masterOid) {
                                prevItem[1].push([item, []]);
                                return;
                            } else {
                                prevItem = prevItem[1].length > 0 ? prevItem[1][prevItem[1].length - 1] : undefined;
                            }
                        }
                        temp.push([item, []]);
                    })
                }
            });
            console.log('Finished reloading tables.');
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
            const temp: SchemaHierarchyItem[] = [];
            await queryAsync({
                reports: {
                    channel: new Channel<HierarchicalListItemMetadata>((item) => {
                        console.log(item);
                        let prevItem: SchemaHierarchyItem | undefined = temp.length > 0 ? temp[temp.length - 1] : undefined;
                        while (prevItem) {
                            if (prevItem[0].oid == item.masterOid) {
                                prevItem[1].push([item, []]);
                                return;
                            } else {
                                prevItem = prevItem[1].length > 0 ? prevItem[1][prevItem[1].length - 1] : undefined;
                            }
                        }
                        temp.push([item, []]);
                    })
                }
            });
            startReportListTransition(() => {
                setReportList(temp);
            });
        });
    }

    /**
     * Creates a recursive, expandable hierarchy of nodes to represent the hierarchy of schemas.
     */
    function createSchemaHierarchyItemNode(item: SchemaHierarchyItem): React.JSX.Element {
        const [schema, inheritorSchemas] = item;
        if (inheritorSchemas.length > 0) {
            const key: string = `${schema.oid}<${schema.masterOid}`;
            const expandedSchemaIndex: number = expandedSchemaHierarchies.indexOf(key);
            return (<div className="p-0">
                <div 
                    className={'grid grid-cols-[1fr_40px] items-center w-full' + (props.selectedSchemaOid == schema.oid ? ' bg-(--color-secondary-dark)' : '')}
                    onClick={() => { 
                        if (expandedSchemaIndex >= 0) {
                            setExpandedSchemaHierarchies(expandedSchemaHierarchies.slice(0, expandedSchemaIndex).concat(expandedSchemaHierarchies.slice(expandedSchemaIndex + 1)));
                        } else {
                            setExpandedSchemaHierarchies(expandedSchemaHierarchies.concat([key]));
                        }
                    }}
                >
                    <Typography 
                        className={`px-4 pt-1 indent-${Math.min(40, 4 * schema.level)} text-[rgb(var(--color-secondary-foreground)/1)]`}
                        onClick={() => {
                            props.onSelectSchema(schema.oid, schema.name);
                        }}
                    >
                        {schema.name}
                    </Typography>
                    <img src={expandImageSrc} className={"px-2 transition-transform" + (expandedSchemaIndex >= 0 ? ' rotate-180' : '')} />
                </div>
                {expandedSchemaIndex >= 0 && (<div className="flex flex-col">
                    {inheritorSchemas.map(createSchemaHierarchyItemNode)}
                </div>)}
            </div>);
        } else {
            return (<Typography 
                className={`px-4 py-1 indent-${Math.min(40, 4 * schema.level)} ${(props.selectedSchemaOid == schema.oid ? 'bg-[rgb(var(--color-secondary)/1)] border-y-1 border-y-[rgb(var(--color-secondary-dark)/1)]' : '')} text-[rgb(var(--color-secondary-foreground)/1)]`}
                onClick={() => {
                    props.onSelectSchema(schema.oid, schema.name);
                }}
                onContextMenu={async () => {
                    const menu = await Menu.new({
                        items: [
                            {
                                text: "Edit",
                                action: async () => {
                                    const fullSchema = await getSchemaMetadataAsync(schema.oid);
                                    props.onRequestEditSchema(fullSchema);
                                }
                            },
                            {
                                text: "Delete",
                                action: async () => {
                                    try {
                                        await executeAsync({
                                            trashSchema: schema.oid
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
                {schema.name}
            </Typography>);
        }
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
                            {tableList.map(createSchemaHierarchyItemNode)}
                            </div>}
                            <Button 
                                variant="gradient" 
                                className="text-center w-fit mx-auto cursor-pointer" 
                                onClick={() => { props.onRequestCreateSchema('table'); }}
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
                                {reportList.map(createSchemaHierarchyItemNode)}
                            </div>)}
                            <Button 
                                variant="gradient" 
                                className="text-center w-fit mx-auto cursor-pointer" 
                                onClick={() => { props.onRequestCreateSchema('report'); }}
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