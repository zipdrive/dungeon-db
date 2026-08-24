import { useEffect, useState, useTransition } from "react";
import {
  Accordion,
  AccordionBody,
  AccordionHeader,
  Button,
  List,
  ListItem,
  Spinner,
  Typography
} from '@material-tailwind/react';
import { HierarchicalListItemMetadata, queryAsync } from "./api/query";
import { Channel } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

type SchemaHierarchyItem = [HierarchicalListItemMetadata, SchemaHierarchyItem[]];

type SidebarProps = {
    selectedSchemaOid: number | null,
    onSelectSchema: (schemaOid: number, schemaName: string) => void,
    onRequestCreateSchema: (defaultType: 'table' | 'report') => void,
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
            return (<Accordion 
                open={expandedSchemaHierarchies.indexOf(key) >= 0}
                icon={(<img src="/src-tauri/icons/expand_down.png" className={`mx-5 transition-transform ${expandedSchemaHierarchies.indexOf(key) >= 0 ? "rotate-180" : ""}`} />)}
            >
                <AccordionHeader onClick={() => { 
                        const newExpandedSchemaHierarchies = [...expandedSchemaHierarchies];
                        const idx: number = newExpandedSchemaHierarchies.indexOf(key);
                        if (idx < 0) {
                            newExpandedSchemaHierarchies.push(key);
                        } else {
                            newExpandedSchemaHierarchies.splice(idx, 1);
                        }
                        setExpandedSchemaHierarchies(newExpandedSchemaHierarchies);
                    }}
                    className="border-b-0 px-0 py-0"
                >
                    <ListItem 
                        className="rounded-none px-3 py-1 text-xs" 
                        ripple={false}
                        selected={schema.oid == props.selectedSchemaOid} 
                        onClick={() => { props.onSelectSchema(schema.oid, schema.name); }}
                    >
                        <Typography variant="small" className={`indent-${Math.min(30, 3 * schema.level)}`}>{schema.name}</Typography>
                    </ListItem>
                </AccordionHeader>
                <AccordionBody className="px-0 py-0">
                    <List className="px-0 py-0 gap-0">
                    {inheritorSchemas.map(createSchemaHierarchyItemNode)}
                    </List>
                </AccordionBody>
            </Accordion>);
        } else {
            return (<ListItem 
                className="rounded-none px-3 py-1 text-xs" 
                ripple={false}
                selected={schema.oid == props.selectedSchemaOid} 
                onClick={() => { props.onSelectSchema(schema.oid, schema.name); }}
            >
                <Typography variant="small" className={`indent-${Math.min(30, 3 * schema.level)}`}>{schema.name}</Typography>
            </ListItem>);
        }
    }

    return (
        <div className="w-sm border-r-4 border-r-blue-gray-100">
            <Accordion 
                open={isTableSidebarOpen}
                icon={(<img src="/src-tauri/icons/expand_down.png" className={`mx-2 transition-transform ${isTableSidebarOpen ? "rotate-180" : ""}`} />)}
            >
                <AccordionHeader className="h-10" onClick={() => { setIsTableSidebarOpen(!isTableSidebarOpen); }}>
                    <Typography variant="h6" className="px-2">Tables</Typography>
                </AccordionHeader>
                <AccordionBody>
                {
                    isTableListPending ? (<Spinner />) :
                    (<div className="flex flex-col justify-center gap-2">
                        {tableList.length > 0 && <List className="px-0 py-0 gap-0">
                        {tableList.map(createSchemaHierarchyItemNode)}
                        </List>}
                        <Button className="text-center w-fit mx-auto" onClick={() => { props.onRequestCreateSchema('table'); }}>New Table</Button>
                    </div>)
                }
                </AccordionBody>
            </Accordion>
            <Accordion 
                open={isReportSidebarOpen}
                icon={(<img src="/src-tauri/icons/expand_down.png" className={`mx-2 transition-transform ${isReportSidebarOpen ? "rotate-180" : ""}`} />)}
            >
                <AccordionHeader className="h-10" onClick={() => { setIsReportSidebarOpen(!isReportSidebarOpen); }}>
                    <Typography variant="h6" className="px-2">Reports</Typography>
                </AccordionHeader>
                <AccordionBody>
                {
                    isReportListPending ? (<Spinner />) :
                    (<div className="flex flex-col justify-center gap-2">
                        {reportList.length > 0 && <List>
                        {reportList.map(createSchemaHierarchyItemNode)}
                        </List>}
                        <Button className="text-center w-fit mx-auto" onClick={() => { props.onRequestCreateSchema('report'); }}>New Report</Button>
                    </div>)
                }
                </AccordionBody>
            </Accordion>
        </div>
    );
}