import { DungeonGridCell } from "dungeon-grid-react";
import { TableCell, TableCellContent, TableRowLabel } from "../../api/model/tableRow";
import { executeAsync } from "../../api/action";
import { ObjectPageBreadcrumb, ReportPageBreadcrumb } from "../../breadcrumb";
import { TableCellReference } from "../../api/model/reportRow";
import { Chip } from "@material-tailwind/react";
import AsyncSelect from 'react-select/async';
import { SingleSelectDropdownCellEditor } from "./editor/SingleSelectDropdownCellEditor";
import { SingleSelectDropdownCellRenderer } from "./renderer/SingleSelectDropdownCellRenderer";
import { getFileAsync, getTableColumnMetadataAsync } from "../../api/query";
import { TableColumnMetadata } from "../../api/model/tableColumn";
import { ReportColumnMetadata } from "../../api/model/reportColumn";
import { BBCodeTextCellRenderer } from "./renderer/BBCodeTextCellRenderer";
import { MarkdownTextCellRenderer } from "./renderer/MarkdownTextCellRenderer";
import { JsonTextCellRenderer } from "./renderer/JsonTextCellRenderer";
import { XmlTextCellRenderer } from "./renderer/XmlTextCellRenderer";
import { SubreportCellRenderer } from "./renderer/SubreportCellRenderer";
import { File } from "../../api/model/file";
import { MultiSelectDropdownCellRenderer } from "./renderer/MultiSelectDropdownCellRenderer";
import { MultiSelectDropdownCellEditor } from "./editor/MultiSelectDropdownCellEditor";
import { ObjectCellRenderer } from "./renderer/ObjectCellRenderer";

export function cellEntry(
    columnMetadata: TableColumnMetadata | ReportColumnMetadata,
    content: TableCellContent,
    cellRef: TableCellReference | null, 
    dropdownValues: {[tableOid: number]: Promise<TableRowLabel[]>},
    onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void,
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onRequestUploadFile: (absolutePath: string, relativePath: string, onUploadFile: (fileOid: number) => Promise<any>) => void,
    onError: (e: unknown) => void
): DungeonGridCell {
    if ('text' in content) {
        const { format: currentFormat, value: currentValue } = content.text;
        const onCommit = cellRef ? async (value: string) => {
            try {
                await executeAsync({
                    editTableCellContents: {
                        ...cellRef,
                        content: {
                            text: {
                                format: currentFormat,
                                value: value || null
                            }
                        }
                    }
                });
            } catch (e) {
                onError(e);
            }
        } : undefined;
        const onClear = cellRef ? async () => {
            try {
                await executeAsync({
                    editTableCellContents: {
                        ...cellRef,
                        content: {
                            text: {
                                format: currentFormat,
                                value: null
                            }
                        }
                    }
                });
            } catch (e) {
                onError(e);
            }
        } : undefined;
        const onCopy = () => currentValue ?? '';

        switch (currentFormat) {
            case 'Plain':
                return {
                    className: `column${columnMetadata.oid}`,
                    label: currentValue ?? '',
                    editor: 'text',
                    onCommit,
                    onPaste: onCommit,
                    onClear,
                    onCopy
                };
            case 'Json':
                return {
                    label: <JsonTextCellRenderer value={currentValue} />,
                    editor: 'text',
                    editorValue: currentValue || '',
                    onCommit,
                    onPaste: onCommit,
                    onClear,
                    onCopy
                };
            case 'Xml':
                return {
                    label: <XmlTextCellRenderer value={currentValue} />,
                    editor: 'text',
                    editorValue: currentValue || '',
                    onCommit,
                    onPaste: onCommit,
                    onClear,
                    onCopy
                };
            case 'Markdown':
                return {
                    label: <MarkdownTextCellRenderer value={currentValue} />,
                    editor: 'text',
                    editorValue: currentValue || '',
                    onCommit,
                    onPaste: onCommit,
                    onClear,
                    onCopy
                };
            case 'BBCode':
                return {
                    label: <BBCodeTextCellRenderer value={currentValue} />,
                    editor: 'text',
                    editorValue: currentValue || '',
                    onCommit,
                    onPaste: onCommit,
                    onClear,
                    onCopy
                };
        }
    } else if ('number' in content) {
        // Number cells
        const currentValue: number | null = content.number.value;
        const onCommit = cellRef ? async (value: string) => {
            const num = parseFloat(value);
            try {
                await executeAsync({
                    editTableCellContents: {
                        ...cellRef,
                        content: {
                            number: {
                                value: Number.isNaN(num) ? null : num 
                            }
                        }
                    }
                });
            } catch (e) {
                onError(e);
            }
        } : undefined;
        return {
            label: currentValue?.toString() || '',
            editor: 'text',
            onCommit,
            onPaste: onCommit,
            onClear: cellRef ? async () => {
                try {
                    await executeAsync({
                        editTableCellContents: {
                            ...cellRef,
                            content: {
                                number: {
                                    value: null 
                                }
                            }
                        }
                    });
                } catch (e) {
                    onError(e);
                }
            } : undefined,
            onCopy: () => {
                return currentValue?.toString() || '';
            }
        };
    } else if ('integer' in content) {
        // Integer cells
        const currentValue: number | null = content.integer.value;
        const onCommit = cellRef ? async (value: string) => {
            const num = parseInt(value);
            try {
                await executeAsync({
                    editTableCellContents: {
                        ...cellRef,
                        content: {
                            integer: {
                                value: Number.isNaN(num) ? null : num 
                            }
                        }
                    }
                });
            } catch (e) {
                onError(e);
            }
        } : undefined;
        return {
            label: currentValue?.toString() || '',
            editor: 'text',
            onCommit,
            onPaste: onCommit,
            onClear: cellRef ? async () => {
                try {
                    await executeAsync({
                        editTableCellContents: {
                            ...cellRef,
                            content: {
                                integer: {
                                    value: null 
                                }
                            }
                        }
                    });
                } catch (e) {
                    onError(e);
                }
            } : undefined,
            onCopy: () => {
                return currentValue?.toString() || '';
            }
        };
    } else if ('boolean' in content) {
        const currentValue: boolean = content.boolean.value;
        return {
            editor: 'checkbox',
            value: content.boolean.value,
            onCommit: cellRef ? async (value) => {
                try {
                    await executeAsync({
                        editTableCellContents: {
                            ...cellRef,
                            content: {
                                boolean: {
                                    value
                                }
                            }
                        }
                    });
                } catch (e) {
                    onError(e);
                }
            } : undefined,
            onClear: cellRef ? async () => {
                try {
                    await executeAsync({
                        editTableCellContents: {
                            ...cellRef,
                            content: {
                                boolean: {
                                    value: false
                                }
                            }
                        }
                    });
                } catch (e) {
                    onError(e);
                }
            } : undefined,
            onCopy: () => {
                return currentValue ? 'TRUE' : 'FALSE';
            },
            onPaste: cellRef ? async (value) => {
                try {
                    await executeAsync({
                        editTableCellContents: {
                            ...cellRef,
                            content: {
                                boolean: {
                                    value: value === 'TRUE'
                                }
                            }
                        }
                    });
                } catch (e) {
                    onError(e);
                }
            } : undefined
        };
    } else if ('date' in content) {
        const date: Date | null = content.date.label ? new Date(Date.parse(content.date.label)) : null;
        const dateText: string = date ? `${date.getFullYear().toString().padStart(4, '0')}-${date.getMonth().toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')}` : '';
        const onCommit = cellRef ? async (value: string) => {
            try {
                await executeAsync({
                    editTableCellContents: {
                        ...cellRef,
                        content: {
                            date: {
                                value: null,
                                label: value
                            }
                        }
                    }
                });
            } catch (e) {
                onError(e);
            }
        } : undefined;
        return {
            label: dateText,
            editor: 'date',
            onCommit,
            onClear: cellRef ? async () => {
                try {
                    await executeAsync({
                        editTableCellContents: {
                            ...cellRef,
                            content: {
                                date: {
                                    value: null,
                                    label: null
                                }
                            }
                        }
                    });
                } catch (e) {
                    onError(e);
                }
            } : undefined,
            onCopy: () => dateText,
            onPaste: onCommit,
        };
    } else if ('datetime' in content) {
        const date: Date | null = content.datetime.label ? new Date(Date.parse(content.datetime.label)) : null;
        const dateText: string = date ? date.toISOString() : '';
        const onCommit = cellRef ? async (value: string) => {
            try {
                await executeAsync({
                    editTableCellContents: {
                        ...cellRef,
                        content: {
                            date: {
                                value: null,
                                label: value
                            }
                        }
                    }
                });
            } catch (e) {
                onError(e);
            }
        } : undefined;
        return {
            label: dateText,
            editor: 'datetime',
            onCommit,
            onClear: cellRef ? async () => {
                try {
                    await executeAsync({
                        editTableCellContents: {
                            ...cellRef,
                            content: {
                                date: {
                                    value: null,
                                    label: null
                                }
                            }
                        }
                    });
                } catch (e) {
                    onError(e);
                }
            } : undefined,
            onCopy: () => dateText,
            onPaste: onCommit,
        };
    } else if ('file' in content) {
        return {
            // TODO label
            label: content.file.value !== null ? ('path' in content.file.value ? content.file.value.path.name : content.file.value.blob.name) : '',
            editor: null,
            onClear: cellRef ? async () => {
                try {
                    await executeAsync({
                        editTableCellContents: {
                            ...cellRef,
                            content: {
                                file: {
                                    value: null
                                }
                            }
                        }
                    });
                } catch (e) {
                    onError(e);
                }
            } : undefined,
            onCopy: () => content.file.value !== null ? `FILE${('path' in content.file.value ? content.file.value.path.oid : content.file.value.blob.oid)}` : '',
            onPaste: cellRef ? async (value) => {
                try {
                    const match = /^FILE(\d+)$/i.exec(value);
                    if (match) {
                        // Set cell contents to the pasted file
                        const fileOid: number = parseInt(match[1]);
                        const file: File = await getFileAsync(fileOid);
                        await executeAsync({
                            editTableCellContents: {
                                ...cellRef,
                                content: {
                                    file: {
                                        value: file 
                                    }
                                }
                            }
                        });
                    } else {
                        // Set cell contents to null instead
                        await executeAsync({
                            editTableCellContents: {
                                ...cellRef,
                                content: {
                                    file: {
                                        value: null 
                                    }
                                }
                            }
                        });
                    }
                } catch (e) {
                    onError(e);
                }
            } : undefined
        };
    } else if ('object' in content) {
        return {
            label: <ObjectCellRenderer 
                tableOid={content.object.tableOid} 
                value={content.object.value} 
                onError={onError}
            />,
            className: "text-[rgb(var(--color-primary)/1)] !cursor-pointer flex justify-center",
            editor: null,
            onClear: cellRef ? async () => {
                try {
                    await executeAsync({
                        editTableCellContents: {
                            ...cellRef,
                            content: {
                                object: {
                                    tableOid: content.object.tableOid,
                                    value: null,
                                    label: null
                                }
                            }
                        }
                    });
                } catch (e) {
                    onError(e);
                }
            } : undefined,
            onCopy: () => content.object.value !== null ? `TABLE${content.object.tableOid}ROW${content.object.value}` : '',
            onPaste: cellRef ? async (value) => {
                try {
                    const match = /^TABLE(\d+)ROW(\d+)$/i.exec(value);
                    if (match) {
                        const tableOid: number = parseInt(match[1]);
                        const rowOid: number = parseInt(match[2]);
                        // TODO
                    }
                } catch (e) {
                    onError(e);
                }
            } : undefined,
            onClick: () => {
                if (content.object.value) {
                    onRequestOpenObject({
                        page: 'object',
                        name: columnMetadata.name,
                        tableOid: content.object.tableOid,
                        rowOid: content.object.value
                    });
                }
            }
        };
    } else if ('singleSelectDropdown' in content) {
        //const currentLabel: string | null = content.singleSelectDropdown.tableOid in dropdownValues ? dropdownValues[content.singleSelectDropdown.tableOid].find(({ oid }) => oid == content.singleSelectDropdown.value)?.label ?? null;
        if (cellRef) {
            return {
                editor: (props) => <SingleSelectDropdownCellEditor
                    tableOid={content.singleSelectDropdown.tableOid}
                    value={content.singleSelectDropdown.value}
                    cellRef={cellRef}
                    items={dropdownValues[content.singleSelectDropdown.tableOid]}
                    onError={onError}
                    {...props}
                />,
                typeToStartEditing: false,
                label: <SingleSelectDropdownCellRenderer
                    value={content.singleSelectDropdown.value}
                    items={dropdownValues[content.singleSelectDropdown.tableOid]}
                />,
                onClear: async () => {
                    try {
                        await executeAsync({
                            editTableCellContents: {
                                ...cellRef,
                                content: {
                                    singleSelectDropdown: {
                                        tableOid: content.singleSelectDropdown.tableOid,
                                        value: null 
                                    }
                                }
                            }
                        });
                    } catch (e) {
                        onError(e);
                    }
                },
                onCopy: () => content.singleSelectDropdown.value !== null ? `TABLE${content.singleSelectDropdown.tableOid}ROW${content.singleSelectDropdown.value}` : '',
                onPaste: cellRef ? async (value) => {
                    try {
                        const match = /^TABLE(\d+)ROW(\d+)$/i.exec(value);
                        if (match) {
                            const tableOid: number = parseInt(match[1]);
                            const rowOid: number = parseInt(match[2]);
                            // TODO
                        }
                    } catch (e) {
                        onError(e);
                    }
                } : undefined
            }
        } else {
            return {
                label: <SingleSelectDropdownCellRenderer
                    value={content.singleSelectDropdown.value}
                    items={dropdownValues[content.singleSelectDropdown.tableOid]}
                />,
                editor: null,
                onCopy: () => content.singleSelectDropdown.value !== null ? `TABLE${content.singleSelectDropdown.tableOid}ROW${content.singleSelectDropdown.value}` : '',
            };
        }
    } else if ('multiSelectDropdown' in content) {
        if (cellRef) {
            return {
                editor: (props) => <MultiSelectDropdownCellEditor
                    tableOid={content.multiSelectDropdown.tableOid}
                    value={content.multiSelectDropdown.value}
                    cellRef={cellRef}
                    items={dropdownValues[content.multiSelectDropdown.tableOid]}
                    onError={onError}
                    {...props}
                />,
                typeToStartEditing: false,
                label: <MultiSelectDropdownCellRenderer
                    value={content.multiSelectDropdown.value}
                    items={dropdownValues[content.multiSelectDropdown.tableOid]}
                />,
                onClear: async () => {
                    try {
                        await executeAsync({
                            editTableCellContents: {
                                ...cellRef,
                                content: {
                                    multiSelectDropdown: {
                                        tableOid: content.multiSelectDropdown.tableOid,
                                        value: [] 
                                    }
                                }
                            }
                        });
                    } catch (e) {
                        onError(e);
                    }
                },
                onCopy: () => content.multiSelectDropdown.value.map((rowOid) => `TABLE${content.multiSelectDropdown.tableOid}ROW${rowOid}`).join(';'),
                onPaste: cellRef ? async (value) => {
                    try {
                        const regex = /^TABLE(\d+)ROW(\d+)(;TABLE\d+ROW\d+)*$/i;
                        let match: RegExpExecArray | null;
                        while (match = regex.exec(value)) {
                            const tableOid: number = parseInt(match[1]);
                            const rowOid: number = parseInt(match[2]);
                            // TODO
                        }
                    } catch (e) {
                        onError(e);
                    }
                } : undefined
            }
        } else {
            return {
                label: <MultiSelectDropdownCellRenderer
                    value={content.multiSelectDropdown.value}
                    items={dropdownValues[content.multiSelectDropdown.tableOid]}
                />,
                editor: null,
                onCopy: () => content.multiSelectDropdown.value.map((rowOid) => `TABLE${content.multiSelectDropdown.tableOid}ROW${rowOid}`).join(';'),
            };
        }
    } else if ('subreport' in content) {
        return {
            label: <SubreportCellRenderer 
                reportOid={content.subreport.reportOid}
                oidFilters={content.subreport.oidFilters}
                onError={onError}
            />,
            editor: null,
            className: "text-[rgb(var(--color-primary)/1)] !cursor-pointer flex justify-center",
            onClick: () => {
                onRequestOpenDrillDownReport({
                    page: 'report',
                    name: columnMetadata.name,
                    reportOid: content.subreport.reportOid,
                    oidFilters: content.subreport.oidFilters
                });
            }
        };
    }
    // Unknown cell content type
    return {
        editor: null 
    };
}