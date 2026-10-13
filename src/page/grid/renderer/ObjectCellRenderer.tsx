import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import { ObjectPageBreadcrumb } from '../../../breadcrumb';
import { ObjectTableCellContent, TableCell } from '../../../api/model/tableRow';
import { getObjectLabelAsync, getReportColumnMetadataAsync, getTableColumnMetadataAsync } from '../../../api/query';
import { TableColumnMetadata } from '../../../api/model/tableColumn';
import { executeAsync } from '../../../api/action';
import { ReportCell, TableCellReference } from '../../../api/model/reportRow';
import { ReportColumnMetadata } from '../../../api/model/reportColumn';

type ObjectCellRendererParams = ICellRendererParams<any, any, any> & {
    onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

export class ObjectCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('flex', 'justify-center', 'h-full');
    }

    init(params: ObjectCellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: ObjectCellRendererParams): boolean {
        const onRequestOpenObject: (object: ObjectPageBreadcrumb) => void = params.onRequestOpenObject;
        const onError: (e: unknown) => void = params.onError;

        try {
            // Clear prior children
            for (const child of this.gui.childNodes) {
                this.gui.removeChild(child);
            }

            // Get the cell and the Object content
            const cell: TableCell<{ object: ObjectTableCellContent }> | ReportCell<{ object: ObjectTableCellContent }> | null = params.getValue ? params.getValue() : null;
            const content: ObjectTableCellContent | null = cell?.content.object ?? null;
            
            if (cell && content?.value) {
                const columnOid: number = cell.columnOid;
                const objectRowOid: number = content.value;

                // Create a link to the Object page
                const link: HTMLAnchorElement = document.createElement('a');
                link.className = classNames(
                    'text-[rgb(var(--color-primary)/1)]',
                    'absolute',
                    'left-0',
                    'right-0',
                    'top-0',
                    'bottom-0',
                    'cursor-pointer'
                );
                this.gui.appendChild(link);

                // Set label of Object
                getObjectLabelAsync(content.tableOid, content.value)
                    .then((label) => {
                        link.innerText = label;
                    })
                    .catch(onError);
                
                link.onclick = async (_e) => {
                    try {
                        const columnMetadata: TableColumnMetadata | ReportColumnMetadata = 'reference' in cell
                            ? await getReportColumnMetadataAsync(columnOid)
                            : await getTableColumnMetadataAsync(columnOid);
                        onRequestOpenObject({
                            page: 'object',
                            name: columnMetadata.name,
                            tableOid: content.tableOid,
                            rowOid: objectRowOid
                        });
                    } catch (e) {
                        onError(e);
                    }
                };
            } else if (cell && content) {
                const reference: TableCellReference | null = 'reference' in cell 
                    ? cell.reference 
                    : { tableOid: cell.tableOid, columnOid: cell.columnOid, rowOid: cell.rowOid }; 

                if (reference !== null) {
                    // Create a Button that will make a new Object
                    const button: HTMLButtonElement = document.createElement('button');
                    button.innerText = 'New';
                    button.className = classNames([
                        'inline-flex items-center justify-center border align-middle select-none font-sans font-medium text-center transition-all duration-300 ease-in disabled:opacity-50 disabled:shadow-none disabled:cursor-not-allowed data-[shape=pill]:rounded-full data-[width=full]:w-full focus:shadow-none text-sm rounded-md py-2 px-4 shadow-sm hover:shadow-md bg-gradient-to-tr from-primary-dark to-primary-light border-primary text-primary-foreground hover:brightness-105 cursor-pointer'
                    ]);
                    this.gui.appendChild(button);

                    // The Button creates a new Object when clicked
                    button.addEventListener('click', async (_e) => {
                        try {
                            await executeAsync({
                                createObject: {
                                    tableOid: reference.tableOid,
                                    columnOid: reference.columnOid,
                                    rowOid: reference.rowOid,
                                    objectTableOid: content.tableOid,
                                }
                            });
                        } catch (e) {
                            onError(e);
                        }
                    });
                }
            }
            
            return true;
        } catch (e) {
            onError(e);
            return false;
        }
    }
}