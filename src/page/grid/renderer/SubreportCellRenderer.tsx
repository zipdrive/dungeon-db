import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import { ReportPageBreadcrumb } from '../../../breadcrumb';
import { SubreportTableCellContent, TableCell } from '../../../api/model/tableRow';
import { getDrillDownReportLabelAsync, getReportColumnMetadataAsync, getTableColumnMetadataAsync } from '../../../api/query';
import { ReportCell } from '../../../api/model/reportRow';
import { TableColumnMetadata } from '../../../api/model/tableColumn';
import { ReportColumnMetadata } from '../../../api/model/reportColumn';

type SubreportCellRendererParams = ICellRendererParams<any, any, any> & {
    onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

export class SubreportCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('flex');
    }

    init(params: SubreportCellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: SubreportCellRendererParams): boolean {
        const onRequestOpenDrillDownReport = params.onRequestOpenDrillDownReport;
        const onError: (e: unknown) => void = params.onError;
        if (!onError)
            return false;

        try {
            // Clear prior children
            for (const child of this.gui.childNodes) {
                this.gui.removeChild(child);
            }
            
            // Get the cell and the Drill-Down Report content
            const cell: TableCell<{ subreport: SubreportTableCellContent }> | ReportCell<{ subreport: SubreportTableCellContent }> | null = params.getValue ? params.getValue() : null;
            const content: SubreportTableCellContent | null = cell?.content.subreport ?? null;

            if (cell && content) {
                // Create a link to the Drill-Down Report page
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

                // Set label of Drill-Down Report
                getDrillDownReportLabelAsync(content.reportOid, content.oidFilters)
                    .then((label) => {
                        link.innerText = label;
                    })
                    .catch(onError);
                
                link.onclick = async (_e) => {
                    try {
                        const columnOid: number = cell.columnOid;
                        const columnMetadata: TableColumnMetadata | ReportColumnMetadata = 'reference' in cell
                            ? await getReportColumnMetadataAsync(columnOid)
                            : await getTableColumnMetadataAsync(columnOid);
                        onRequestOpenDrillDownReport({
                            page: 'report',
                            name: columnMetadata.name,
                            reportOid: content.reportOid,
                            oidFilters: content.oidFilters
                        });
                    } catch (e) {
                        onError(e);
                    }
                };
            }
            return true;
        } catch (e) {
            onError(e);
            return false;
        }
    }
}