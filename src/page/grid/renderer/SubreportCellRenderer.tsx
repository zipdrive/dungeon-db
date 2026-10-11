import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import { ReportPageBreadcrumb } from '../../../breadcrumb';
import { SubreportTableCellContent, TableCell } from '../../../api/model/tableRow';
import { getTableColumnMetadataAsync } from '../../../api/query';

type SubreportCellRendererParams = ICellRendererParams<any, any, any> & {
    onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void,
    onError: (e: unknown) => void,
};

export class SubreportCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('flex', 'justify-center', 'h-full');
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

        try {
            // Clear prior children
            for (const child of this.gui.childNodes) {
                this.gui.removeChild(child);
            }
            
            // 
            const link: HTMLAnchorElement = document.createElement('a');
            link.className = classNames(
                'text-[rgb(var(--color-primary)/1)]',
                'cursor-pointer',
                'w-full',
                'h-full',
                'inline-block'
            );
            this.gui.appendChild(link);

            const cell: TableCell<{ subreport: SubreportTableCellContent }> | null = params.getValue ? params.getValue() : null;
            const content: SubreportTableCellContent | null = cell?.content.subreport ?? null;
            // TODO link label?
            //link.innerText = content?.label ?? '';
            link.onclick = async (_e) => {
                if (cell && content) {
                    const columnMetadata = await getTableColumnMetadataAsync(cell.columnOid);
                    onRequestOpenDrillDownReport({
                        key: 'report',
                        name: columnMetadata.name,
                        reportOid: content.reportOid,
                        oidFilters: content.oidFilters
                    });
                }
            };
            return true;
        } catch (e) {
            onError(e);
            return false;
        }
    }
}