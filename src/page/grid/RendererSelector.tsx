import { CellRendererSelectorResult } from 'ag-grid-community';
import { PlainTextCellRenderer } from './renderer/PlainTextCellRenderer';
import { CheckboxCellRenderer } from './renderer/CheckboxCellRenderer';
import { DateCellRenderer } from './renderer/DateCellRenderer';
import { DatetimeCellRenderer } from './renderer/DatetimeCellRenderer';
import { ObjectCellRenderer } from './renderer/ObjectCellRenderer';
import { SchemaCellRenderer } from './renderer/SchemaCellRenderer';
import { TableCell, TableCellContent } from '../../api/model/tableRow';
import { ObjectPageBreadcrumb, ReportPageBreadcrumb } from '../../breadcrumb';

export function selectRenderer(cell: TableCell, onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void, onRequestOpenObject: (object: ObjectPageBreadcrumb) => void,): CellRendererSelectorResult | undefined {
    const content: TableCellContent = cell.content;
    if ('text' in content) {
        switch (content.text.format) {
            default: 
                return {
                    component: PlainTextCellRenderer,
                    params: {
                        deferRender: true,
                    }
                };
        }
    } else if ('boolean' in content) {
        return {
            component: CheckboxCellRenderer,
            params: {
                deferRender: true
            }
        };
    } else if ('date' in content) {
        return {
            component: DateCellRenderer,
            params: {
                deferRender: true
            }
        };
    } else if ('datetime' in content) {
        return {
            component: DatetimeCellRenderer,
            params: {
                deferRender: true
            }
        };
    } else if ('object' in content) {
        return {
            component: ObjectCellRenderer,
            params: {
                deferRender: true,
                onRequestOpenObject
            }
        };
    } else if ('subreport' in content) {
        return {
            component: SchemaCellRenderer,
            params: {
                deferRender: true,
                onRequestOpenSchema: onRequestOpenDrillDownReport
            }
        };
    }

    return undefined;
}