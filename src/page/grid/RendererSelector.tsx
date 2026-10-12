import { CellRendererSelectorResult } from 'ag-grid-community';
import { PlainTextCellRenderer } from './renderer/PlainTextCellRenderer';
import { CheckboxCellRenderer } from './renderer/CheckboxCellRenderer';
import { DateCellRenderer } from './renderer/DateCellRenderer';
import { DatetimeCellRenderer } from './renderer/DatetimeCellRenderer';
import { ObjectCellRenderer } from './renderer/ObjectCellRenderer';
import { SubreportCellRenderer } from './renderer/SubreportCellRenderer';
import { TableCell, TableCellContent, TableRowLabel } from '../../api/model/tableRow';
import { ObjectPageBreadcrumb, ReportPageBreadcrumb } from '../../breadcrumb';
import { SingleSelectCellRenderer } from './renderer/SingleSelectCellRenderer';
import { MultiSelectCellRenderer } from './renderer/MultiSelectCellRenderer';
import { ReportCell } from '../../api/model/reportRow';

export function selectRenderer(cell: TableCell | ReportCell, dropdownValues: {[tableOid: number]: TableRowLabel[]}, onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void, onRequestOpenObject: (object: ObjectPageBreadcrumb) => void, onError: (e: unknown) => void): CellRendererSelectorResult | undefined {
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
                onRequestOpenObject,
                onError
            }
        };
    } else if ('singleSelectDropdown' in content) {
        return {
            component: SingleSelectCellRenderer,
            params: {
                deferRender: true,
                dropdownValues,
                onError 
            }
        };
    } else if ('multiSelectDropdown' in content) {
        return {
            component: MultiSelectCellRenderer,
            params: {
                deferRender: true,
                dropdownValues,
                onError 
            }
        };
    } else if ('subreport' in content) {
        return {
            component: SubreportCellRenderer,
            params: {
                deferRender: true,
                onRequestOpenSchema: onRequestOpenDrillDownReport
            }
        };
    }

    return undefined;
}