import { CellRendererSelectorResult } from 'ag-grid-community';
import { PlainTextCellRenderer } from './renderer/old/PlainTextCellRenderer';
import { CheckboxCellRenderer } from './renderer/old/CheckboxCellRenderer';
import { DateCellRenderer } from './renderer/old/DateCellRenderer';
import { DatetimeCellRenderer } from './renderer/old/DatetimeCellRenderer';
import { ObjectCellRenderer } from './renderer/old/ObjectCellRenderer';
import { SubreportCellRenderer } from './renderer/old/SubreportCellRenderer';
import { TableCell, TableCellContent, TableRowLabel } from '../../api/model/tableRow';
import { ObjectPageBreadcrumb, ReportPageBreadcrumb } from '../../breadcrumb';
import { SingleSelectCellRenderer } from './renderer/old/SingleSelectCellRenderer';
import { MultiSelectCellRenderer } from './renderer/old/MultiSelectCellRenderer';
import { ReportCell } from '../../api/model/reportRow';
import { MarkdownTextCellRenderer } from './renderer/old/MarkdownTextCellRenderer';
import { JsonTextCellRenderer } from './renderer/old/JsonTextCellRenderer';
import { XmlTextCellRenderer } from './renderer/old/XmlTextCellRenderer';
import { BBCodeTextCellRenderer } from './renderer/old/BBCodeTextCellRenderer';

export function selectRenderer(cell: TableCell | ReportCell, dropdownValues: {[tableOid: number]: TableRowLabel[]}, onRequestOpenDrillDownReport: (report: ReportPageBreadcrumb) => void, onRequestOpenObject: (object: ObjectPageBreadcrumb) => void, onError: (e: unknown) => void): CellRendererSelectorResult | undefined {
    const content: TableCellContent = cell.content;
    if ('text' in content) {
        switch (content.text.format) {
            case 'Json':
                return {
                    component: JsonTextCellRenderer,
                    params: {
                        deferRender: true 
                    }
                };
            case 'Xml':
                return {
                    component: XmlTextCellRenderer,
                    params: {
                        deferRender: true 
                    }
                };
            case 'Markdown':
                return {
                    component: MarkdownTextCellRenderer,
                    params: {
                        deferRender: true
                    }
                };
            case 'BBCode':
                return {
                    component: BBCodeTextCellRenderer,
                    params: {
                        deferRender: true 
                    }
                };
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
                onRequestOpenDrillDownReport,
                onError
            }
        };
    }

    return undefined;
}