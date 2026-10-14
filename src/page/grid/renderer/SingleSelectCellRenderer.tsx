import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import { SingleSelectDropdownTableCellContent, TableCell, TableRowLabel } from '../../../api/model/tableRow';
import { ReportCell } from '../../../api/model/reportRow';

type SingleSelectCellRendererParams = ICellRendererParams<any, any, any> & {
    dropdownValues: {[tableOid: number]: TableRowLabel[]},
    onError: (e: unknown) => void,
};

export class SingleSelectCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('flex', 'justify-center', 'h-full');
    }

    init(params: SingleSelectCellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: SingleSelectCellRendererParams): boolean {
        const onError: (e: unknown) => void = params.onError;
        if (!onError)
            return false;

        try {
            this.gui.innerText = '';

            // Get the cell and the Single-Select Dropdown content
            const cell: TableCell<{ singleSelectDropdown: SingleSelectDropdownTableCellContent }> | ReportCell<{ singleSelectDropdown: SingleSelectDropdownTableCellContent }> | null = params.getValue ? params.getValue() : null;
            const content: SingleSelectDropdownTableCellContent | null = cell?.content.singleSelectDropdown ?? null;

            if (content && content.value && content.tableOid in params.dropdownValues) {
                const dropdownValues = params.dropdownValues[content.tableOid];

                // Do binary search to find row label
                let minIndex: number = 0;
                let maxIndex: number = dropdownValues.length - 1;
                while (maxIndex >= minIndex) {
                    const index: number = Math.floor(0.5 * (minIndex + maxIndex));
                    if (dropdownValues[index].oid < content.value) {
                        minIndex = index + 1;
                    } else if (dropdownValues[index].oid > content.value) {
                        maxIndex = index - 1;
                    } else {
                        this.gui.innerText = dropdownValues[index].label;
                        break;
                    }
                }
            }
            return true;
        } catch (e) {
            onError(e);
            return false;
        }
    }
}