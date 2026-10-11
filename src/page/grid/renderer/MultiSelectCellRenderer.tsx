import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import { MultiSelectDropdownTableCellContent, SingleSelectDropdownTableCellContent, TableCell, TableRowLabel } from '../../../api/model/tableRow';

type MultiSelectCellRendererParams = ICellRendererParams<any, any, any> & {
    dropdownValues: {[tableOid: number]: TableRowLabel[]},
    onError: (e: unknown) => void,
};

export class MultiSelectCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('flex', 'justify-center', 'h-full');
    }

    init(params: MultiSelectCellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: MultiSelectCellRendererParams): boolean {
        const onError: (e: unknown) => void = params.onError;

        try {
            this.gui.innerText = '';

            // Get the cell and the Multi-Select Dropdown content
            const cell: TableCell<{ multiSelectDropdown: MultiSelectDropdownTableCellContent }> | null = params.getValue ? params.getValue() : null;
            const content: MultiSelectDropdownTableCellContent | null = cell?.content.multiSelectDropdown ?? null;

            if (content && content.value && content.tableOid in params.dropdownValues) {
                const dropdownValues = params.dropdownValues[content.tableOid];

                /**
                 * Gets the label corresponding to a row OID.
                 */
                function getLabel(value: number): string | null {
                    // Do binary search to find row label
                    let minIndex: number = 0;
                    let maxIndex: number = dropdownValues.length - 1;
                    while (maxIndex >= minIndex) {
                        const index: number = Math.floor(0.5 * (minIndex + maxIndex));
                        if (dropdownValues[index].oid < value) {
                            minIndex = index + 1;
                        } else if (dropdownValues[index].oid > value) {
                            maxIndex = index - 1;
                        } else {
                            return dropdownValues[index].label;
                        }
                    }
                    return null;
                }
                
                this.gui.innerText = content.value.map(getLabel).filter((label) => label !== null).join('\n');
            }
            return true;
        } catch (e) {
            onError(e);
            return false;
        }
    }
}