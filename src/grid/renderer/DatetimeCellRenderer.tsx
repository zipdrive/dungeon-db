import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';

export class DatetimeCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
    }

    init(params: ICellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: ICellRendererParams<any, any, any>): boolean {
        this.gui.innerText = params.getValue ? ((params.getValue() as Date | null)?.toLocaleString() ?? '') : '';
        return true;
    }
}