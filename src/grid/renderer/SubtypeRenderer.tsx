import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';

export class SubtypeRenderer implements ICellRendererComp {
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

    refresh(params: ICellRendererParams): boolean {
        this.gui.innerText = params.data.inheritorTables.find(({ value }: { value: number }) => value == params.data.subtypeTableOid)?.label ?? '';
        return true;
    }
}