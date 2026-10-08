import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import { createRoot, Root } from 'react-dom/client';

export class CheckboxCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;
    private input: HTMLInputElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.className = classNames('flex', 'justify-center');

        this.input = document.createElement('input');
        this.input.type = 'checkbox';
        this.input.className = classNames("");
        this.gui.appendChild(this.input);
    }

    init(params: ICellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: ICellRendererParams<any, any, any>): boolean {
        return true;
    }
}