import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import { createRoot, Root } from 'react-dom/client';

export class PlainTextCellRenderer implements ICellRendererComp {
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
        // Clear prior children
        for (const child of this.gui.childNodes) {
            this.gui.removeChild(child);
        }
        

        // Add <p> element for each paragraph
        for (const paragraph of String(params.getValue ? (params.getValue() ?? '') : '').split('\n')) {
            const p: HTMLParagraphElement = document.createElement('p');
            p.innerText = paragraph;
            this.gui.appendChild(p);
        }
        return true;
    }
}