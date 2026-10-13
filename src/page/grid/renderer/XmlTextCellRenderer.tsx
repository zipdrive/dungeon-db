import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import { createRoot, Root } from 'react-dom/client';

let nextXmlId: bigint = 1n;

export class XmlTextCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.id = `xml-render-${nextXmlId++}`;
    }

    init(params: ICellRendererParams) {
        this.refresh(params);
    }

    getGui(): HTMLElement {
        return this.gui;
    }

    refresh(params: ICellRendererParams<any, any, any>): boolean {
        navigator.locks.request(this.gui.id, () => {
            // Clear prior children
            for (const child of this.gui.childNodes) {
                this.gui.removeChild(child);
            }
            
            const pre = document.createElement('pre');
            pre.innerText = String(params.getValue ? (params.getValue() ?? '') : '');
            this.gui.appendChild(pre);
        });
        return true;
    }
}