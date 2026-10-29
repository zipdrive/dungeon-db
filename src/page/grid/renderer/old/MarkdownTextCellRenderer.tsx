import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import { marked } from 'marked';
import DOMPurify from 'dompurify';

let nextMarkdownId: bigint = 1n;

export class MarkdownTextCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.id = `markdown-render-${nextMarkdownId++}`;
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

            // Parse Markdown formatting
            const raw: string = String(params.getValue ? (params.getValue() ?? '') : '');
            const markdownHtml = DOMPurify.sanitize(marked.parse(raw, { async: false }));
            this.gui.innerHTML = markdownHtml;
        });
        return true;
    }
}