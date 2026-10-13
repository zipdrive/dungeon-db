import { ICellRendererComp, ICellRendererParams } from 'ag-grid-community';
import classNames from 'classnames';
import DOMPurify from 'dompurify';
import { createRoot, Root } from 'react-dom/client';

let nextJsonId: bigint = 1n;

export class JsonTextCellRenderer implements ICellRendererComp {
    private gui: HTMLDivElement;

    constructor() {
        this.gui = document.createElement('div');
        this.gui.id = `json-render-${nextJsonId++}`;
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

            const raw: string = String(params.getValue ? (params.getValue() ?? '') : '');
            try {
                const beautifulJson = JSON.stringify(JSON.parse(raw), null, 2);

                const hiddenPre = document.createElement('pre');
                hiddenPre.className = classNames(
                    'hidden'
                );
                hiddenPre.innerText = beautifulJson;

                const shownPre = document.createElement('pre');
                shownPre.className = classNames(
                    'absolute',
                    'top-0',
                    'left-0',
                    'right-0',
                    'bottom-0'
                );

                // Following code more or less shamelessly stolen from https://gist.github.com/QasimTalkin/0b973e88e77ad2e6233873fdadb72bf7
                const colorifiedJson = beautifulJson
                    .replace(/&/g, '&amp;')
                    .replace(/\\"/g, '&quot;')
                    .replace(/</g, '&lt;')
                    .replace(/>/g, '&gt;')
                    .replace(
                        /^( *)("[\w]+": )?("[^"]*"|[\w.+-]*)?([,[{])?$/mg,
                        (match, pIndent, pKey, pVal, pEnd) => {
                            const key = '<span class=json-key>';
                            const val = '<span class=json-value>';
                            const str = '<span class=json-string>';
                            let r = pIndent || '';
                            if (pKey)
                                r = r + key + pKey.replace(/[": ]/g, '') + '</span>: ';
                            if (pVal)
                                r = r + (pVal[0] == '"' ? str : val) + pVal + '</span>';
                            return r + (pEnd || '');
                        }
                    );
                shownPre.innerHTML = DOMPurify.sanitize(colorifiedJson); // Sanitize, just in case?

                this.gui.appendChild(hiddenPre);
                this.gui.appendChild(shownPre);
            } catch (_e) {
                const pre = document.createElement('pre');
                pre.innerText = raw;
                this.gui.appendChild(pre);
            }
        });
        return true;
    }
}