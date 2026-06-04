import { Cell, CellClipboardData } from "./cell";
import { FullMetadata as ColumnFullMetadata, ColumnType } from "./column";


class GridCell {
    /**
     * The HTMLElement representing the cell in the grid.
     */
    elem: HTMLTableCellElement;

    /**
     * The content of the cell.
     */
    content: Cell;

    constructor(cwd: Document, content: Cell) {
        this.elem = cwd.createElement('td');
        this.content = content;

        // Construct specific HTMLElement based on column type
    }
}

type GridRow = {
    /**
     * The index, listed at the start of the row.
     */
    index: HTMLTableCellElement,

    /**
     * The content of each column.
     */
    cells: {[key: number]: GridCell}
};


export class Grid {
    /**
     * The IFrame containing the grid.
     */
    #iframe: HTMLIFrameElement;

    /**
     * The Document hosted by the IFrame.
     */
    #cwd: Document;

    /**
     * The TBODY of the table.
     */
    #tbody: HTMLElement;


    /**
     * The columns of the grid.
     */
    #columns: {[key: number]: ColumnFullMetadata} = {};

    /**
     * The rows of the grid.
     */
    #rows: GridRow[] = [];

    /**
     * The cells that are currently selected. More than one cell may be selected at a time.
     * A single item in the selectedCells array can be copied to clipboard.
     */
    #selectedCells: {
        topRowIndex: number,
        leftColumnOid: number,
        rowSpan: number,
        columnSpan: number
    }[] = [];

    /**
     * The cell that is currently focused. Only one cell can be focused at a time.
     */
    #focusedCell: {
        rowIndex: number,
        columnOid: number
    } | null = null;

    /**
     * The current mode.
     * The "select" mode allows the user to select cells.
     * The "edit" mode indicates the user is currently editing a cell.
     */
    #mode: 'select' | 'edit' = 'select';

    /**
     * Code blatantly lifted (and slightly modified) from https://github.com/renanlecaro/importabular/blob/master/src/index.js
     * @param columns The columns of the grid.
     */
    constructor(columns: {[key: number]: ColumnFullMetadata}) {
        this.#columns = columns;


        /**
         * First, set up the grid in the DOM.
         */

        // Create IFrame.
        const iframe = document.createElement("iframe");
        this.#iframe = iframe;
        
        // Get the content window document
        const cwd = iframe.contentWindow?.document;
        if (!cwd) throw new Error("Content window document is null or undefined.");
        this.#cwd = cwd;
        
        // Construct HTML head and body
        const html = cwd.createElement('html');
        html.lang = navigator.language;
        const head = cwd.createElement('head');
        html.appendChild(head);
        const style = cwd.createElement('style');
        head.appendChild(style);
        const body = cwd.createElement('body');
        html.appendChild(body);

        // Start constructing the table
        const table = document.createElement("table");
        body.appendChild(table);

        // Construct the headers for each column
        const thead = document.createElement("THEAD");
        const tr = document.createElement("TR");
        thead.appendChild(tr);
        for (const columnOid in this.#columns) {
            const column = this.#columns[columnOid];

            const th = document.createElement("TH");
            const div = document.createElement("div");
            div.innerHTML = column.name;
            th.appendChild(div);
            tr.appendChild(th);

        }
        table.appendChild(thead);

        // Construct the body of the table
        const tbody = document.createElement("tbody");
        table.appendChild(tbody);
        this.#tbody = tbody;

        // Set up event listeners within the IFrame
        for (const eventName in this.#eventListeners) {
            cwd.addEventListener(eventName, this.#eventListeners[eventName], true);
        }
    }


    /**
     * The event listeners for the grid.
     */
    #eventListeners: {[key: string]: (e: any) => void} = {
        "paste": (e) => {
            if (this.#mode == 'edit') return;
            e.preventDefault();

            // window.clipboardData handles clipboard data in IE
            const plaintextData = (e.clipboardData || window.clipboardData).getData("text/plain");
            const pastedCells: CellClipboardData = JSON.parse(plaintextData);

            if ('columnType' in pastedCells) { // User is pasting a single column of data, which can be transposed
                this.#selectedCells.forEach(({ topRowIndex, leftColumnOid }) => {
                    
                });
            } else { // User is pasting multiple columns of data
                this.#selectedCells.forEach(({ topRowIndex }) => {
                    let k: number = 0;
                    while (k < pastedCells.rows.length) {
                        const rowIndex = topRowIndex + k;
                        let row: GridRow;
                        if (rowIndex < this.#rows.length) {
                            row = this.#rows[rowIndex];
                        } else {
                            // Expand with a new row, if that is an option
                            return; // TODO
                        }

                        // Replace the content of each pasted cell in the row
                        Object.entries(pastedCells.rows[k]).forEach(([columnOidStr, pastedCell]) => {
                            const columnOid = Number.parseInt(columnOidStr);
                            if (columnOid in row.cells) {
                                const pastedGridCell: GridCell = new GridCell(this.#cwd, pastedCell);
                                row.cells[columnOid].elem.replaceWith(pastedGridCell.elem);
                                row.cells[columnOid] = pastedGridCell;
                            }
                        });

                        // Increment the row index
                        ++k;
                    }
                });
            }

            const { rx, ry } = this._selection;
            const offset = { x: rx[0], y: ry[0] };

            for (let y = 0; y < rows.length; y++)
            // Using the first column here makes sure that
            // if the paste data had various row length, we only
            // paste a clean rectangle
            for (let x = 0; x < rows[0].length; x++)
            this._setVal(offset.x + x, offset.y + y, rows[y][x]);

            this._changeSelectedCellsStyle(() => {
            this._selectionStart = offset;
            this._selectionEnd = {
            x: offset.x + rows[0].length - 1,
            y: offset.y + rows.length - 1,
            };
            // THis needs to run before rerender
            this._onDataChanged();
            });
        },

        "mousedown": (e) => {

        }
    };
}