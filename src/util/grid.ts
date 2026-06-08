import { CellContent, CellClipboardData } from "./cell";
import { FullMetadata as ColumnFullMetadata, ColumnType } from "./column";

/**
 * The index for a cell in the grid.
 */
type GridCellIndex = {
    rowIndex: number,
    columnIndex: number
};

/**
 * A cell in the grid.
 */
class GridCell {
    /**
     * The HTMLElement representing the cell in the grid.
     */
    elem: HTMLTableCellElement;

    /**
     * The content of the cell.
     */
    content: CellContent;

    constructor(cwd: Document, content: CellContent) {
        this.elem = cwd.createElement('td');
        this.content = content;

        // Construct specific HTMLElement based on column type
    }
}

class GridRow {
    /**
     * The row's index element, listed at the start of the row.
     */
    index: HTMLTableCellElement;

    /**
     * The content of each column.
     */
    cells: GridCell[] = [];
};

class GridColumn {
    /**
     * The column's header element, listed at the head of the column.
     */
    header: HTMLElement;

    /**
     * The column's metadata.
     */
    metadata: ColumnFullMetadata;

    /**
     * The stylesheet specific to this column.
     */
    stylesheet: HTMLStyleElement;

    constructor(cwd: Document, metadata: ColumnFullMetadata) {
        this.metadata = metadata;

        // Construct the header element
        this.header = cwd.createElement('th');
        this.header.classList.add('header', `column${metadata.oid}`);

        // Insert the stylesheet
        this.stylesheet = cwd.createElement('style');
        this.stylesheet.id = `column${metadata.oid}-stylesheet`;
        cwd.head.appendChild(this.stylesheet);
    }

    /**
     * Reloads the column.
     */
    reload() {
        // Reload the column name
        this.header.innerText = `${(this.metadata.isPrimaryKey ? '🔑 ' : '')}${this.metadata.name}`;

        // Reload the stylesheet
        this.stylesheet.innerText = `.column${this.metadata.oid} { ${this.metadata.style} }`;
    }
}


export type GridOptions = {
    transposed?: boolean
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
    #columns: GridColumn[] = [];

    /**
     * The rows of the grid.
     */
    #rows: GridRow[] = [];

    /**
     * True if the columns go down vertically and the rows go right horizontally.
     * False if the columns go right horizontally and the rows go down vertically.
     */
    #transposed: boolean;


    /**
     * The cells that are currently selected. More than one cell may be selected at a time.
     * A single item in the selectedCells array can be copied to clipboard.
     */
    #selectedCells: {
        rowIndex: number,
        columnOid: number,
        rowSpan: number,
        columnSpan: number
    }[] = [];

    /**
     * The cell that is currently focused. Only one cell can be focused at a time.
     */
    #focusedCell: GridCellIndex | null = null;

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
    constructor(columns: ColumnFullMetadata[], options: GridOptions) {
        const gridOptions = Object.assign({
            transposed: false
        }, options);
        this.#transposed = gridOptions.transposed;

        /**
         * First, set up the DOM container.
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


        /**
         * Construct the headers for the table
         */

        this.#columns = columns.map(c => new GridColumn(cwd, c));

        
        /**
         * Start constructing the table
         */ 

        const table = document.createElement("table");
        body.appendChild(table);

        // Construct the headers for each column
        const thead = document.createElement("THEAD");
        const tr = document.createElement("TR");
        thead.appendChild(tr);
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


    #moveCursor(magnitude: { rowSpanShift?: number, columnSpanShift?: number }, shiftKey: boolean) {
        const lastSelectedRegion = this.#selectedCells.pop();
        if (lastSelectedRegion) {
            if (shiftKey) {
                // Move the span of the last selection region
                if (magnitude.rowSpanShift) {
                    lastSelectedRegion.rowSpan += Math.min(
                        Math.max(magnitude.rowSpanShift, -(lastSelectedRegion.rowIndex + lastSelectedRegion.rowSpan)), 
                        this.#rows.length - 1 - (lastSelectedRegion.rowIndex + lastSelectedRegion.rowSpan)
                    );
                }
                if (magnitude.columnSpanShift) {
                    const columns: ColumnFullMetadata[] = Object.values(this.#columns).sort(c => c.ordering);
                    const columnIndex: number = Math.max(0, columns.findIndex(c => c.oid == lastSelectedRegion.columnOid));
                    lastSelectedRegion.columnSpan += Math.min(
                        Math.max(magnitude.columnSpanShift, -(columnIndex + lastSelectedRegion.columnSpan)), 
                        columns.length - 1 - (columnIndex + lastSelectedRegion.columnSpan)
                    );
                }
            } else {
                // Move the last selection region's focal point
                if (magnitude.rowSpanShift) {
                    lastSelectedRegion.rowIndex = Math.min(
                        Math.max(lastSelectedRegion.rowIndex + magnitude.rowSpanShift, 0),
                        this.#rows.length - 1
                    );
                }
                if (magnitude.columnSpanShift) {
                    const columns: ColumnFullMetadata[] = Object.values(this.#columns).sort(c => c.ordering);
                    const oldColumnIndex: number = Math.max(0, columns.findIndex(c => c.oid == lastSelectedRegion.columnOid));
                    const newColumnIndex: number = Math.min(
                        Math.max(oldColumnIndex + magnitude.columnSpanShift, 0),
                        columns.length - 1
                    );
                    lastSelectedRegion.columnOid = columns[newColumnIndex].oid;
                }

                // Drop row span and column span
                lastSelectedRegion.rowSpan = 0;
                lastSelectedRegion.columnSpan = 0;
            }

            // Dump everything but the last selection region
            this.#selectedCells = [lastSelectedRegion];
        }

        const curr = shiftSelectionEnd ? this._selectionEnd : this._selectionStart;
        const nc = { x: curr.x + magnitude.x, y: curr.y + magnitude.y };
        if (!this._fitBounds(nc)) return;
        this.#stopEditing();
        this._incrementToFit(nc);
        this._changeSelectedCellsStyle(() => {
            if (shiftSelectionEnd) {
                this._selectionEnd = nc;
            } else {
                this._selectionStart = this._selectionEnd = this._focus = nc;
            }
        });
        this._scrollIntoView(nc);
    }

    #tabCursor() {
        
    }


    #startEditing() {

    }

    #revertEdit() {

    }

    #stopEditing() {

    }

    /**
     * Clears the content of the cells that are currently selected.
     */
    #clearSelectedCells() {
        this.#selectedCells.forEach((selectedCellRegion) => {

        });
    }


    /**
     * The event listeners for the grid.
     */
    #eventListeners: {[key: string]: (e: any) => void} = {
        "paste": (e: ClipboardEvent) => {
            if (this.#mode == 'edit') return;
            e.preventDefault();

            // window.clipboardData handles clipboard data in IE
            const plaintextData = (e.clipboardData || window.clipboardData).getData("text/plain");
            const pastedCells: CellClipboardData = JSON.parse(plaintextData);

            if ('columnType' in pastedCells) { // User is pasting a single column of data, which can be transposed
                this.#selectedCells.forEach(({ rowIndex: topRowIndex, columnOid: leftColumnOid }) => {
                    
                });
            } else { // User is pasting multiple columns of data
                this.#selectedCells.forEach(({ rowIndex: topRowIndex }) => {
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

        "keydown": (e: KeyboardEvent) => {
            if ((e.ctrlKey && e.key !== "ArrowDown" && e.key !== "ArrowUp" && e.key !== "ArrowLeft" && e.key !== "ArrowRight") || e.metaKey) return;

            if (this.#selectedCells.length > 0) {
                if (e.key === "Escape" && this.#mode == 'edit') {
                    e.preventDefault();
                    this.#revertEdit();
                    this.#stopEditing();
                }
                if (e.key === "Enter") {
                    e.preventDefault();
                    this._tabCursorInSelection(false, e.shiftKey ? -1 : 1);
                }

                if (e.key === "Tab") {
                    e.preventDefault();
                    this._tabCursorInSelection(true, e.shiftKey ? -1 : 1);
                }
                if (this.#mode == 'select') {
                    if (e.key === "F2") {
                        e.preventDefault();
                        this._startEditing(this._focus);
                    }
                    if (e.key === "Delete" || e.key === "Backspace") {
                        e.preventDefault();
                        this.#clearSelectedCells();
                    }

                    if (e.key === "ArrowDown") {
                        e.preventDefault();
                        if (e.ctrlKey) {
                            // Move to the last row
                            this.#moveCursor({ rowSpanShift: Number.MAX_SAFE_INTEGER }, e.shiftKey);
                        } else {
                            // Move down by a single row
                            this.#moveCursor({ rowSpanShift: +1 }, e.shiftKey);
                        }
                    }
                    if (e.key === "ArrowUp") {
                        e.preventDefault();
                        if (e.ctrlKey) {
                            // Move to the first row
                            this.#moveCursor({ rowSpanShift: Number.MIN_SAFE_INTEGER }, e.shiftKey);
                        } else {
                            // Move up by a single row
                            this.#moveCursor({ rowSpanShift: -1 }, e.shiftKey);
                        }
                    }
                    if (e.key === "ArrowLeft") {
                        e.preventDefault();
                        if (e.ctrlKey) {
                            // Move to the first column
                            this.#moveCursor({ columnSpanShift: Number.MIN_SAFE_INTEGER }, e.shiftKey);
                        } else {
                            // Move left by a single column
                            this.#moveCursor({ columnSpanShift: -1 }, e.shiftKey);
                        }
                    }
                    if (e.key === "ArrowRight") {
                        e.preventDefault();
                        if (e.ctrlKey) {
                            // Move to the last column
                            this.#moveCursor({ columnSpanShift: Number.MAX_SAFE_INTEGER }, e.shiftKey);
                        } else {
                            // Move right by a single column
                            this.#moveCursor({ columnSpanShift: +1 }, e.shiftKey);
                        }
                    }
                }

                if (e.key.length === 1 && this.#mode != 'edit') {
                    this._changeSelectedCellsStyle(() => {
                        const { x, y } = this._focus;
                        // We clear the value of the cell, and the keyup event will
                        // happen with the cursor inside the cell and type the character there
                        this._startEditing({ x, y });
                        this._getCell(x, y).firstChild.value = "";
                    });
                }
            }
        }

        "mousedown": (e: MouseEvent) => {

        }
    };
}