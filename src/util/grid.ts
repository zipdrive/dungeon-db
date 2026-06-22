import { message } from "@tauri-apps/plugin-dialog";
import { executeAsync } from "./action";
import { CellContent, ClipboardCellsData, SchemaRow, Cell, isClipboardCellData, AddNewRowButton } from "./cell";
import { FullMetadata as ColumnFullMetadata, ColumnType } from "./column";

/**
 * The index for a cell in the grid.
 */
type GridPosition = {
    rowIndex: number,
    columnIndex: number
};

/**
 * A row of cells in the grid.
 */
class GridRow {
    /**
     * The row's index element, listed at the start of the row.
     */
    index: HTMLTableCellElement;

    /**
     * The content of each column.
     */
    cells: Cell[] = [];

    constructor(cwd: Document, row: SchemaRow) {
        // Construct the index element
        this.index = cwd.createElement('th');
        this.index.innerText = `${row.index}`;
    }
};

/**
 * A column of cells in the grid.
 */
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

        // Do a hot reload of the column metadata
        this.hotReload(metadata);
    }

    /**
     * Attempts to hot reload the column.
     * @param metadata The new metadata for the column.
     * @returns 
     */
    hotReload(metadata: ColumnFullMetadata): boolean {
        if (metadata.columnType == this.metadata.columnType 
            && metadata.hidden == this.metadata.hidden 
            && metadata.ordering == this.metadata.ordering
        ) {
            // Swap out the column DOM class
            for (const c of this.header.ownerDocument.getElementsByClassName(`column${this.metadata.oid}`)) {
                c.classList.remove(`column${this.metadata.oid}`);
                c.classList.add(`column${metadata.oid}`);
            }

            // Set the new column metadata
            this.metadata = metadata;

            // Reload the column name
            this.header.innerText = `${(this.metadata.isPrimaryKey ? '🔑 ' : '')}${this.metadata.name}`;

            // Reload the stylesheet
            this.stylesheet.innerText = `.column${this.metadata.oid} { ${this.metadata.style} }`;
            
            // Full reload is not necessary
            return true;
        } else {
            // Full reload is necessary
            return false;
        }
    }
}


export class Grid {
    /**
     * The IFrame containing the grid.
     */
    #iframe: HTMLIFrameElement;

    /**
     * Retrieves the IFrame containing the grid.
     * @returns 
     */
    getIframe(): HTMLIFrameElement {
        return this.#iframe;
    }


    /**
     * The Document hosted by the IFrame.
     */
    #cwd: Document;


    /**
     * The columns of the grid.
     */
    #columns: GridColumn[] = [];

    /**
     * Adds a column to the grid.
     * @param metadata The column to add.
     */
    addColumn(metadata: ColumnFullMetadata) {
        const column: GridColumn = new GridColumn(this.#cwd, metadata);
        this.#columns.push(column);
    }

    /**
     * The rows of the grid.
     */
    #rows: GridRow[] = [];

    /**
     * Adds a row to the grid.
     * @param row The row to add.
     */
    addRow(row: SchemaRow) {
        this.#rows.push(new GridRow(this.#cwd, row));
    }

    /**
     * Adds cell content to the last row in the grid.
     * @param content The cell content to add.
     */
    addCellContentToRow(content: CellContent, fullReloadCallbackFn: () => Promise<void>) {
        if (this.#rows.length > 0) {
            const lastRow: GridRow = this.#rows[this.#rows.length - 1];
            const cell: Cell = new Cell(this.#cwd, content);

            // Ensure that the cell content is being added at the correct location
            while (this.#columns.length > lastRow.cells.length) {
                const expectedColumnOid: number = this.#columns[lastRow.cells.length].metadata.oid;
                const actualColumnOid: number = cell.cellIdentifier.columnOid;
                if (expectedColumnOid != actualColumnOid) {
                    // Add a dummy cell, then continue
                    lastRow.cells.push(new Cell(
                        this.#cwd, 
                        {
                            readonly: {
                                cellIdentifier: {
                                    columnOid: expectedColumnOid,
                                    queryFilter: '',
                                    isolatedCellDependencies: [],
                                    fullReloadCellDependencies: []
                                },
                                label: null,
                                format: 'plain',
                                validationFailures: []
                            }
                        }
                    ));
                    continue;
                } else {
                    // Add the constructed cell, then break from the loop
                    const pos: GridPosition = {
                        rowIndex: this.#rows.length - 1,
                        columnIndex: lastRow.cells.length
                    };
                    cell.setStartEditingCallback(async () => {    
                        // User can only edit one cell at a time                    
                        if (this.#editing && this.#editing !== pos)
                            await this.#stopEditingAsync();

                        // Mark the position of the cell being edited
                        this.#editing = pos;
                    });
                    cell.setStopEditingCallback(async () => {
                        // Unmark that the cell is being edited
                        if (this.#editing === pos) 
                            this.#editing = null;
                    });

                    lastRow.cells.push(cell);

                    // Start listening for if the cell needs to be updated
                    cell.startListeningForReloadAsync({
                        async hotReloadCallbackFn() {
                            const newCell: Cell = await cell.getReloadedCellAsync();
                            cell.destroy();
                            cell.elem.replaceWith(newCell.elem);
                            lastRow.cells[pos.columnIndex] = newCell;
                        },
                        fullReloadCallbackFn
                    });

                    break;
                }
            }
        }
    }


    /**
     * The HREF to the previous page.
     */
    #prevHref: (() => void) | null = null;

    /**
     * Sets an HREF linking to the previous page.
     * @param href 
     */
    setPrevHref(href: () => void) {
        this.#prevHref = href;
    }

    /**
     * The HREF to the next page.
     */
    #nextHref: (() => void) | null = null;

    /**
     * Sets an HREF linking to the next page.
     * @param href 
     */
    setNextHref(href: () => void) {
        this.#nextHref = href;
    }


    #newRowButton: AddNewRowButton | null = null;

    /**
     * Adds an "Add New Row" button.
     * @param button The button to add.
     */
    addNewRowButton(button: AddNewRowButton) {
        this.#newRowButton = button;
    }


    /**
     * The cells that are currently selected. More than one cell may be selected at a time.
     * A single item in the selectedCells array can be copied to clipboard.
     */
    #selectedCells: {
        rowIndex: number,
        columnIndex: number,
        rowSpan: number,
        columnSpan: number
    }[] = [];

    /**
     * The cell that is currently focused. Only one cell can be focused at a time.
     */
    #focusedCell: GridPosition | null = null;

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
    constructor({ iframe, columns }: { iframe: HTMLIFrameElement, columns?: ColumnFullMetadata[] }) {
        /**
         * First, set up the DOM container.
         */

        // Record the IFrame
        this.#iframe = iframe;
        
        // Get the content window document
        const cwd = iframe.contentDocument;
        if (!cwd) throw new Error("Content window document is null or undefined.");
        this.#cwd = cwd;
        
        // Set the language
        const html: HTMLHtmlElement = cwd.getElementsByTagName('html')[0];
        html.lang = navigator.language;
        
        // Set the primary stylesheet
        const gridStyle = cwd.createElement('link');
        gridStyle.rel = 'stylesheet';
        gridStyle.href = '/src/util/grid.css';
        cwd.head.appendChild(gridStyle);


        /**
         * Construct the headers for the table
         */

        this.#columns = columns?.map(c => new GridColumn(cwd, c)) || [];

        
        /**
         * Start constructing the table
         */

        // Set up event listeners within the IFrame
        for (const eventName in this.#eventListeners) {
            cwd.addEventListener(eventName, this.#eventListeners[eventName], true);
        }
    }

    /**
     * Builds the table, after all columns and cells have been inputted.
     */
    build({ scrollTop, scrollLeft, constructAdditionalColumns }: { scrollTop: number, scrollLeft: number, constructAdditionalColumns: (cwd: Document) => HTMLElement[] }) {
        console.debug(this);

        // Build the table
        const div = document.createElement("div");
        this.#cwd.body.appendChild(div);
        const table = document.createElement("table");
        div.appendChild(table);

        // Build the body of the table
        const tbody = document.createElement("tbody");
        table.appendChild(tbody);

        if (this.#prevHref) {
            // Add "Previous Page" button
            const tr = document.createElement('tr');
            tbody.appendChild(tr);

            const td = document.createElement('td');
            td.colSpan = this.#columns.length + 2;
            td.innerHTML = '<div class="one-line">Go To Previous Page</div>';
            td.classList.add('clickable-text', 'centered-link');
            td.addEventListener('click', this.#prevHref);
            tr.appendChild(td);
        }

        this.#rows.forEach(row => {
            const tr = document.createElement('tr');
            tbody.appendChild(tr);

            tr.appendChild(row.index);
            row.cells.forEach(cell => {
                tr.appendChild(cell.elem);
            });
        });

        if (this.#nextHref) {
            // Add "Next Page" button
            const tr = document.createElement('tr');
            tbody.appendChild(tr);

            const td = document.createElement('td');
            td.colSpan = this.#columns.length + 2;
            td.innerHTML = '<div class="one-line">Go To Next Page</div>';
            td.classList.add('clickable-text', 'centered-link');
            td.addEventListener('click', this.#nextHref);
            tr.appendChild(td);
        }

        // Build the headers for each column
        const thead = document.createElement("THEAD");
        const theadr = document.createElement("TR");
        theadr.innerHTML = '<th></th>';
        thead.appendChild(theadr);
        table.appendChild(thead);

        this.#columns.forEach(column => {
            theadr.appendChild(column.header);
        });
        constructAdditionalColumns(this.#cwd).forEach(header => {
            theadr.appendChild(header);
        })

        // Build the foot of the table
        if (this.#newRowButton) {
            const newRowButton = this.#newRowButton;
            const tfoot = document.createElement("tfoot");
            table.appendChild(tfoot);

            const tfootr = document.createElement("tr");
            tfoot.appendChild(tfootr);

            const tfooth = document.createElement("td");
            tfooth.colSpan = this.#columns.length + 2;
            tfooth.classList.add('clickable-text', 'centered-link');
            tfooth.innerHTML = '<div class="one-line">Add New Row</div>';
            tfooth.addEventListener('click', async () => {
                await executeAsync({
                    createRow: {
                        tableOid: newRowButton.tableOid,
                        rowOid: null,
                        fixedParentDatasource: newRowButton.fixedParentDatasource
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'An error occurred while creating new row.',
                        kind: 'error'
                    });
                });
            });
            tfootr.appendChild(tfooth);
        }

        // Scroll the grid to the specified position
        if (this.#cwd.scrollingElement) {
            this.#cwd.scrollingElement.scrollTop = scrollTop;
            this.#cwd.scrollingElement.scrollLeft = scrollLeft;
        }
    }


    /**
     * Retrieves the cell at a particular position.
     * @param pos The position of the cell in the grid.
     * @returns 
     */
    #getCellByPosition(pos: GridPosition): Cell | undefined {
        if (pos.rowIndex < 0 || pos.columnIndex < 0 || pos.rowIndex >= this.#rows.length) {
            return undefined;
        }
        const row = this.#rows[pos.rowIndex];
        if (pos.columnIndex >= row.cells.length) {
            return undefined;
        }
        return row.cells[pos.columnIndex];
    }

    #getSelectedCellPositions(): { rowIndex: number, pos: GridPosition[] }[] {
        const selectedPos: Set<GridPosition> = new Set();
        this.#selectedCells.forEach((selectedCellRegion) => {
            for (
                let r: number = Math.min(selectedCellRegion.rowIndex, selectedCellRegion.rowIndex + selectedCellRegion.rowSpan);
                r <= Math.max(selectedCellRegion.rowIndex, selectedCellRegion.rowIndex + selectedCellRegion.rowSpan);
                ++r
            ) {
                for (
                    let c: number = Math.min(selectedCellRegion.columnIndex, selectedCellRegion.columnIndex + selectedCellRegion.columnSpan);
                    c <= Math.max(selectedCellRegion.columnIndex, selectedCellRegion.columnIndex + selectedCellRegion.columnSpan);
                    ++r
                ) {
                    selectedPos.add({
                        rowIndex: r,
                        columnIndex: c 
                    });
                }
            }
        });

        // Group each position by its row index
        const groupedSelectedPos: { rowIndex: number, pos: GridPosition[] }[] = [];
        [...selectedPos]
            .sort((lhs, rhs) => {
                if (lhs.rowIndex < rhs.rowIndex) 
                    return -1;
                else if (lhs.rowIndex > rhs.rowIndex)
                    return 1;
                else if (lhs.columnIndex < rhs.columnIndex)
                    return -1;
                else if (lhs.columnIndex > rhs.columnIndex)
                    return 1;
                else 
                    return 0;
            })
            .forEach(pos => {
                if (groupedSelectedPos.length === 0 || groupedSelectedPos[groupedSelectedPos.length - 1].rowIndex !== pos.rowIndex)
                    groupedSelectedPos.push({ rowIndex: pos.rowIndex, pos: [pos]});
                else 
                    groupedSelectedPos[groupedSelectedPos.length - 1].pos.push(pos);
            });
        return groupedSelectedPos;
    }

    /**
     * Iterates over the selected cell positions, calling a callback function for each one.
     * @param callbackFn 
     */
    #forEachSelectedCell(callbackFn: (pos: GridPosition) => void) {
        const coveredPos: Set<GridPosition> = new Set();
        this.#selectedCells.forEach((selectedCellRegion) => {
            for (
                let r: number = Math.min(selectedCellRegion.rowIndex, selectedCellRegion.rowIndex + selectedCellRegion.rowSpan);
                r <= Math.max(selectedCellRegion.rowIndex, selectedCellRegion.rowIndex + selectedCellRegion.rowSpan);
                ++r
            ) {
                for (
                    let c: number = Math.min(selectedCellRegion.columnIndex, selectedCellRegion.columnIndex + selectedCellRegion.columnSpan);
                    c <= Math.max(selectedCellRegion.columnIndex, selectedCellRegion.columnIndex + selectedCellRegion.columnSpan);
                    ++r
                ) {
                    const pos: GridPosition = {
                        rowIndex: r,
                        columnIndex: c 
                    };
                    if (!coveredPos.has(pos)) {
                        coveredPos.add(pos);
                        callbackFn(pos);
                    }
                }
            }
        });
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
        this.#stopEditingAsync();
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


    /**
     * True if the user is currently selecting cells with their mouse.
     */
    #isMouseSelecting: boolean = false;


    /**
     * True if the user is currently editing a cell.
     */
    #editing: GridPosition | null = null;

    /**
     * Begin editing the cell at the given position.
     * @param pos 
     */
    async #startEditingAsync(pos: GridPosition) {
        const cell = this.#getCellByPosition(pos);
        if (cell) {
            await cell.startEditingAsync();
        }
    }

    /**
     * Stop editing the cell currently being edited, and do not push the changes to the database.
     */
    async #revertEditAsync() {
        if (this.#editing) {
            const cell = this.#getCellByPosition(this.#editing);
            if (cell) {
                // TODO
            }
        }
    }

    /**
     * Stop editing the cell currently being edited, and push the changes to the database.
     */
    async #stopEditingAsync() {
        if (this.#editing) {
            const cell = this.#getCellByPosition(this.#editing);
            if (cell) {
                await cell.stopEditingAsync();
            }
        }
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
        "cut": (e: ClipboardEvent) => {
            const groupedSelectedPos = this.#getSelectedCellPositions();
            // Map the rows into nested arrays of clipboard data, then compile into JSON
            const data: ClipboardCellsData = groupedSelectedPos
                .map(({ pos: posInRow }) => 
                    posInRow
                        .map(pos => { 
                            return {
                                columnIndex: pos.columnIndex,
                                cell: this.#getCellByPosition(pos)
                            }; 
                        })
                        .filter(({ cell }) => cell !== undefined)
                        .map(({ columnIndex, cell }) => {
                            return {
                                columnOid: this.#columns[columnIndex].metadata.oid,
                                data: (cell as Cell).clip
                            };
                        })
                );
            const json = JSON.stringify(data);

            // Set stringified selection to clipboard
            e.clipboardData?.setData('text/plain', json);

            // Clear the data from each cell
            groupedSelectedPos.flatMap(({ pos }) => pos).map(pos => this.#getCellByPosition(pos)).forEach(async (cell) => { 
                await cell?.clearAsync();
            });
        },

        "copy": (e: ClipboardEvent) => {
            // Group each position by its row index
            const groupedSelectedPos = this.#getSelectedCellPositions();
            // Map the rows into nested arrays of clipboard data, then compile into JSON
            const data: ClipboardCellsData = groupedSelectedPos
                .map(({ pos: posInRow }) => 
                    posInRow
                        .map(pos => { 
                            return {
                                columnIndex: pos.columnIndex,
                                cell: this.#getCellByPosition(pos)
                            }; 
                        })
                        .filter(({ cell }) => cell !== undefined)
                        .map(({ columnIndex, cell }) => {
                            return {
                                columnOid: this.#columns[columnIndex].metadata.oid,
                                data: (cell as Cell).clip
                            };
                        })
                );
            const json = JSON.stringify(data);

            // Set stringified selection to clipboard
            e.clipboardData?.setData('text/plain', json);
        },

        "paste": (e: ClipboardEvent) => {
            if (this.#mode == 'edit') return;
            e.preventDefault();

            function parsePastedData(plaintextData: string | undefined): ClipboardCellsData {
                if (!plaintextData)
                    return [[{
                        columnOid: 0,
                        value: null
                    }]];

                try {
                    const obj: any = JSON.parse(plaintextData);

                    // Validate the format, then cast
                    if (!Array.isArray(obj) || !obj.every(arr => Array.isArray(arr) && arr.every((item: any) => isClipboardCellData(item))))
                        throw new Error('Pasted object is not in the expected format.');
                    return obj;
                } catch (e) {
                    // Treat as pasting plaintext
                    return [[{
                        columnOid: 0,
                        value: {
                            text: plaintextData
                        }
                    }]];
                }
            }

            // window.clipboardData handles clipboard data in IE
            const plaintextData = e.clipboardData?.getData("text/plain");
            const pastedRows: ClipboardCellsData = parsePastedData(plaintextData);
            
            const groupedSelectedPos = this.#getSelectedCellPositions();
            let pastedRowIndex: number = 0;
            groupedSelectedPos.forEach(({ pos: posInRow }) => {
                const pastedRow = pastedRows[pastedRowIndex];
                // Increment the index of the row to paste, looping if necessary
                pastedRowIndex = ++pastedRowIndex % pastedRows.length;
            });
        },

        "keydown": (e: KeyboardEvent) => {
            if ((e.ctrlKey && e.key !== "ArrowDown" && e.key !== "ArrowUp" && e.key !== "ArrowLeft" && e.key !== "ArrowRight") || e.metaKey) return;

            if (this.#selectedCells.length > 0) {
                if (e.key === "Escape" && this.#mode == 'edit') {
                    e.preventDefault();
                    this.#revertEditAsync();
                    this.#stopEditingAsync();
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
                        this.#startEditingAsync(this.#focusedCell);
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
        },

        "mousedown": (e: MouseEvent) => {
            if (e.button === 0) { // Main mouse button pressed

            }
        },
        "mouseenter": (e: MouseEvent) => {
            if (this.#isMouseSelecting) {

            }
        },
    };
}