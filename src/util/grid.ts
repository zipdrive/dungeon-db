import { message } from "@tauri-apps/plugin-dialog";
import { executeAsync } from "./action";
import { CellContent, ClipboardCellsData, SchemaRow, Cell, isClipboardCellData, AddNewRowButton, ClipboardCellData } from "./cell";
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


class GridSelectionRegion {
    start: GridPosition;
    rowSpan: number;
    columnSpan: number;
    deselecting: boolean;

    constructor(start: GridPosition, rowSpan: number = 0, columnSpan: number = 0, deselecting: boolean = false) {
        this.start = start;
        this.rowSpan = rowSpan;
        this.columnSpan = columnSpan;
        this.deselecting = deselecting;
    }

    /**
     * The top-left corner of the region.
     */
    get topLeftCorner(): GridPosition {
        return {
            rowIndex: this.start.rowIndex + Math.min(this.rowSpan, 0),
            columnIndex: this.start.columnIndex + Math.min(this.columnSpan, 0)
        };
    }

    /**
     * Tests if the region contains the given position.
     * @param pos 
     * @returns 
     */
    contains(pos: GridPosition): boolean {
        const rowMin: number = Math.min(this.start.rowIndex, this.start.rowIndex + this.rowSpan);
        const rowMax: number = Math.max(this.start.rowIndex, this.start.rowIndex + this.rowSpan);
        const columnMin: number = Math.min(this.start.columnIndex, this.start.columnIndex + this.columnSpan);
        const columnMax: number = Math.max(this.start.columnIndex, this.start.columnIndex + this.columnSpan);
        return pos.rowIndex >= rowMin
            && pos.rowIndex <= rowMax
            && pos.columnIndex >= columnMin
            && pos.columnIndex <= columnMax;
    }

    disjoint({ rowMin: otherRowMin, rowMax: otherRowMax, columnMin: otherColumnMin, columnMax: otherColumnMax }: { rowMin: number, rowMax: number, columnMin: number, columnMax: number }): GridSelectionRegion[] {
        const rowMin: number = Math.min(this.start.rowIndex, this.start.rowIndex + this.rowSpan);
        const rowMax: number = Math.max(this.start.rowIndex, this.start.rowIndex + this.rowSpan);
        const columnMin: number = Math.min(this.start.columnIndex, this.start.columnIndex + this.columnSpan);
        const columnMax: number = Math.max(this.start.columnIndex, this.start.columnIndex + this.columnSpan);

        if (rowMax < otherRowMin || rowMin > otherRowMax || columnMax < otherColumnMin || columnMin > otherColumnMax) {
            // No intersection between two rectangles
            return [this];
        } else {
            let disjointRegions: GridSelectionRegion[] = [];

            // Rows above disallowed region
            if (rowMin < otherRowMin) {
                disjointRegions.push(new GridSelectionRegion({ rowIndex: rowMin, columnIndex: columnMin }, otherRowMin - rowMin - 1, columnMax - columnMin));
            }
            // Columns to left of disallowed region
            if (columnMin < otherColumnMin) {
                disjointRegions.push(new GridSelectionRegion({ rowIndex: Math.max(rowMin, otherRowMin), columnIndex: columnMin }, Math.min(rowMax, otherRowMax) - Math.max(rowMin, otherRowMin), otherColumnMin - columnMin - 1));
            }
            // Columns to right of disallowed region
            if (columnMax < otherColumnMax) {
                disjointRegions.push(new GridSelectionRegion({ rowIndex: Math.max(rowMin, otherRowMin), columnIndex: otherColumnMax }, Math.min(rowMax, otherRowMax) - Math.max(rowMin, otherRowMin), columnMax - otherColumnMax - 1));
            }
            // Rows below disallowed region
            if (rowMax > otherRowMax) {
                disjointRegions.push(new GridSelectionRegion({ rowIndex: otherRowMax, columnIndex: columnMin }, rowMax - otherRowMax - 1, columnMax - columnMin));
            }
            return disjointRegions;
        }
    }
};

class GridSelection {
    #onSetFocusCallbackFn: ((oldFocus: GridPosition, newFocus: GridPosition) => void);
    #onRemoveSelectionCallbackFn: ((pos: GridPosition) => void);
    #onAddSelectionCallbackFn: ((pos: GridPosition) => void);

    #currentRegion: GridSelectionRegion;

    #otherRegions: GridSelectionRegion[] = [];

    /**
     * The focused cell.
     */
    #focus: GridPosition;

    /**
     * The focused cell.
     */
    get focus(): GridPosition {
        return this.#focus;
    }

    /**
     * Sets the focused cell.
     */
    set focus(pos: GridPosition) {
        this.#onSetFocusCallbackFn(this.focus, pos);
        this.#focus = pos;
    }

    /**
     * The regions of the selection.
     */
    get regions(): GridSelectionRegion[] {
        return [...this.#otherRegions, this.#currentRegion];
    }

    constructor(pos: GridPosition, { onSetFocus, onRemoveSelection, onAddSelection } : {
        onSetFocus?: ((oldFocus: GridPosition, newFocus: GridPosition) => void),
        onRemoveSelection?: ((pos: GridPosition) => void),
        onAddSelection?: ((pos: GridPosition) => void)
    }) {
        this.#onSetFocusCallbackFn = onSetFocus || (() => {});
        this.#onRemoveSelectionCallbackFn = onRemoveSelection || (() => {});
        this.#onAddSelectionCallbackFn = onAddSelection || (() => {});

        this.#currentRegion = new GridSelectionRegion(pos);
        this.#onAddSelectionCallbackFn(pos);
        this.#focus = pos;
        this.#onSetFocusCallbackFn(pos, pos);
    }

    reset(pos: GridPosition) {
        // Invoke the deselect callback for each currently-selected cell
        this.getSelectedPositions().forEach(({ rowIndex, columnIndices }) => {
            columnIndices.forEach(columnIndex => {
                this.#onRemoveSelectionCallbackFn({ rowIndex: rowIndex, columnIndex: columnIndex });
            });
        });

        // Reset the selection to a single cell
        this.#currentRegion = new GridSelectionRegion(pos);
        this.#onAddSelectionCallbackFn(pos);
        this.#otherRegions = [];
        this.focus = pos;
    }

    /**
     * Determines the shape of the selection.
     * @returns The string "rect" if the selection is a rectangular area. Otherwise, the string "free".
     */
    shape(): 'cell' | 'rect' | 'free' {
        if (this.#otherRegions.length === 0) 
            return (this.#currentRegion.rowSpan === 0 && this.#currentRegion.columnSpan === 0 ? 'cell' : 'rect');

        // Calculate each region as a bounding box
        const allRegions = this.regions.filter(region => region.deselecting);
        const allMinmax = allRegions.map(region => {
            return {
                rowMin: Math.min(region.start.rowIndex, region.start.rowIndex + region.rowSpan),
                rowMax: Math.max(region.start.rowIndex, region.start.rowIndex + region.rowSpan),
                columnMin: Math.min(region.start.columnIndex, region.start.columnIndex + region.columnSpan),
                columnMax: Math.max(region.start.columnIndex, region.start.columnIndex + region.columnSpan)
            };
        })

        // Calculate the bounding box of all regions
        let rowMin: number = Number.MAX_SAFE_INTEGER;
        let rowMax: number = 0;
        let columnMin: number = Number.MAX_SAFE_INTEGER;
        let columnMax: number = 0;
        allMinmax.forEach(region => {
            rowMin = Math.min(rowMin, region.rowMin);
            rowMax = Math.max(rowMax, region.rowMax);
            columnMin = Math.min(columnMin, region.columnMin);
            columnMax = Math.max(columnMax, region.columnMax);
        });

        // Check if all parts of the bounding box are covered by some region
        let uncoveredRegions: GridSelectionRegion[] = [new GridSelectionRegion({ rowIndex: rowMin, columnIndex: columnMin }, rowMax - rowMin, columnMax - columnMin)];
        allMinmax.forEach(region => {
            for (let k = uncoveredRegions.length - 1; k >= 0; --k) {
                uncoveredRegions.splice(k, 1, ...uncoveredRegions[k].disjoint(region));
            }
        });
        return uncoveredRegions.length === 0 ? 'rect' : 'free';
    }

    /**
     * Invokes the callback functions for when a cell is selected or deselected.
     * @param param0 
     */
    #afterAdjustSelection({ oldRowSpan, oldColumnSpan } : { oldRowSpan?: number, oldColumnSpan?: number }) {
        const deselecting: boolean = this.#otherRegions.some(otherRegion => otherRegion.contains(this.#currentRegion.start));

        if (oldRowSpan !== undefined) {
            // Adjust selection for added/removed rows only

            const columnSpan: number = oldColumnSpan !== undefined ? oldColumnSpan : this.#currentRegion.columnSpan;
            const sgnColumnSpan: number = columnSpan < 0 ? -1 : 1; // Normalize columnSpan to be >= 0
            const iterateOverColumnIndices = (fn: (columnIndex: number) => void) => {
                for (let cs: number = 0; cs <= sgnColumnSpan * columnSpan; ++cs) {
                    fn(this.#currentRegion.start.columnIndex + (sgnColumnSpan * cs));
                }
            }

            const sgn: number = oldRowSpan < 0 ? -1 : 1; // Normalize oldRowSpan to be >= 0
            if (sgn * this.#currentRegion.rowSpan < sgn * oldRowSpan) {
                // Remove selected cells
                for (let rs: number = sgn * oldRowSpan; rs > Math.max(0, sgn * this.#currentRegion.rowSpan); --rs) {
                    iterateOverColumnIndices((columnIndex) => {
                        const pos: GridPosition = {
                            rowIndex: this.#currentRegion.start.rowIndex + (sgn * rs),
                            columnIndex: columnIndex
                        };

                        if (deselecting) {
                            // Check if the un-deselected cell belongs to some other selection region, and add it back if so
                            if (this.#otherRegions.some(otherRegion => otherRegion.contains(pos))) {
                                this.#onAddSelectionCallbackFn(pos);
                            }
                        } else {
                            this.#onRemoveSelectionCallbackFn(pos);
                        }
                    });
                }

                if (sgn * this.#currentRegion.rowSpan < 0) {
                    // Add selected cells on the other side (or remove, if deselecting)
                    for (let rs: number = -1; rs >= sgn * this.#currentRegion.rowSpan; --rs) {
                        iterateOverColumnIndices((columnIndex) => {
                            (deselecting ? this.#onRemoveSelectionCallbackFn : this.#onAddSelectionCallbackFn)({
                                rowIndex: this.#currentRegion.start.rowIndex + (sgn * rs),
                                columnIndex: columnIndex
                            });
                        });
                    }
                }
            } else if (sgn * this.#currentRegion.rowSpan > sgn * oldRowSpan) {
                // Add selected cells (or remove, if deselecting)
                for (let rs: number = (sgn * oldRowSpan) + 1; rs <= sgn * this.#currentRegion.rowSpan; ++rs) {
                    iterateOverColumnIndices((columnIndex) => {
                        (deselecting ? this.#onRemoveSelectionCallbackFn : this.#onAddSelectionCallbackFn)({
                            rowIndex: this.#currentRegion.start.rowIndex + (sgn * rs),
                            columnIndex: columnIndex
                        });
                    });
                }
            }
        } 
        
        if (oldColumnSpan !== undefined) {
            // Adjust selection for added/removed columns only

            const iterateOverRowIndices = (fn: (rowIndex: number) => void) => {
                for (let rs: number = 0; (this.#currentRegion.rowSpan > 0 ? rs <= this.#currentRegion.rowSpan : rs >= this.#currentRegion.rowSpan); (this.#currentRegion.rowSpan > 0 ? ++rs : --rs)) {
                    fn(this.#currentRegion.start.rowIndex + rs);
                }
            }
            
            const sgn: number = oldColumnSpan < 0 ? -1 : 1; // Normalize oldRowSpan to be >= 0
            if (sgn * this.#currentRegion.columnSpan < sgn * oldColumnSpan) {
                // Remove selected cells
                for (let cs: number = sgn * oldColumnSpan; cs > Math.max(0, sgn * this.#currentRegion.columnSpan); --cs) {
                    iterateOverRowIndices((rowIndex) => {
                        const pos: GridPosition = {
                            rowIndex: rowIndex,
                            columnIndex: this.#currentRegion.start.columnIndex + (sgn * cs)
                        };

                        if (deselecting) {
                            // Check if the un-deselected cell belongs to some other selection region, and add it back if so
                            if (this.#otherRegions.some(otherRegion => otherRegion.contains(pos))) {
                                this.#onAddSelectionCallbackFn(pos);
                            }
                        } else {
                            this.#onRemoveSelectionCallbackFn(pos);
                        }
                    });
                }

                if (sgn * this.#currentRegion.columnSpan < 0) {
                    // Add selected cells on the other side (or remove, if deselecting)
                    for (let cs: number = -1; cs >= sgn * this.#currentRegion.columnSpan; --cs) {
                        iterateOverRowIndices((rowIndex) => {
                            (deselecting ? this.#onRemoveSelectionCallbackFn : this.#onAddSelectionCallbackFn)({
                                rowIndex: rowIndex,
                                columnIndex: this.#currentRegion.columnSpan + (sgn * cs)
                            });
                        });
                    }
                }
            } else if (sgn * this.#currentRegion.columnSpan > sgn * oldColumnSpan) {
                // Add selected cells (or remove, if deselecting)
                for (let cs: number = (sgn * oldColumnSpan) + 1; cs <= sgn * this.#currentRegion.columnSpan; ++cs) {
                    iterateOverRowIndices((rowIndex) => {
                        (deselecting ? this.#onRemoveSelectionCallbackFn : this.#onAddSelectionCallbackFn)({
                            rowIndex: rowIndex,
                            columnIndex: this.#currentRegion.start.columnIndex + (sgn * cs)
                        });
                    });
                }
            }
        }
    }

    /**
     * Adds a new selection region.
     * @param pos 
     */
    pushRegion(pos: GridPosition, rowSpan?: number, columnSpan?: number, deselecting?: boolean) {
        this.#otherRegions.push(this.#currentRegion);
        this.#currentRegion = new GridSelectionRegion(pos, rowSpan ?? 0, columnSpan ?? 0, deselecting !== undefined ? deselecting : (this.#otherRegions.some(otherRegion => otherRegion.contains(pos))));
        this.#onAddSelectionCallbackFn(pos);

        // Change the focused cell, if the new region is not deselecting
        if (!this.#currentRegion.deselecting)
            this.focus = pos;
    }

    /**
     * Resizes the selection region that is currently editable.
     * @param param0 
     */
    resizeCurrentRegion({ rowSpanShift, columnSpanShift, maxRowIndex, maxColumnIndex }: { rowSpanShift?: number, columnSpanShift?: number, maxRowIndex: number, maxColumnIndex: number }) {
        if (rowSpanShift) {
            const oldRowSpan = this.#currentRegion.rowSpan;

            this.#currentRegion.rowSpan += rowSpanShift;
            if (this.#currentRegion.start.rowIndex + this.#currentRegion.rowSpan < 0) 
                this.#currentRegion.rowSpan = -this.#currentRegion.start.rowIndex;
            if (this.#currentRegion.start.rowIndex + this.#currentRegion.rowSpan >= maxRowIndex)
                this.#currentRegion.rowSpan = maxRowIndex - this.#currentRegion.start.rowIndex - 1;

            this.#afterAdjustSelection({ oldRowSpan: oldRowSpan });
        }
        if (columnSpanShift) {
            const oldColumnSpan: number = this.#currentRegion.columnSpan;

            this.#currentRegion.columnSpan += columnSpanShift;
            if (this.#currentRegion.start.columnIndex + this.#currentRegion.columnSpan < 0) 
                this.#currentRegion.columnSpan = -this.#currentRegion.start.columnIndex;
            if (this.#currentRegion.start.columnIndex + this.#currentRegion.columnSpan >= maxColumnIndex)
                this.#currentRegion.columnSpan = maxColumnIndex - this.#currentRegion.start.columnIndex - 1;

            this.#afterAdjustSelection({ oldColumnSpan: oldColumnSpan });
        }
    }

    /**
     * Directly sets the size of the selection region that is currently editable.
     * @param end The endpoint of the current selection region.
     */
    setSizeCurrentRegion(end: GridPosition) {
        const oldRowSpan: number = this.#currentRegion.rowSpan;
        const oldColumnSpan: number = this.#currentRegion.columnSpan;

        this.#currentRegion.rowSpan = end.rowIndex - this.#currentRegion.start.rowIndex;
        this.#currentRegion.columnSpan = end.columnIndex - this.#currentRegion.start.columnIndex;

        this.#afterAdjustSelection({ oldRowSpan: oldRowSpan, oldColumnSpan: oldColumnSpan });
    }

    /**
     * Stops doing mouse selection of the current region.
     */
    finalizeCurrentRegion() {
        if (this.#currentRegion.deselecting) {
            const rowMin: number = Math.min(this.#currentRegion.start.rowIndex, this.#currentRegion.start.rowIndex + this.#currentRegion.rowSpan);
            const rowMax: number = Math.max(this.#currentRegion.start.rowIndex, this.#currentRegion.start.rowIndex + this.#currentRegion.rowSpan);
            const columnMin: number = Math.min(this.#currentRegion.start.columnIndex, this.#currentRegion.start.columnIndex + this.#currentRegion.columnSpan);
            const columnMax: number = Math.max(this.#currentRegion.start.columnIndex, this.#currentRegion.start.columnIndex + this.#currentRegion.columnSpan);            

            // Remove the current region from the selection
            for (let k = this.#otherRegions.length - 1; k >= 0; --k) {
                const otherRegion: GridSelectionRegion = this.#otherRegions[k];
                this.#otherRegions.splice(k, 1, ...otherRegion.disjoint({ rowMin: rowMin, rowMax: rowMax, columnMin: columnMin, columnMax: columnMax }));
            }

            // Set the new current region
            this.#currentRegion = this.#otherRegions.pop() || new GridSelectionRegion(this.#currentRegion.start);
        }
    }

    /**
     * Shifts the focused position within the selection, prioritizing keeping the columnIndex the same if possible.
     * @param delta 
     * @returns True if multiple cells are selected. False if only a single cell is selected.
     */
    shiftFocusByRowThenColumn(delta: number): boolean {
        const selectedPositions = this.getSelectedPositions();
        if (selectedPositions.length <= 1)
            return false;

        const selectedPositionsIndex = selectedPositions.findIndex(({ rowIndex }) => rowIndex === this.focus.rowIndex);
        if (selectedPositionsIndex < 0) {
            this.focus = this.#currentRegion.start;
            return true;
        }

        // Prioritize shifting rowIndex in the direction of delta while preserving columnIndex
        for (let newSelectedPositionsIndex: number = selectedPositionsIndex + delta; newSelectedPositionsIndex >= 0 && newSelectedPositionsIndex < selectedPositions.length; newSelectedPositionsIndex += delta) {
            const newSelectedPositionsColumnIndex = selectedPositions[newSelectedPositionsIndex].columnIndices.indexOf(this.focus.columnIndex);
            if (newSelectedPositionsColumnIndex >= 0) {
                this.focus = {
                    rowIndex: selectedPositions[newSelectedPositionsIndex].rowIndex,
                    columnIndex: this.focus.columnIndex
                };
                return true;
            }
        }

        // If columnIndex cannot be preserved, shift columnIndex in the direction of delta and reset rowIndex
        if (delta > 0) {
            // Find the next-greatest columnIndex
            let next: GridPosition | null = null;
            selectedPositions.forEach(({ rowIndex, columnIndices }) => {
                const newColumnIndex = columnIndices.find(columnIndex => columnIndex > this.focus.columnIndex);
                if (newColumnIndex !== undefined) {
                    if (next === null || next.columnIndex > newColumnIndex)
                        next = { rowIndex: rowIndex, columnIndex: newColumnIndex };
                }
            });

            if (!next) {
                // If no next-greatest columnIndex exists in selection, find the lowest columnIndex
                selectedPositions.forEach(({ rowIndex, columnIndices }) => {
                    const newColumnIndex = columnIndices[0];
                    if (newColumnIndex !== undefined) {
                        if (next === null || next.columnIndex > newColumnIndex)
                            next = { rowIndex: rowIndex, columnIndex: newColumnIndex };
                    }
                });
            }

            if (next) {
                this.focus = next;
            } else {
                // This case shouldn't happen
                this.focus = this.#currentRegion.start;
            }
            return true;
        } else {
            const reversedSelectedPositions = selectedPositions.reverse();

            // Find the next-lowest columnIndex
            let next: GridPosition | null = null;
            reversedSelectedPositions.forEach(({ rowIndex, columnIndices }) => {
                const newColumnIndex = columnIndices.find(columnIndex => columnIndex < this.focus.columnIndex);
                if (newColumnIndex !== undefined) {
                    if (next === null || next.columnIndex < newColumnIndex)
                        next = { rowIndex: rowIndex, columnIndex: newColumnIndex };
                }
            });

            if (!next) {
                // If no next-lowest columnIndex exists in selection, find the greatest columnIndex
                reversedSelectedPositions.forEach(({ rowIndex, columnIndices }) => {
                    const newColumnIndex = columnIndices[columnIndices.length - 1];
                    if (newColumnIndex !== undefined) {
                        if (next === null || next.columnIndex < newColumnIndex)
                            next = { rowIndex: rowIndex, columnIndex: newColumnIndex };
                    }
                });
            }

            if (next) {
                this.focus = next;
            } else {
                // This case shouldn't happen
                this.focus = this.#currentRegion.start;
            }
            return true;
        }
    }

    /**
     * Shifts the focused position within the selection, prioritizing keeping the rowIndex the same if possible.
     * @param delta 
     * @returns True if multiple cells are selected. False if only a single cell is selected.
     */
    shiftFocusByColumnThenRow(delta: number): boolean {
        const selectedPositions = this.getSelectedPositions();
        if (selectedPositions.length <= 1)
            return false;

        const selectedPositionsIndex = selectedPositions.findIndex(({ rowIndex }) => rowIndex === this.focus.rowIndex);
        if (selectedPositionsIndex < 0) {
            this.focus = this.#currentRegion.start;
            return true;
        }

        // Prioritize shifting columnIndex in the direction of delta while preserving rowIndex
        const columnIndexIndex = selectedPositions[selectedPositionsIndex].columnIndices.indexOf(this.focus.columnIndex);
        if (columnIndexIndex < 0) {
            this.focus = this.#currentRegion.start;
            return true;
        }
        const newColumnIndexIndex = columnIndexIndex + delta;

        if (newColumnIndexIndex >= 0 && newColumnIndexIndex < selectedPositions[selectedPositionsIndex].columnIndices.length) {
            this.focus = {
                rowIndex: this.focus.rowIndex,
                columnIndex: selectedPositions[selectedPositionsIndex].columnIndices[newColumnIndexIndex]
            };
            return true;
        }

        // If rowIndex cannot be preserved, shift rowIndex in the direction of delta and reset columnIndex
        const newSelectedPositionsIndex = (selectedPositionsIndex + delta) % selectedPositions.length;
        this.focus = {
            rowIndex: selectedPositions[newSelectedPositionsIndex].rowIndex,
            columnIndex: selectedPositions[newSelectedPositionsIndex].columnIndices[delta > 0 ? 0 : (selectedPositions[newSelectedPositionsIndex].columnIndices.length - 1)]
        };
        return true;
    }

    /**
     * Tests if the selection contains the given position.
     * @param pos 
     * @returns 
     */
    contains(pos: GridPosition): boolean {
        return this.regions.some((region: GridSelectionRegion) => region.contains(pos));
    }

    /**
     * Gets an array of all selected positions.
     * @returns 
     */
    getSelectedPositions(): { rowIndex: number, columnIndices: number[] }[] {
        // Get all column spans associated with each row
        const rows: { [rowIndex: number]: [number, number][] } = {};
        this.regions.forEach((region: GridSelectionRegion) => {
            const rowMin: number = Math.min(region.start.rowIndex, region.start.rowIndex + region.rowSpan);
            const rowMax: number = Math.max(region.start.rowIndex, region.start.rowIndex + region.rowSpan);
            const columnMin: number = Math.min(region.start.columnIndex, region.start.columnIndex + region.columnSpan);
            const columnMax: number = Math.max(region.start.columnIndex, region.start.columnIndex + region.columnSpan);         

            for (let rowIndex: number = rowMin; rowIndex <= rowMax; ++rowIndex) {
                if (region.deselecting) {
                    if (rowIndex in rows) {
                        for (let k = rows[rowIndex].length - 1; k >= 0; --k) {
                            const [selectedColumnMin, selectedColumnMax] = rows[rowIndex][k];
                            if (columnMax < selectedColumnMin || selectedColumnMax < columnMin)
                                continue;
                            if (columnMin <= selectedColumnMin && selectedColumnMax <= columnMax)
                                

                            let replacements: [number, number][] = [];
                            if (columnMin > selectedColumnMin)
                                replacements.push([selectedColumnMin, columnMin - 1])
                            rows[rowIndex].splice(k, 1, replacements);
                        }
                        rows[rowIndex].splice
                    }
                } else {
                    if (rowIndex in rows) {
                        rows[rowIndex].push([columnMin, columnMax]);
                    } else {
                        rows[rowIndex] = [[columnMin, columnMax]];
                    }
                }
            }
        });

        // Filter out duplicate columns
        let selectedPositions: { rowIndex: number, columnIndices: number[] }[] = [];
        for (const rowIndex of Object.keys(rows).map(str => parseInt(str)).sort()) {
            const columnIndices: Set<number> = new Set();
            rows[rowIndex].forEach(([columnMin, columnMax]) => {
                for (let columnIndex: number = columnMin; columnIndex <= columnMax; ++columnIndex) {
                    columnIndices.add(columnIndex);
                }
            });
            selectedPositions.push({
                rowIndex: rowIndex,
                columnIndices: [...columnIndices].sort()
            });
        }
        return selectedPositions;
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
                    cell.elem.setAttribute('pos', JSON.stringify(pos));

                    cell.setStartEditingCallback(async () => {
                        if (!this.#mode)
                            this.#initSelection(pos);
                        else {
                            // User can only edit one cell at a time                    
                            if (this.#mode && this.#mode.state == 'editingFocus' && this.#mode.selection.focus !== pos) {
                                await this.#stopEditingAsync();
                            }

                            // Mark the position of the cell being edited
                            this.#mode.selection.focus = pos;
                        }
                    });
                    cell.setStopEditingCallback(async () => {
                        // Unmark that the cell is being edited
                        if (this.#mode && this.#mode.state == 'editingFocus' && this.#mode.selection.focus === pos) 
                            this.#mode.state = null;
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


        // Set up initial columns
        this.#columns = columns?.map(c => new GridColumn(cwd, c)) || [];

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

    /**
     * Retrieves the cell by its corresponding HTMLElement, or a child of its corresponding HTMLElement.
     * @param elem 
     * @returns 
     */
    #getCellByElem(elem: Node | null) : Cell | undefined {
        while (elem) {
            if (elem instanceof Element && elem.hasAttribute('pos')) 
                return this.#getCellByPosition(JSON.parse(elem.getAttribute('pos') || '{"rowIndex":0,"columnIndex":0}'));
            elem = elem.parentElement;
        }
        return undefined;
    }


    /**
     * Moves the selection in the specified direction.
     * @param param0 
     * @param shiftKey 
     */
    #moveSelection({ rowShift, columnShift }: { rowShift?: number, columnShift?: number }, shiftKey: boolean) {
        if (this.#mode) {
            if (shiftKey) {
                // Move the span of the last selection region
                this.#mode.selection.resizeCurrentRegion({
                    rowSpanShift: rowShift,
                    columnSpanShift: columnShift,
                    maxRowIndex: this.#rows.length - 1,
                    maxColumnIndex: this.#columns.length - 1
                });
            } else {
                // Reset selection to focus on single cell shifted from formerly-focused cell
                const rowIndex: number = this.#mode.selection.focus.rowIndex + (rowShift || 0);
                const columnIndex: number = this.#mode.selection.focus.columnIndex + (columnShift || 0);
                this.#resetSelection({
                    rowIndex: Math.max(0, Math.min(this.#rows.length - 1, rowIndex)),
                    columnIndex: Math.max(0, Math.min(this.#columns.length - 1, columnIndex))
                });
            }
        }
    }

    /**
     * Shifts the focus in a direction, as by ENTER or TAB key-presses.
     * @param columnThenRow True if prioritizing keeping rowIndex the same while shifting columnIndex. False if prioritizing keeping columnIndex the same while shifting rowIndex.
     * @param delta The direction to shift in. Should be either +1 or -1.
     */
    #shiftFocus(columnThenRow: boolean, delta: number) {
        if (this.#mode) {
            // Remove the "focused" class from the currently-focused cell
            const oldFocus = this.#mode.selection.focus;

            // Shift the focus within the selection by the indicated direction
            if (!(columnThenRow ? this.#mode.selection.shiftFocusByColumnThenRow : this.#mode.selection.shiftFocusByRowThenColumn)(delta)) {
                // Only a single cell is selected, so shift the single cell that is selected
                let newRowIndex: number = oldFocus.rowIndex;
                let newColumnIndex: number = oldFocus.columnIndex;
                if (columnThenRow) {
                    // Shift by column first, then row
                    newColumnIndex += delta;
                    if (newColumnIndex < 0) {
                        newRowIndex = (newRowIndex - 1) % this.#rows.length;
                        newColumnIndex = this.#columns.length - 1;
                    } else if (newColumnIndex >= this.#columns.length) {
                        newRowIndex = (newRowIndex + 1) % this.#rows.length;
                        newColumnIndex = 0;
                    }
                } else {
                    // Shift by row first, then column
                    newRowIndex += delta;
                    if (newRowIndex < 0) {
                        newColumnIndex = (newColumnIndex - 1) % this.#columns.length;
                        newRowIndex = this.#rows.length - 1;
                    } else if (newRowIndex >= this.#rows.length) {
                        newColumnIndex = (newColumnIndex + 1) % this.#columns.length;
                        newRowIndex = 0;
                    }
                }
                this.#mode.selection.focus = {
                    rowIndex: newRowIndex,
                    columnIndex: newColumnIndex
                };
            }
        }
    }



    /**
     * The current user mode.
     */
    #mode: {
        selection: GridSelection,
        state: 'editingFocus' | 'draggingCurrentRegion' | null
    } | null = null;

    /**
     * Initializes the selection as a single cell.
     * @param pos 
     */
    #initSelection(pos: GridPosition) {
        this.#mode = {
            selection: new GridSelection(pos, {
                onSetFocus: (oldFocus, newFocus) => {
                    const oldFocusedCell = this.#getCellByPosition(oldFocus);
                    if (oldFocusedCell)
                        oldFocusedCell.elem.classList.remove('focused');

                    const newFocusedCell = this.#getCellByPosition(newFocus);
                    if (newFocusedCell)
                        newFocusedCell.elem.classList.add('focused');
                },
                onAddSelection: (pos) => {
                    const cell = this.#getCellByPosition(pos);
                    if (cell)
                        cell.elem.classList.add('selected');
                },
                onRemoveSelection: (pos) => {
                    const cell = this.#getCellByPosition(pos);
                    if (cell)
                        cell.elem.classList.remove('selected');
                }
            }),
            state: null
        }
    }

    /**
     * Resets the user's selection to a single cell.
     * @param pos 
     */
    #resetSelection(pos: GridPosition) {
        if (!this.#mode)
            this.#initSelection(pos);
        else 
            this.#mode.selection.reset(pos);
    }

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
        if (this.#mode && this.#mode.state === 'editingFocus') {
            const cell = this.#getCellByPosition(this.#mode.selection.focus);
            if (cell) {
                await cell.revertEditAsync();
            }
        }
    }

    /**
     * Stop editing the cell currently being edited, and push the changes to the database.
     */
    async #stopEditingAsync() {
        if (this.#mode && this.#mode.state === 'editingFocus') {
            const cell = this.#getCellByPosition(this.#mode.selection.focus);
            if (cell) {
                await cell.stopEditingAsync();
            }
        }
    }

    /**
     * Clears the content of the cells that are currently selected.
     */
    #clearSelectedCells() {
        // Iterate over all selected positions of the grid
        if (this.#mode) {
            this.#mode.selection.getSelectedPositions().forEach(({ rowIndex, columnIndices }) => {
                columnIndices.forEach((columnIndex) => {
                    const pos: GridPosition = { rowIndex: rowIndex, columnIndex: columnIndex };
                    const cell: Cell | undefined = this.#getCellByPosition(pos);
                    if (cell) {
                        // Clear the contents of the cell
                        cell.clearAsync();
                    }
                })
            });
        }
    }


    /**
     * Pastes the content of a single cell into the selection.
     * @param content 
     */
    async cellPasteAsync(content: ClipboardCellData) {
        if (this.#mode) {
            // Loop over all selected cells, pasting the clipboard data into each of them
            for (const { rowIndex, columnIndices } of this.#mode.selection.getSelectedPositions()) {
                for (const columnIndex of columnIndices) {
                    const pos: GridPosition = { rowIndex: rowIndex, columnIndex: columnIndex };
                    const cell = this.#getCellByPosition(pos);
                    if (cell) {
                        await cell.setAsync(content);
                    }
                }
            }
        }
    }

    /**
     * Pastes a rectangle of cells into the selection.
     * @param content 
     * @returns 
     */
    async rectPasteAsync(content: ClipboardCellData[][]) {
        if (this.#mode && content.length > 0 && content[0].length > 0) {
            const regions = this.#mode.selection.regions;
            for (let regionIndex: number = 0; regionIndex < regions.length; ++regionIndex) {
                const region = regions[regionIndex];
                const { rowIndex: baseRowIndex, columnIndex: baseColumnIndex } = region.topLeftCorner;

                // Loop over each pasted row. If there are more selected rows than the pasted rows (in a multiple of the pasted rows), repeat the pasting process
                let rowIndex: number = baseRowIndex;
                for (let r = (1 + Math.abs(region.rowSpan)) % content.length === 0 ? Math.max(1, Math.round((1 + Math.abs(region.rowSpan)) / content.length)) : 1; r > 0 && rowIndex < this.#rows.length; --r) {
                    for (const pastedRow of content) {

                        // Loop over each column in the row. Unlike rows, do not duplicate if the selection extends past the pasted area
                        let columnIndex: number = baseColumnIndex;
                        for (const pastedContent of pastedRow) {
                            const pos: GridPosition = {
                                rowIndex: rowIndex,
                                columnIndex: columnIndex
                            };
                            const cell = this.#getCellByPosition(pos);
                            if (cell) {
                                await cell.setAsync(pastedContent);
                            }

                            // Increment the column
                            ++columnIndex;
                            if (columnIndex >= this.#columns.length)
                                break;
                        }

                        // Increment the row
                        ++rowIndex;
                        if (rowIndex >= this.#rows.length)
                            return; // TODO create more rows
                    }
                }
            }
        }
    }

    async freePasteAsync(content: ClipboardCellData[][]) {
        if (this.#mode) {
            if (this.#mode.selection.shape() == 'cell') {
                // Paste the full size of the clipboard data

                let rowIndex: number = this.#mode.selection.focus.rowIndex;
                let needsReset: boolean = true;

                for (const pastedRow of content) {
                    // For each row being pasted, look up whether the row in the destination table has that columnOid, and paste it in if so
                    for (const pastedContent of pastedRow) {
                        const columnIndex: number = this.#columns.findIndex(column => column.metadata.oid === pastedContent.columnOid);
                        if (columnIndex >= 0) {
                            const pos: GridPosition = { rowIndex: rowIndex, columnIndex: columnIndex };
                            const cell = this.#getCellByPosition(pos);
                            if (cell) {
                                await cell.setAsync(pastedContent);
                                
                                if (needsReset) {
                                    this.#resetSelection(pos);
                                } else {
                                    this.#mode.selection.pushRegion(pos);
                                }
                            }
                        }
                    }

                    if (++rowIndex >= this.#rows.length)
                        break; // TODO automatically add new rows
                }
            } else {
                // Paste the clipboard data, as constrained by the selection area
                let contentIndex: number = 0;
                for (const { rowIndex, columnIndices } of this.#mode.selection.getSelectedPositions()) {
                    const pastedRow: ClipboardCellData[] = content[contentIndex];

                    // For each row being pasted, look up whether the row in the destination table has that columnOid, and paste it in if so
                    for (const pastedContent of pastedRow) {
                        const columnIndex: number = this.#columns.findIndex(column => column.metadata.oid === pastedContent.columnOid);
                        if (columnIndex >= 0 && columnIndices.indexOf(columnIndex) >= 0) { // Constrain to only the selected columns in this row
                            const pos: GridPosition = { rowIndex: rowIndex, columnIndex: columnIndex };
                            const cell = this.#getCellByPosition(pos);
                            if (cell) {
                                await cell.setAsync(pastedContent);
                            }
                        }
                    }

                    // Increment the contentIndex, looping if necessary
                    contentIndex = (contentIndex + 1) % content.length;
                }
            }
        }
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

        "paste": async (e: ClipboardEvent) => {
            if (this.#mode && this.#mode.state == 'editingFocus') return;
            e.preventDefault();

            function parsePastedData(plaintextData: string | undefined): ClipboardCellsData {
                if (!plaintextData)
                    return {
                        content: {
                            columnOid: 0,
                            value: null
                        },
                        shape: 'cell'
                    };

                try {
                    const obj: any = JSON.parse(plaintextData);

                    // Validate the format, then cast
                    if (typeof obj === 'object' && 'shape' in obj && 'content' in obj
                        && (
                            ((obj.shape === 'rect' || obj.shape === 'free') 
                                && Array.isArray(obj.content) 
                                && obj.content.every((arr: any) => Array.isArray(arr) && arr.every((item: any) => isClipboardCellData(item))))
                            || (obj.shape === 'cell' && isClipboardCellData(obj.content))
                        )) {
                        return obj;
                    }
                    throw new Error('Pasted object is not in the expected format.');
                } catch (e) {
                    // Treat as pasting plaintext
                    return {
                        content: {
                            columnOid: 0,
                            value: {
                                text: plaintextData
                            }
                        },
                        shape: 'cell'
                    };
                }
            }

            // window.clipboardData handles clipboard data in IE
            const plaintextData = e.clipboardData?.getData("text/plain");
            const pastedContent: ClipboardCellsData = parsePastedData(plaintextData);

            if (pastedContent.shape == 'cell') {
                // Paste a single cell
                await this.cellPasteAsync(pastedContent.content);
                return;
            }

            if (pastedContent.shape == 'rect') {
                // Determine if pasting as a rectangle is appropriate
            }

            // If pasting as a rectangle is inappropriate or invalid, paste as a free shape
            await this.freePasteAsync(pastedContent.content);
        },

        "keydown": (e: KeyboardEvent) => {
            if (e.metaKey
                || (e.ctrlKey && e.key !== "ArrowDown" && e.key !== "ArrowUp" && e.key !== "ArrowLeft" && e.key !== "ArrowRight")) 
                return;

            if (this.#mode) {
                if (e.key === "Escape" && this.#mode && this.#mode.state === 'editingFocus') {
                    // Cancel changes and stop editing the cell
                    e.preventDefault();
                    this.#revertEditAsync();
                }

                if (e.key === "Enter") {
                    // Shift the focus by row
                    e.preventDefault();
                    this.#shiftFocus(false, e.shiftKey ? -1 : 1);
                }

                if (e.key === "Tab") {
                    // Shift the focus by column
                    e.preventDefault();
                    this.#shiftFocus(true, e.shiftKey ? -1 : 1);
                }

                if (this.#mode && this.#mode.state !== 'editingFocus') {
                    if (e.key === "F2") {
                        // Start editing the focused cell
                        e.preventDefault();
                        this.#startEditingAsync(this.#mode.selection.focus);
                    }
                    if (e.key === "Delete" || e.key === "Backspace") {
                        // Clear the selected cells
                        e.preventDefault();
                        this.#clearSelectedCells();
                    }

                    if (e.key === "ArrowDown") {
                        e.preventDefault();
                        if (e.ctrlKey) {
                            // Move to the last row
                            this.#moveSelection({ rowShift: Number.MAX_SAFE_INTEGER }, e.shiftKey);
                        } else {
                            // Move down by a single row
                            this.#moveSelection({ rowShift: +1 }, e.shiftKey);
                        }
                    }
                    if (e.key === "ArrowUp") {
                        e.preventDefault();
                        if (e.ctrlKey) {
                            // Move to the first row
                            this.#moveSelection({ rowShift: Number.MIN_SAFE_INTEGER }, e.shiftKey);
                        } else {
                            // Move up by a single row
                            this.#moveSelection({ rowShift: -1 }, e.shiftKey);
                        }
                    }
                    if (e.key === "ArrowLeft") {
                        e.preventDefault();
                        if (e.ctrlKey) {
                            // Move to the first column
                            this.#moveSelection({ columnShift: Number.MIN_SAFE_INTEGER }, e.shiftKey);
                        } else {
                            // Move left by a single column
                            this.#moveSelection({ columnShift: -1 }, e.shiftKey);
                        }
                    }
                    if (e.key === "ArrowRight") {
                        e.preventDefault();
                        if (e.ctrlKey) {
                            // Move to the last column
                            this.#moveSelection({ columnShift: Number.MAX_SAFE_INTEGER }, e.shiftKey);
                        } else {
                            // Move right by a single column
                            this.#moveSelection({ columnShift: +1 }, e.shiftKey);
                        }
                    }
                }

                if (e.key.length === 1 && this.#mode && this.#mode.state !== 'editingFocus') {
                    // Start editing the cell. The first keypress will happen after the cell's text has already been highlighted.
                    this.#startEditingAsync(this.#mode.selection.focus);
                }
            }
        },

        "mousedown": (e: MouseEvent) => {
            if (e.button === 0) { // Main mouse button pressed
                const highlightedCell = this.#getCellByElem(e.target as Node);
                if (!highlightedCell)
                    return;
                
                const pos: GridPosition = JSON.parse(highlightedCell.elem.getAttribute('pos') || '{"rowIndex":0,"columnIndex":0}');
                if (e.ctrlKey) {
                    // Multiselect
                    if (this.#mode) {
                        this.#mode.selection.pushRegion(pos);
                    } else {
                        this.#initSelection(pos);
                    }
                } else {
                    // Reset the selection
                    this.#resetSelection(pos);
                }
                if (this.#mode) this.#mode.state = 'draggingCurrentRegion';
            }
        },
        "mouseenter": (e: MouseEvent) => {
            if (this.#mode && this.#mode.state == 'draggingCurrentRegion' && e.target instanceof Node) {
                const highlightedCell = this.#getCellByElem(e.target as Node);
                if (!highlightedCell)
                    return;

                const pos: GridPosition = JSON.parse(highlightedCell.elem.getAttribute('pos') || '{"rowIndex":0,"columnIndex":0}');
                this.#mode.selection.setSizeCurrentRegion(pos);
            }
        },
        "mouseup": (e: MouseEvent) => {
            if (e.button === 0 && this.#mode && this.#mode.state == 'draggingCurrentRegion') {
                this.#mode.selection.finalizeCurrentRegion();
                this.#mode.state = null;
            }
        }
    };
}