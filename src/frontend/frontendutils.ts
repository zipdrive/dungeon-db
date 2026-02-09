import '@interactjs/auto-start';
import '@interactjs/actions/drag';
import '@interactjs/actions/resize';
import interact from '@interactjs/interact';
import { ResizeEvent } from '@interactjs/actions/resize/plugin';

import Sortable, { SortableEvent, AutoScroll } from 'sortablejs/modular/sortable.core.esm.js';
Sortable.mount(new AutoScroll());


/**
 * Makes the columns of a table resizable.
 * Applied only to columns with class "resizable-column".
 */
export function makeColumnsResizable(onResizeCallbackFn: (resizedCell: Element, newColumnWidth: number) => void) {
    interact('.resizable-column').resizable({
        edges: { right: true },
        onmove(event: ResizeEvent) {
            const target = event.target as HTMLElement;
            const width = target.offsetWidth;
            onResizeCallbackFn(target, width + event.dx);
        },
    });
}

/**
 * Makes the columns of a table draggable into a different ordering.
 * Applied only to columns with class "reorderable-column".
 */
export function makeColumnsReorderable(tableHeaderRow: HTMLTableRowElement, onChangeCallbackFn: (reorderedColumnHeader: HTMLElement, columnHeaderToImmediateRight: HTMLElement | null) => void, onUpdateCallbackFn: (reorderedColumnHeader: HTMLElement, columnHeaderToImmediateRight: HTMLElement | null) => void) {
    new Sortable(tableHeaderRow, {
        draggable: '.reorderable-column',
        onChange(e: SortableEvent) {
            onChangeCallbackFn(
                e.item,
                e.newIndex ? e.to.querySelector(`.reorderable-column:nth-child(${(e.newIndex + 2)})`) : null
            );
        },
        onUpdate(e: SortableEvent) {
            onUpdateCallbackFn(
                e.item,
                e.newIndex ? e.to.querySelector(`.reorderable-column:nth-child(${(e.newIndex + 2)})`) : null
            );
        }
    });
}

/**
 * Makes the rows of a table draggable into a different ordering.
 */
export function makeRowsDraggable(tableBodyElement: HTMLElement, onUpdateCallbackFn: (reorderedRow: HTMLElement, rowImmediatelyBelow: HTMLElement | null) => void) {
    new Sortable(tableBodyElement, {
        draggable: '.reorderable-row',
        onUpdate(e: SortableEvent) {
            onUpdateCallbackFn(
                e.item,
                e.newDraggableIndex ? e.to.querySelector(`reorderable-row:nth-child(${(e.newDraggableIndex + 1)})`) : null
            );
        }
    });
}
