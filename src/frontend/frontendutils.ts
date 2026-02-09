import '@interactjs/auto-start';
import '@interactjs/actions/drag';
import '@interactjs/actions/resize';
import interact from '@interactjs/interact';
import { ResizeEvent } from '@interactjs/actions/resize/plugin';

import Sortable, { SortableEvent } from 'sortablejs';


/**
 * Makes the columns of a table resizable.
 */
export function makeColumnsResizable(target: string) {
    interact(target).resizable({
        edges: { right: true },
        onmove(event: ResizeEvent) {
            const target = event.target;
            const width = parseFloat(target.style.width) || 0;
            target.style.width = width + event.dx + 'px';
        },
    });
}

/**
 * Makes the columns of a table draggable into a different ordering.
 */
export function makeColumnsReorderable(tableHeaderRow: HTMLTableRowElement, onUpdateCallbackFn: (reorderedColumnHeader: HTMLElement, columnHeaderToImmediateRight: HTMLElement | null) => void) {
    new Sortable(tableHeaderRow, {
        draggable: '.reorderable-column',
        onUpdate(e: SortableEvent) {
            onUpdateCallbackFn(
                e.item,
                e.newDraggableIndex && e.newDraggableIndex > 0 ? e.to.querySelector(`.reorderable-column:nth-child(${(e.newDraggableIndex - 1)})`) : null
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
