import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { FullMetadata as SchemaFullMetadata } from "./schema";
import { openDialogAsync } from "./dialog";
import { executeAsync } from "./action";
import { message } from "@tauri-apps/plugin-dialog";

import '@interactjs/auto-start';
import '@interactjs/actions/drag';
import '@interactjs/actions/resize';
import interact from '@interactjs/interact';
import { ResizeEvent } from '@interactjs/actions/resize/plugin';

export type Primitive = 'plainText' | 'markdownText' | 'jsonText' | 'xmlText' | 'integer' | 'number' | 'boolean' | 'date' | 'datetime' | 'file' | 'image';

export type ColumnType = {
    primitive: Primitive
} | {
    object: {
        oid: number,
        tableOid: number 
    }
} | {
    select: {
        oid: number,
        tableOid: number 
    }
} | {
    multiselect: {
        oid: number,
        tableOid: number 
    }
} | {
    formula: {
        oid: number,
        formula: string 
    }
} | {
    subreport: {
        oid: number,
        reportOid: number
    }
};

export type FullMetadata = {
    oid: number,
    hidden: boolean,
    schema: SchemaFullMetadata,
    name: string,
    columnType: ColumnType,
    style: string,
    ordering: number,
    defaultValue: string | null,
    isPrimaryKey: boolean
};


let resizeSetupCallbacks: (() => void)[] = [];

function addResizeSetupCallback(callbackFn: () => void) {
    navigator.locks.request('resize-setup', () => {
        resizeSetupCallbacks.push(callbackFn);
    });
}

export function runResizeSetupCallbacks() {
    navigator.locks.request('resize-setup', () => {
        resizeSetupCallbacks.forEach(callbackFn => callbackFn());
        resizeSetupCallbacks = [];
    });
}


/**
 * Creates a <td> HTMLElement to represent the column header.
 * @param schemaOid The OID of the most granular schema being displayed.
 * @param column The metadata for the column.
 */
export function createColumnHeaderHTML(schemaOid: number, column: FullMetadata): HTMLTableCellElement {
    const columnClassName: string = `column${column.oid}`;

    const elem: HTMLTableCellElement = document.createElement('th');
    elem.classList.add(columnClassName);
    elem.dataset.columnMetadata = JSON.stringify(column);
    elem.innerText = `${(column.isPrimaryKey ? '🔑 ' : '')}${column.name}`;

    // Apply the style to the column
    const columnStylesheet: HTMLStyleElement | null = document.getElementById('column-stylesheet') as HTMLStyleElement;
    columnStylesheet?.insertAdjacentText('beforeend', `.${columnClassName} { ${column.style} } `);

    // Attach context menu
    elem.addEventListener('contextmenu', async (e) => {
        e.preventDefault();

        const contextMenu: Menu = await Menu.new({
            items: await Promise.all([
                MenuItem.new({
                    text: 'Edit Column',
                    action: () => {
                        openDialogAsync({
                            editColumn: {
                                columnOid: column.oid
                            }
                        });
                    }
                }),
                MenuItem.new({
                    text: 'Insert New Column',
                    action: () => {
                        openDialogAsync({
                            createColumn: {
                                schemaOid,
                                columnOrdering: column.ordering
                            }
                        });
                    }
                }),
                MenuItem.new({
                    text: 'Delete Column',
                    action: () => {
                        executeAsync({
                            trashColumn: {
                                schemaOid,
                                columnOid: column.oid
                            }
                        })
                        .catch(async (e) => {
                            await message(e, {
                                title: 'An error occurred while deleting the column.',
                                kind: 'error'
                            });
                        });
                    }
                })
            ])
        });

        await contextMenu.popup()
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while displaying context menu.',
                kind: 'error'
            });
        });
    });

    // Add callback function to setup column resizing
    addResizeSetupCallback(() => {
        interact(`.${columnClassName}`).resizable({
            edges: { right: true },
            onmove(event: ResizeEvent) {
                // Do a temporary resize
                elem.style.width = `${Math.round(event.rect.width)}px`;
            },
            onend(event: ResizeEvent) {
                // Replace the width property in the CSS style
                const widthRe: RegExp = /(?<!\{[^\}]*)(?<=^|[;\{\}])(\s*width\s*:\s*)(?:[^;]|"(?:[^\\"]|\\"|\\\\)*")*;/;
                let newColumnStyle: string = column.style.replace(widthRe, `$1${Math.round(event.rect.width)}px;`);

                // Update the CSS style in the database
                executeAsync({
                    editColumnStyle: {
                        metadata: column,
                        newColumnStyle: newColumnStyle
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'An error occurred while updating column width.',
                        kind: 'error'
                    });
                });
            }
        });
    });

    // Return the column TD
    return elem;
}