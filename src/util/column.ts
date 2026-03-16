import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { FullMetadata as SchemaFullMetadata } from "./schema";
import { openDialogAsync } from "./dialog";
import { executeAsync } from "./action";
import { message } from "@tauri-apps/plugin-dialog";

export type Primitive = 'text' | 'integer' | 'number' | 'checkbox' | 'date' | 'datetime' | 'file' | 'image' | 'jSON';

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
    isNullable: boolean,
    isUnique: boolean,
    isPrimaryKey: boolean
};


/**
 * Creates a <td> HTMLElement to represent the column header.
 * @param schemaOid The OID of the most granular schema being displayed.
 * @param column The metadata for the column.
 */
export function createColumnHeaderHTML(schemaOid: number, column: FullMetadata): HTMLTableCellElement {
    const columnClassName: string = `column${column.oid}`;

    const elem: HTMLTableCellElement = document.createElement('td');
    elem.classList.add(columnClassName);
    elem.innerText = `${(column.isPrimaryKey ? '🔑 ' : '')}${column.name}`;

    // Apply the style to the column
    const columnStylesheet: HTMLStyleElement | null = document.getElementById('column-stylesheet') as HTMLStyleElement;
    columnStylesheet?.insertAdjacentText('beforeend', `.${columnClassName} { ${column.style} } `);

    // Attach context menu
    elem.addEventListener('contextmenu', async () => {
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
                                schemaOid: schemaOid,
                                columnOrdering: column.ordering
                            }
                        });
                    }
                }),
                MenuItem.new({
                    text: 'Delete Column',
                    action: () => {
                        executeAsync({
                            trashColumn: column.oid
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

    // Return the column TD
    return elem;
}