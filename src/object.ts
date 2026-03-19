import { message } from "@tauri-apps/plugin-dialog";
import { getReportMetadataAsync, getTableMetadataAsync, queryAsync, ToggledHierarchicalListItemMetadata } from "./util/query";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata, createColumnHeaderHTML } from "./util/column";
import { Cell, CellOid, createCell, runDropdownValueQueries, updateCell } from "./util/cell";
import { listen } from "@tauri-apps/api/event";
import { openDialogAsync } from "./util/dialog";

const urlParams = new URLSearchParams(window.location.search);
const urlParamSchemaOid: string | null = urlParams.get('schema_oid');
if (urlParamSchemaOid) {
    const schemaOid: number = parseInt(urlParamSchemaOid);

    let filters: [string, number][] = [];
    urlParams.forEach((urlParamValue, urlParamKey) => {
        if (urlParamKey != 'schema_oid') {
            filters.push([urlParamKey, parseInt(urlParamValue)]);
        }
    });


    /**
     * Reloads all cells in the schema.
     */
    function reloadAllCells() {
        navigator.locks.request('page-content', async () => {
            // Reset the table
            const pageContentElem: HTMLTableElement = document.getElementById('page-content') as HTMLTableElement;
            pageContentElem.innerHTML = '';
            const pageContentBody: HTMLElement = document.createElement('tbody');
            pageContentElem.appendChild(pageContentBody);
            const pageContentHead: HTMLElement = document.createElement('thead');
            pageContentElem.appendChild(pageContentHead);
            const pageContentFoot: HTMLElement = document.createElement('tfoot');
            pageContentElem.appendChild(pageContentFoot);

            // Construct header
            const columnHeaderRow: HTMLElement = document.createElement('tr');
            columnHeaderRow.appendChild(document.createElement('th'));
            pageContentHead.appendChild(columnHeaderRow);
            const columnChannel: Channel<ColumnFullMetadata> = new Channel<ColumnFullMetadata>((column) => {
                console.debug(`COLUMN: ${JSON.stringify(column)}`);
                const elem: HTMLTableCellElement = createColumnHeaderHTML(schemaOid, column);
                elem.classList.add('resizable-column');
                elem.classList.add('reorderable-column');
                columnHeaderRow.appendChild(elem);
            });

            // Construct body
            let currentRow: HTMLElement = pageContentBody;
            const cellChannel: Channel<Cell & { maxIndex: number }> = new Channel<Cell & { maxIndex: number }>((cell) => {
                console.debug(`CELL: ${JSON.stringify(cell)}`);
                if ('maxIndex' in cell) {
                    // Ignore
                } else {
                    const elem: HTMLTableRowElement | HTMLTableCellElement = createCell(cell, false, filters);
                    if (elem.nodeName == 'TR') {
                        currentRow = elem;
                        pageContentBody.appendChild(elem);
                    } else {
                        currentRow.appendChild(elem);
                    }
                }
            });

            // Query for columns and cells
            await queryAsync({
                cells: {
                    schemaOid: schemaOid,
                    filters: filters,
                    limit: {
                        singleRow: null
                    },
                    columnChannel: columnChannel,
                    cellChannel: cellChannel
                }
            });

            // Once all cells have been created, query for dropdown values
            await runDropdownValueQueries();

            // Add button to add new column
            const addNewColumnButton: HTMLTableCellElement = document.createElement('th');
            addNewColumnButton.classList.add('clickable-text');
            addNewColumnButton.style.whiteSpace = 'nowrap';
            addNewColumnButton.style.width = '8em';
            addNewColumnButton.innerText = 'Add New Column';
            addNewColumnButton.addEventListener('click', () => {
                openDialogAsync({
                    createColumn: {
                        schemaOid: schemaOid,
                        columnOrdering: null
                    }
                });
            });
            columnHeaderRow.appendChild(addNewColumnButton);
        });
    }


    window.addEventListener("DOMContentLoaded", () => {
        reloadAllCells();
    });

    listen<number>('schema', (e) => {
        if (e.payload == schemaOid) {
            reloadAllCells();
        }
    });
    listen<number>('table', (e) => {
        if (e.payload == schemaOid) {
            reloadAllCells();
        }
    });
    listen<number>('report', (e) => {
        if (e.payload == schemaOid) {
            reloadAllCells();
        }
    });
    listen<CellOid>('cell', (e) => {
        console.debug(`cellOid: ${JSON.stringify(e.payload)}`);
        updateCell(e.payload, false);
    });
}
