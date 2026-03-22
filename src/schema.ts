import { message } from "@tauri-apps/plugin-dialog";
import { getReportMetadataAsync, getTableMetadataAsync, queryAsync, ToggledHierarchicalListItemMetadata } from "./util/query";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata, createColumnHeaderHTML, runResizeSetupCallbacks } from "./util/column";
import { Cell, CellOid, createCell, runDropdownValueQueries, updateCell } from "./util/cell";
import { listen } from "@tauri-apps/api/event";
import { openDialogAsync } from "./util/dialog";

import Sortable, { SortableEvent, AutoScroll } from 'sortablejs/modular/sortable.core.esm.js';
Sortable.mount(new AutoScroll());

const urlParams = new URLSearchParams(window.location.search);
const urlParamSchemaOid: string | null = urlParams.get('schema_oid');
const urlParamPageNum: string | null = urlParams.get('page_num');
const urlParamPageSize: string | null = urlParams.get('page_size');
if (urlParamSchemaOid) {
    const schemaOid: number = parseInt(urlParamSchemaOid);
    let pageNum: number = urlParamPageNum ? parseInt(urlParamPageNum) : 1;
    let maxPageNum: number = 100; // TODO dynamically query this value when refreshing cells
    let pageSize: number = urlParamPageSize ? parseInt(urlParamPageSize) : 2000;

    let filters: [string, number][] = [];
    urlParams.forEach((urlParamValue, urlParamKey) => {
        if (urlParamKey != 'schema_oid' && urlParamKey != 'page_num' && urlParamKey != 'page_size') {
            filters.push([urlParamKey, parseInt(urlParamValue)]);
        }
    });


    /**
     * Reloads all cells in the schema.
     */
    function reloadAllCells() {
        navigator.locks.request('page-content', async () => {
            const page: HTMLElement = document.getElementById('page') as HTMLElement;
            const pageBottomSpacer: HTMLElement = document.getElementById('page-bottom-spacer') as HTMLElement;
            pageBottomSpacer.style.height = '50%';
            const pageHeight: number = page.scrollHeight;
            const pageScrollTop: number = page.scrollTop;

            // Update page number
            const pageNumInput: HTMLInputElement = document.getElementById('page-num-input') as HTMLInputElement;
            pageNumInput.value = `${pageNum}`;

            // Update page size
            const pageSizeInput: HTMLInputElement = document.getElementById('page-size-input') as HTMLInputElement;
            pageSizeInput.value = `${pageSize}`;

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
                //console.debug(`COLUMN: ${JSON.stringify(column)}`);
                const elem: HTMLTableCellElement = createColumnHeaderHTML(schemaOid, column);
                elem.classList.add('reorderable-column');
                columnHeaderRow.appendChild(elem);
            });

            // Construct body
            let currentRow: HTMLElement = pageContentBody;
            const cellChannel: Channel<Cell & { maxIndex: number }> = new Channel<Cell & { maxIndex: number }>((cell) => {
                //console.debug(`CELL: ${JSON.stringify(cell)}`);
                if ('maxIndex' in cell) {
                    maxPageNum = 1 + Math.floor(cell.maxIndex / pageSize);
                } else {
                    const elem: HTMLTableRowElement | HTMLTableCellElement | null = createCell(cell, true, filters);
                    if (elem) {
                        if (elem.nodeName == 'TR') {
                            currentRow = elem;
                            pageContentBody.appendChild(elem);
                        } else {
                            currentRow.appendChild(elem);
                        }
                    }
                }
            });

            // Query for columns and cells
            await queryAsync({
                cells: {
                    schemaOid: schemaOid,
                    filters: filters,
                    limit: {
                        page: {
                            num: pageNum,
                            size: pageSize
                        }
                    },
                    columnChannel: columnChannel,
                    cellChannel: cellChannel
                }
            });

            // Once all cells have been created, query for dropdown values
            await runDropdownValueQueries();
            runResizeSetupCallbacks();

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

            // Enable or disable page navigation buttons
            const firstPageButton: HTMLButtonElement = document.getElementById('first-page-button') as HTMLButtonElement;
            firstPageButton.disabled = pageNum <= 1;
            const prevPageButton: HTMLButtonElement = document.getElementById('prev-page-button') as HTMLButtonElement;
            prevPageButton.disabled = pageNum <= 1;
            const nextPageButton: HTMLButtonElement = document.getElementById('next-page-button') as HTMLButtonElement;
            nextPageButton.disabled = pageNum >= maxPageNum;
            const lastPageButton: HTMLButtonElement = document.getElementById('last-page-button') as HTMLButtonElement;
            lastPageButton.disabled = pageNum >= maxPageNum;

            // Return the page to where it was scrolled before
            if (pageHeight > page.scrollHeight) {
                pageBottomSpacer.style.height = `calc(50% + ${Math.ceil(pageHeight - page.scrollHeight)}px)`;
            }
            page.scrollTop = pageScrollTop;
        });
    }


    window.addEventListener("DOMContentLoaded", () => {
        reloadAllCells();

        // Listen for manual input of page number
        const pageNumInput: HTMLInputElement = document.getElementById('page-num-input') as HTMLInputElement;
        pageNumInput.addEventListener('change', () => {
            const newPageNum: number = parseInt(pageNumInput.value);
            if (newPageNum > 0 && isFinite(newPageNum)) {
                pageNum = newPageNum;
                reloadAllCells();
            } else {
                pageNumInput.value = `${pageNum}`;
            }
        });

        // Listen for manual input of page size
        const pageSizeInput: HTMLInputElement = document.getElementById('page-size-input') as HTMLInputElement;
        pageSizeInput.addEventListener('change', () => {
            const newPageSize: number = parseInt(pageSizeInput.value);
            if (newPageSize > 0 && isFinite(newPageSize)) {
                pageSize = newPageSize;
                reloadAllCells();
            } else {
                pageSizeInput.value = `${pageSize}`;
            }
        });

        // Listen for buttons adjusting page number
        const firstPageButton: HTMLElement = document.getElementById('first-page-button') as HTMLElement;
        firstPageButton.addEventListener('click', () => {
            pageNum = 1;
            reloadAllCells();
        });
        const prevPageButton: HTMLElement = document.getElementById('prev-page-button') as HTMLElement;
        prevPageButton.addEventListener('click', () => {
            pageNum = Math.max(pageNum - 1, 1);
            reloadAllCells();
        });
        const nextPageButton: HTMLElement = document.getElementById('next-page-button') as HTMLElement;
        nextPageButton.addEventListener('click', () => {
            pageNum = Math.min(pageNum + 1, maxPageNum);
            reloadAllCells();
        });
        const lastPageButton: HTMLElement = document.getElementById('last-page-button') as HTMLElement;
        lastPageButton.addEventListener('click', () => {
            pageNum = maxPageNum;
            reloadAllCells();
        });
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
        updateCell(e.payload, true);
    });
}
