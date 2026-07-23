import { message } from "@tauri-apps/plugin-dialog";
import { queryAsync } from "./util/query";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata } from "./util/column";
import { CellContent, CellStream } from "./util/cell";
import { Grid } from "./util/grid";
import { listen } from "@tauri-apps/api/event";
import { openDialogAsync } from "./util/dialog";
import "./util/shortcut"; // Install shortcuts

const urlParams = new URLSearchParams(window.location.search);
const urlParamSchemaOid: string | null = urlParams.get('schema_oid');
const urlParamPageNum: string | null = urlParams.get('page_num');
const urlParamPageSize: string | null = urlParams.get('page_size');
if (urlParamSchemaOid) {
    const schemaOid: number = parseInt(urlParamSchemaOid);

    // Update page number
    const pageNum: number = urlParamPageNum ? parseInt(urlParamPageNum) : 1;
    const pageNumInput: HTMLInputElement = document.getElementById('page-num-input') as HTMLInputElement;
    pageNumInput.value = `${pageNum}`;

    // Update page size
    const pageSize: number = urlParamPageSize ? parseInt(urlParamPageSize) : 2000;
    const pageSizeInput: HTMLInputElement = document.getElementById('page-size-input') as HTMLInputElement;
    pageSizeInput.value = `${pageSize}`;

    // Get filters from query parameters
    const filters: [string, number][] = [];
    urlParams.forEach((urlParamValue, urlParamKey) => {
        if (!new Set([
            'schema_oid', 
            'page_num', 
            'page_size', 
            'scroll_top', 
            'scroll_left', 
            'selected_row_index', 
            'selected_column_index'
        ]).has(urlParamKey)) {
            filters.push([urlParamKey, parseInt(urlParamValue)]);
        }
    });



    window.addEventListener("DOMContentLoaded", async () => {
        // Listen for manual input of page number
        const pageNumInput: HTMLInputElement = document.getElementById('page-num-input') as HTMLInputElement;
        pageNumInput.addEventListener('change', () => {
            const newPageNum: number = parseInt(pageNumInput.value);
            if (newPageNum > 0 && isFinite(newPageNum)) {
                urlParams.set('page_num', newPageNum.toString());
                reload();
            } else {
                pageNumInput.value = `${pageNum}`;
            }
        });

        // Listen for manual input of page size
        const pageSizeInput: HTMLInputElement = document.getElementById('page-size-input') as HTMLInputElement;
        pageSizeInput.addEventListener('change', () => {
            const newPageSize: number = parseInt(pageSizeInput.value);
            if (newPageSize > 0 && isFinite(newPageSize)) {
                urlParams.set('page_size', newPageSize.toString());
                reload();
            } else {
                pageSizeInput.value = `${pageSize}`;
            }
        });
        
        // Construct the grid
        const grid: Grid = new Grid({
            iframe: document.getElementById('page') as HTMLIFrameElement
        });
        function reload() {
            // Record the position that the grid has been scrolled from the top
            const scrollTop: number = grid.scrollTop;
            urlParams.set('scroll_top', scrollTop.toString());
            // Record the position that the grid has been scrolled from the left
            const scrollLeft: number = grid.scrollLeft;
            urlParams.set('scroll_left', scrollLeft.toString());

            // Reload the page
            window.location.href = `/src/schema.html?${urlParams}`;
        }

        const firstPageButton: HTMLButtonElement = document.getElementById('first-page-button') as HTMLButtonElement;
        const prevPageButton: HTMLButtonElement = document.getElementById('prev-page-button') as HTMLButtonElement;
        if (pageNum > 1) {
            // Add listeners to first page and prev page buttons
            firstPageButton.addEventListener('click', () => {
                urlParams.set('page_num', '1');
                reload();
            });
            prevPageButton.addEventListener('click', () => {
                urlParams.set('page_num', (pageNum - 1).toString());
                reload();
            });
            grid.setPrevHref(() => {
                urlParams.set('page_num', (pageNum - 1).toString());
                reload();
            });
        } else {
            // Disable the first page and prev page buttons
            firstPageButton.disabled = true;
            prevPageButton.disabled = true;
        }

        // Query for schema page data
        const columnChannel: Channel<ColumnFullMetadata> = new Channel<ColumnFullMetadata>((columnMetadata) => {
            const column = grid.addColumn(schemaOid, columnMetadata);
            listen<[number, ColumnFullMetadata]>('column', (e) => {
                const [oldColumnOid, newMetadata] = e.payload;
                if (oldColumnOid === column.metadata.oid) {
                    if (!column.hotReload(newMetadata)) {
                        // Column cannot be hot reloaded, needs entire schema to be refreshed
                        reload();
                    }
                }
            });
        });
        const cellChannel: Channel<CellStream> = new Channel<CellStream>((streamedCellContent) => {
            if ('maxIndex' in streamedCellContent) {
                // Set the max page number
                const maxPageNum = 1 + Math.floor(streamedCellContent.maxIndex / pageSize);
                console.debug(`Max index: ${streamedCellContent.maxIndex}, Max page num: ${maxPageNum}`);
                
                const nextPageButton: HTMLButtonElement = document.getElementById('next-page-button') as HTMLButtonElement;
                const lastPageButton: HTMLButtonElement = document.getElementById('last-page-button') as HTMLButtonElement;
                if (pageNum < maxPageNum) {
                    // Create listeners for next page button and last page button
                    nextPageButton.addEventListener('click', () => {
                        urlParams.set('page_num', (pageNum + 1).toString());
                        reload();
                    });
                    lastPageButton.addEventListener('click', () => {
                        urlParams.set('page_num', maxPageNum.toString());
                        reload();
                    });
                    grid.setNextHref(() => {
                        urlParams.set('page_num', (pageNum + 1).toString());
                        reload();
                    });
                } else {
                    // Disable the next page and last page buttons
                    nextPageButton.disabled = true;
                    lastPageButton.disabled = true;
                }
            } else if ('row' in streamedCellContent) {
                // Add row to grid
                grid.addRow(streamedCellContent.row);
            } else if ('addNewRowButton' in streamedCellContent) {
                // Construct an "Add New Row" button
                grid.addNewRowButton(streamedCellContent.addNewRowButton);
            } else {
                console.debug(streamedCellContent.cell);
                try {
                    // Add cell to last row of grid
                    const cellContent: CellContent = streamedCellContent.cell;
                    grid.addCellContentToRow(cellContent, async () => {
                        reload();
                    });
                } catch (e) {
                    console.debug(e);
                }
            }
        });
        await queryAsync({
            'cells': {
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
        })
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while querying data from schema.',
                kind: 'error'
            });
        });

        // Build the grid
        const urlParamScrollTop: string | null = urlParams.get('scroll_top');
        const scrollTop: number = urlParamScrollTop ? parseInt(urlParamScrollTop) : 0;
        const urlParamScrollLeft: string | null = urlParams.get('scroll_left');
        const scrollLeft: number = urlParamScrollLeft ? parseInt(urlParamScrollLeft) : 0;
        const urlParamRowIndex: string | null = urlParams.get('selected_row_index');
        const rowIndex: number = urlParamRowIndex ? parseInt(urlParamRowIndex) : 0;
        const urlParamColumnIndex: string | null = urlParams.get('selected_column_index');
        const columnIndex: number = urlParamColumnIndex ? parseInt(urlParamColumnIndex) : 0;
        grid.build({
            scrollTop,
            scrollLeft,
            rowIndex,
            columnIndex,
            constructAdditionalColumns(cwd): HTMLElement[] {
                const addNewColumn: HTMLElement = cwd.createElement('TH');
                addNewColumn.classList.add('clickable-text', 'one-line');
                addNewColumn.innerText = 'Add New Column';
                addNewColumn.style.width = '10em';
                addNewColumn.addEventListener('click', async () => {
                    await openDialogAsync({
                        createColumn: {
                            schemaOid: schemaOid,
                            columnOrdering: null
                        }
                    });
                });
                return [addNewColumn];
            }
        });

        // Reload page when the schema is updated
        listen<number[]>('schema', (e) => {
            console.debug(`One or more schemas have been updated: ${e.payload}`);
            if (e.payload.some(s => s == schemaOid)) {
                reload();
            }
        });
    });
}
