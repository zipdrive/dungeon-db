import { message } from "@tauri-apps/plugin-dialog";
import { getReportMetadataAsync, getTableMetadataAsync, queryAsync, SelectedHierarchicalListItemMetadata, ToggledHierarchicalListItemMetadata } from "./util/query";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata, createColumnHeaderHTML } from "./util/column";
import { Cell, ValueOid, createCellAsync, runDropdownValueQueries, updateCell } from "./util/cell";
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

            let objectSchemaOid: number = schemaOid;
            if (filters.length == 1) {
                // Construct a selector for the object type
                const objectTypeRow: HTMLTableRowElement = document.createElement('tr');
                const objectTypeLabel: HTMLTableCellElement = document.createElement('td');
                objectTypeLabel.innerText = 'Object Type';
                objectTypeRow.appendChild(objectTypeLabel);
                const objectTypeValue: HTMLTableCellElement = document.createElement('td');
                const objectTypeSelect: HTMLSelectElement = document.createElement('select');
                objectTypeSelect.classList.add('input');
                objectTypeValue.appendChild(objectTypeSelect);
                objectTypeRow.appendChild(objectTypeValue);
                pageContentBody.appendChild(objectTypeRow);

                // Query for object types
                await queryAsync({
                    inheritorTables: {
                        tableOid: schemaOid,
                        rowOid: filters[0][1],
                        channel: new Channel<SelectedHierarchicalListItemMetadata>((dropdownValue) => {
                            const objectTypeOption: HTMLOptionElement = document.createElement('option');
                            objectTypeOption.value = `${dropdownValue.oid}:${dropdownValue.masterOid}`;
                            objectTypeOption.innerText = `${' '.repeat(dropdownValue.level)}${dropdownValue.name}`;
                            if (dropdownValue.selected) {
                                objectTypeOption.selected = true;
                                objectSchemaOid = dropdownValue.oid;
                            }
                            objectTypeSelect.appendChild(objectTypeOption);
                        })
                    }
                });

                // Add event listener for when object type is changed
                

                // Add a horizontal line to separate
                pageContentBody.insertAdjacentHTML('beforeend', '<tr><td colspan="2"><hr></td></tr>');
            }

            // Construct row for each column
            const columnChannel: Channel<ColumnFullMetadata> = new Channel<ColumnFullMetadata>((column) => {
                console.debug(`COLUMN: ${JSON.stringify(column)}`);
                const columnRow: HTMLElement = document.createElement('tr');
                const elem: HTMLTableCellElement = createColumnHeaderHTML(schemaOid, column);
                columnRow.appendChild(elem);
                pageContentBody.appendChild(columnRow);
            });

            // Construct body
            const cellChannel: Channel<Cell | { maxIndex: number }> = new Channel<Cell | { maxIndex: number }>((cell) => {
                console.debug(`CELL: ${JSON.stringify(cell)}`);
                if ('maxIndex' in cell) {
                    // Ignore
                } else {
                    const elem: HTMLTableRowElement | HTMLTableCellElement | null = createCellAsync(cell, false, filters);
                    if (elem) {
                        if (elem.nodeName == 'TR') {
                            // Ignore
                        } else {
                            // Extract the column OID
                            let columnOid: number;
                            if ('readonly' in cell) {
                                columnOid = cell.readonly.cellOid.columnOid;
                            } else if ('subreport' in cell) {
                                columnOid = cell.subreport.cellOid.columnOid;
                            } else if ('primitiveEntry' in cell) {
                                columnOid = cell.primitiveEntry.cellOid.columnOid;
                            } else if ('fileEntry' in cell) {
                                columnOid = cell.fileEntry.cellOid.columnOid;
                            } else if ('object' in cell) {
                                columnOid = cell.object.cellOid.columnOid;
                            } else if ('selectEntry' in cell) {
                                columnOid = cell.selectEntry.cellOid.columnOid;
                            } else if ('multiselectEntry' in cell) {
                                columnOid = cell.multiselectEntry.cellOid.columnOid;
                            } else {
                                throw new Error(`Cell type ${JSON.stringify(cell)} is not covered.`);
                            }

                            // Add the cell to the appropriate row
                            document.querySelector(`#page-content > tbody > tr:has(.column${columnOid})`)?.appendChild(elem);
                        }
                    }
                }
            });

            // Query for columns and cells
            console.debug(`Schema OID: ${objectSchemaOid}\nFilters: ${filters}`);
            await queryAsync({
                cells: {
                    schemaOid: objectSchemaOid,
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
    listen<ValueOid>('cell', (e) => {
        console.debug(`cellOid: ${JSON.stringify(e.payload)}`);
        updateCell(e.payload, false);
    });
}
