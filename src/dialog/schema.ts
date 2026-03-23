import { message } from "@tauri-apps/plugin-dialog";
import { closeDialogAsync } from "../util/dialog";
import { DropdownValue, getReportMetadataAsync, getTableMetadataAsync, queryAsync, ToggledHierarchicalListItemMetadata } from "../util/query";
import { FullMetadata as SchemaFullMetadata } from "../util/schema";
import { FullMetadata as TableFullMetadata } from "../util/table";
import { FullMetadata as ReportFullMetadata } from "../util/report";
import { executeAsync } from "../util/action";
import { listen } from "@tauri-apps/api/event";
import { Channel } from "@tauri-apps/api/core";

const urlParams = new URLSearchParams(window.location.search);
const mode: 'table' | 'report' = urlParams.get('mode') as ('table' | 'report') ?? 'table';
const urlParamSchemaOid: string | null = urlParams.get('schema_oid');
const schemaOid: number | null = urlParamSchemaOid ? parseInt(urlParamSchemaOid) : null;


let columns: DropdownValue[] = [];
function loadColumns(callbackFns: ((dropdownValue: DropdownValue) => void)[]) {
    columns = [];
    if (schemaOid) {
        queryAsync({
            columns: {
                schemaOid: schemaOid,
                channel: new Channel<DropdownValue>((dropdownValue) => {
                    columns.push(dropdownValue);
                    callbackFns.forEach((fn) => fn(dropdownValue));
                })
            }
        });
    }
}


function populateNewSchemaMetadata() {
    // Query the list of master schemas
    const masterSchemaSelect: HTMLSelectElement = document.getElementById('master-schema-select') as HTMLSelectElement;
    queryAsync({
        masterSchemas: {
            schemaOid: null,
            isTable: mode == 'table',
            channel: new Channel<ToggledHierarchicalListItemMetadata>((masterSchema) => {
                let elem: HTMLOptionElement = document.createElement('option');
                elem.value = `${masterSchema.oid}:${masterSchema.masterOid}`;
                elem.innerText = `${' '.repeat(masterSchema.level)}${masterSchema.name}`;
                elem.disabled = masterSchema.disabled;

                masterSchemaSelect.appendChild(elem);
            })
        }
    });

    // Load all columns for ORDER BY and GROUP BY dropdowns
    loadColumns([]);
}

function populatePreexistingSchemaMetadata(schema: SchemaFullMetadata): ((dropdownValue: DropdownValue) => void)[] {
    const nameInput: HTMLInputElement = document.getElementById('schema-name') as HTMLInputElement;
    nameInput.value = schema.name;

    // Query the list of master schemas
    const masterSchemaSelect: HTMLSelectElement = document.getElementById('master-schema-select') as HTMLSelectElement;
    queryAsync({
        masterSchemas: {
            schemaOid: schema.oid,
            isTable: mode == 'table',
            channel: new Channel<ToggledHierarchicalListItemMetadata>((masterSchema) => {
                let elem: HTMLOptionElement = document.createElement('option');
                elem.value = `${masterSchema.oid}:${masterSchema.masterOid}`;
                elem.innerText = `${' '.repeat(masterSchema.level)}${masterSchema.name}`;
                elem.disabled = masterSchema.disabled;
                elem.selected = schema.masterSchemaOids.findIndex((masterSchemaOid) => masterSchemaOid == masterSchema.oid) >= 0;

                masterSchemaSelect.appendChild(elem);
            })
        }
    });

    // Create a row for each ORDER BY column
    const schemaOrderingElem: HTMLElement = document.querySelector('#schema-ordering > tbody') as HTMLElement;
    schemaOrderingElem.innerHTML = '';
    return schema.orderByColumnOids.map(([columnOid, columnSortAscending]) => {
        const rowElem: HTMLTableRowElement = document.createElement('tr');
        rowElem.classList.add('schema-ordering-column');
        rowElem.innerHTML = '<td><img class="orderby-column-icon" src="/src-tauri/icons/swap_vert.png"></td>';
        schemaOrderingElem.appendChild(rowElem);

        const selectTdElem: HTMLTableCellElement = document.createElement('td');
        const selectElem: HTMLSelectElement = document.createElement('select');
        selectElem.classList.add('input');
        selectElem.classList.add('column-oid');
        selectTdElem.appendChild(selectElem);
        rowElem.appendChild(selectTdElem);

        const sortAscendingTdElem: HTMLTableCellElement = document.createElement('td');
        const sortAscendingElem: HTMLSelectElement = document.createElement('select');
        sortAscendingElem.classList.add('input');
        sortAscendingElem.classList.add('column-ascending');
        sortAscendingElem.innerHTML = '<option value="asc">ASCENDING</option><option value="desc">DESCENDING</option>';
        sortAscendingElem.value = columnSortAscending ? 'asc' : 'desc';
        sortAscendingTdElem.appendChild(sortAscendingElem);
        rowElem.appendChild(sortAscendingTdElem);

        return (dropdownValue: DropdownValue) => {
            const optionElem: HTMLOptionElement = document.createElement('option');
            optionElem.value = dropdownValue.value.toString();
            optionElem.innerText = dropdownValue.label;
            if (dropdownValue.value == columnOid) {
                optionElem.selected = true;
            }
            selectElem.appendChild(optionElem);
        };
    });
}

function populateSchemaMetadata() {
    if (schemaOid) {
        if (mode == 'table') {
            getTableMetadataAsync(schemaOid).then((table) => {
                let callbackFns: ((dropdownValue: DropdownValue) => void)[] = populatePreexistingSchemaMetadata(table.schema);
                loadColumns(callbackFns);
            });
        } else {
            getReportMetadataAsync(schemaOid).then((report) => {
                let callbackFns: ((dropdownValue: DropdownValue) => void)[] = populatePreexistingSchemaMetadata(report.schema);
                loadColumns(callbackFns);
            });
        }
    } else {
        populateNewSchemaMetadata();
    }
}

/**
 * Compiles the inputted metadata for any schema.
 */
function compileSchema(): SchemaFullMetadata {
    const nameInput: HTMLInputElement = document.getElementById('schema-name') as HTMLInputElement;
    
    // Compile master schemas
    let masterSchemaOids: number[] = [];
    const masterSchemaSelect: HTMLSelectElement = document.getElementById('master-schema-select') as HTMLSelectElement;
    for (let masterOption of masterSchemaSelect.selectedOptions) {
        const value: string = (masterOption as HTMLOptionElement).value;
        const selectedSchemaOid: number = parseInt(value.split(':')[0]);
        masterSchemaOids.push(selectedSchemaOid);
    }

    // Compile ORDER BY columns
    let orderByColumnOids: [number, boolean][] = [];
    for (let orderByColumn of document.querySelectorAll('#schema-ordering .schema-ordering-column')) {
        const orderByColumnSelectElem: HTMLSelectElement = orderByColumn.querySelector('.column-oid') as HTMLSelectElement;
        if (orderByColumnSelectElem.value) {
            const orderByColumnAscendingElem: HTMLSelectElement = orderByColumn.querySelector('.column-ascending') as HTMLSelectElement;
            orderByColumnOids.push([parseInt(orderByColumnSelectElem.value), orderByColumnAscendingElem.value == 'asc']);
        }
    }

    return {
        oid: schemaOid ?? 0,
        name: nameInput.value,
        masterSchemaOids: masterSchemaOids,
        orderByColumnOids: orderByColumnOids
    }
}

/**
 * Compiles the inputted metadata for a table.
 */
function compileTable(): TableFullMetadata {
    return {
        schema: compileSchema()
    };
}

/**
 * Compiles the inputted metadata for a report.
 */
function compileReport(): ReportFullMetadata {
    return {
        schema: compileSchema()
    };
}

/**
 * Creates a new schema.
 */
async function createAsync(): Promise<void> {
    if (mode == 'table') {
        const table = compileTable();
        executeAsync({
            createTable: table
        })
        .then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while creating the new table.',
                kind: 'error'
            });
        });
    } else {
        const report = compileReport();
        executeAsync({
            createReport: report
        })
        .then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while creating the new report.',
                kind: 'error'
            });
        });
    }
}

/**
 * Edits an existing schema.
 */
async function editAsync(): Promise<void> {
    if (mode == 'table') {
        const table = compileTable();
        executeAsync({
            editTable: table
        })
        .then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while editing table.',
                kind: 'error'
            });
        });
    } else {
        const report = compileReport();
        executeAsync({
            createReport: report
        })
        .then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while editing report.',
                kind: 'error'
            });
        });
    }
}

window.addEventListener("DOMContentLoaded", () => {
    // Populate in preexisting metadata, if any
    populateSchemaMetadata();

    // Create listeners for the buttons
    document.getElementById('add-schema-ordering-column-button')?.addEventListener("click", async (e) => {
        e.preventDefault();

        // Create a new ORDER BY row
        const schemaOrderingElem: HTMLElement = document.querySelector('#schema-ordering > tbody') as HTMLElement;
        const rowElem: HTMLTableRowElement = document.createElement('tr');
        rowElem.classList.add('schema-ordering-column');
        rowElem.innerHTML = '<td><img class="orderby-column-icon" src="/src-tauri/icons/swap_vert.png"></td>';
        schemaOrderingElem.appendChild(rowElem);

        const selectTdElem: HTMLTableCellElement = document.createElement('td');
        const selectElem: HTMLSelectElement = document.createElement('select');
        selectElem.classList.add('input');
        selectElem.classList.add('column-oid');
        selectTdElem.appendChild(selectElem);
        rowElem.appendChild(selectTdElem);

        const sortAscendingTdElem: HTMLTableCellElement = document.createElement('td');
        const sortAscendingElem: HTMLSelectElement = document.createElement('select');
        sortAscendingElem.classList.add('input');
        sortAscendingElem.classList.add('column-ascending');
        sortAscendingElem.innerHTML = '<option value="asc">ASCENDING</option><option value="desc">DESCENDING</option>';
        sortAscendingTdElem.appendChild(sortAscendingElem);
        rowElem.appendChild(sortAscendingTdElem);

        columns.forEach((dropdownValue: DropdownValue) => {
            const optionElem: HTMLOptionElement = document.createElement('option');
            optionElem.value = dropdownValue.value.toString();
            optionElem.innerText = dropdownValue.label;
            selectElem.appendChild(optionElem);
        });
    });
    document.getElementById('confirm-button')?.addEventListener("click", async (e) => {
        e.preventDefault();

        if (schemaOid) {
            // Edit the schema's metadata
            await editAsync();
        } else {
            // Create a new schema
            await createAsync();
        }
    });
    document.getElementById('cancel-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        await closeDialogAsync();
    });
});


listen<number>('schema', (updateSchemaOid) => {
    if (updateSchemaOid == schemaOid) {
        populateSchemaMetadata();
    }
});
listen<number>('table', (updateSchemaOid) => {
    if (updateSchemaOid == schemaOid) {
        populateSchemaMetadata();
    }
});
listen<number>('report', (updateSchemaOid) => {
    if (updateSchemaOid == schemaOid) {
        populateSchemaMetadata();
    }
});