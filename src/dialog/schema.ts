import { message } from "@tauri-apps/plugin-dialog";
import { closeDialogAsync } from "../util/dialog";
import { DropdownValue, getReportMetadataAsync, getTableMetadataAsync, queryAsync, ToggledHierarchicalListItemMetadata } from "../util/query";
import { FullMetadata as SchemaFullMetadata } from "../util/schema";
import { FullMetadata as TableFullMetadata } from "../util/table";
import { FullMetadata as ReportFullMetadata } from "../util/report";
import { executeAsync } from "../util/action";
import { listen } from "@tauri-apps/api/event";
import { Channel } from "@tauri-apps/api/core";
import { FullMetadata as ColumnFullMetadata } from "../util/column";

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
                channel: new Channel<ColumnFullMetadata>((column) => {
                    const dropdownValue: DropdownValue = {
                        value: column.oid,
                        label: column.name
                    };
                    columns.push(dropdownValue);
                    callbackFns.forEach((fn) => fn(dropdownValue));
                })
            }
        })
        .then(() => console.debug(JSON.stringify(columns)))
    }
}

function createNewOrderingColumn(columnSortAscending: boolean): HTMLSelectElement {
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
    sortAscendingElem.value = columnSortAscending ? 'asc' : 'desc';
    sortAscendingTdElem.appendChild(sortAscendingElem);
    rowElem.appendChild(sortAscendingTdElem);

    return selectElem;
}

function createNewGroupingColumn(): HTMLSelectElement {

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
        const selectElem: HTMLSelectElement = createNewOrderingColumn(columnSortAscending);

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

                // Populate in the filter formula
                const filterFormulaElem: HTMLTextAreaElement = document.getElementById('schema-filter') as HTMLTextAreaElement;
                filterFormulaElem.value = report.filterFormula ?? '';

                // Populate in the GROUP BY columns
                report.groupByColumnOid.forEach((columnOid) => {
                    const selectElem: HTMLSelectElement = createNewGroupingColumn();
                    callbackFns.push((dropdownValue) => {
                        const optionElem: HTMLOptionElement = document.createElement('option');
                        optionElem.value = dropdownValue.value.toString();
                        optionElem.innerText = dropdownValue.label;
                        if (dropdownValue.value == columnOid) {
                            optionElem.selected = true;
                        }
                        selectElem.appendChild(optionElem);
                    });
                });

                loadColumns(callbackFns);
            });
        }
    } else {
        populateNewSchemaMetadata();
    }

    // Hide tabs if necessary
    if (!schemaOid) {
        // Hide "Sort By" tab
        const sortByTab: HTMLElement = document.getElementById('ordering-tab') as HTMLElement;
        sortByTab.style.display = 'none';

        // Hide "Group By" tab
        const groupByTab: HTMLElement = document.getElementById('grouping-tab') as HTMLElement;
        groupByTab.style.display = 'none';

        // Hide "Filter" tab
        const filterTab: HTMLElement = document.getElementById('filter-tab') as HTMLElement;
        filterTab.style.display = 'none';
    } else if (mode == 'table') {
        // Hide "Group By" tab
        const groupByTab: HTMLElement = document.getElementById('grouping-tab') as HTMLElement;
        groupByTab.style.display = 'none';

        // Hide "Filter" tab
        const filterTab: HTMLElement = document.getElementById('filter-tab') as HTMLElement;
        filterTab.style.display = 'none';
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
    const filterFormulaElem: HTMLTextAreaElement = document.getElementById('schema-filter') as HTMLTextAreaElement;

    let groupByColumnOid: number[] = [];
    document.querySelectorAll('#schema-grouping .schema-grouping-column').forEach((columnElem) => {
        const columnOidElem: HTMLSelectElement = columnElem.querySelector('.column-oid') as HTMLSelectElement;
        const columnOid: number = parseInt(columnOidElem.value);
        if (isFinite(columnOid)) {
            groupByColumnOid.push(columnOid);
        }
    });

    return {
        schema: compileSchema(),
        filterFormula: filterFormulaElem.value ? filterFormulaElem.value : '',
        groupByColumnOid: groupByColumnOid
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
        const selectElem: HTMLSelectElement = createNewOrderingColumn(true);

        columns.forEach((dropdownValue: DropdownValue) => {
            const optionElem: HTMLOptionElement = document.createElement('option');
            optionElem.value = dropdownValue.value.toString();
            optionElem.innerText = dropdownValue.label;
            selectElem.appendChild(optionElem);
        });
    });
    document.getElementById('add-schema-grouping-column-button')?.addEventListener("click", async (e) => {
        e.preventDefault();

        // Create a new GROUP BY row
        const selectElem: HTMLSelectElement = createNewGroupingColumn();

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


listen<number>('schema', (e) => {
    if (e.payload == schemaOid) {
        populateSchemaMetadata();
    }
});
listen<number>('table', (e) => {
    if (e.payload == schemaOid) {
        populateSchemaMetadata();
    }
});
listen<number>('report', (e) => {
    if (e.payload == schemaOid) {
        populateSchemaMetadata();
    }
});