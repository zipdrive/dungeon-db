import { message } from "@tauri-apps/plugin-dialog";
import { closeDialogAsync } from "../util/dialog";
import { getReportMetadataAsync, getTableMetadataAsync, queryAsync, ToggledHierarchicalListItemMetadata } from "../util/query";
import { FullMetadata as SchemaFullMetadata } from "../util/schema";
import { FullMetadata as TableFullMetadata } from "../util/table";
import { FullMetadata as ReportFullMetadata } from "../util/report";
import { executeAsync } from "../util/action";
import { Channel } from "@tauri-apps/api/core";

const urlParams = new URLSearchParams(window.location.search);
const mode: 'table' | 'report' = urlParams.get('mode') as ('table' | 'report') ?? 'table';
const urlParamSchemaOid: string | null = urlParams.get('schema_oid');
const schemaOid: number | null = urlParamSchemaOid ? parseInt(urlParamSchemaOid) : null;

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
}

function populatePreexistingSchemaMetadata(schema: SchemaFullMetadata) {
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
}

/**
 * Compiles the inputted metadata for any schema.
 */
function compileSchema(): SchemaFullMetadata {
    const nameInput: HTMLInputElement = document.getElementById('schema-name') as HTMLInputElement;
    
    let masterSchemaOids: number[] = [];
    const masterSchemaSelect: HTMLSelectElement = document.getElementById('master-schema-select') as HTMLSelectElement;
    for (let masterOption of masterSchemaSelect.selectedOptions) {
        const value: string = (masterOption as HTMLOptionElement).value;
        const selectedSchemaOid: number = parseInt(value.split(':')[0]);
        masterSchemaOids.push(selectedSchemaOid);
    }

    return {
        oid: schemaOid ?? 0,
        name: nameInput.value,
        masterSchemaOids: masterSchemaOids
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
    if (schemaOid) {
        if (mode == 'table') {
            getTableMetadataAsync(schemaOid).then((table) => {
                populatePreexistingSchemaMetadata(table.schema);
            });
        } else {
            getReportMetadataAsync(schemaOid).then((report) => {
                populatePreexistingSchemaMetadata(report.schema);
            });
        }
    } else {
        populateNewSchemaMetadata();
    }

    // Create listeners for the buttons
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