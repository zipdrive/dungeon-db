import { message } from "@tauri-apps/plugin-dialog";
import { Channel } from "@tauri-apps/api/core";
import { getColumnAsync, HierarchicalListItemMetadata, queryAsync } from "../util/query";
import { FullMetadata as ColumnFullMetadata } from "../util/column";

const urlParams = new URLSearchParams(window.location.search);
const mode: 'table' | 'report' = urlParams.get('mode') as ('table' | 'report') ?? 'table';
const urlParamSchemaOid: string | null = urlParams.get('schema_oid');
const schemaOid: number | null = urlParamSchemaOid ? parseInt(urlParamSchemaOid) : null;
const urlParamColumnOrdering: string | null = urlParams.get('column_ordering');
const columnOrdering: number | null = urlParamColumnOrdering ? parseInt(urlParamColumnOrdering) : null;
const urlParamColumnOid: string | null = urlParams.get('column_oid');
const columnOid: number | null = urlParamColumnOid ? parseInt(urlParamColumnOid) : null;

function populatePreexistingColumnMetadata(column: ColumnFullMetadata) {
    // Populate name
    const columnNameElem: HTMLInputElement = document.getElementById('column-name') as HTMLInputElement;
    columnNameElem.value = column.name;

    // Populate column type
    let columnTypeStr: string;
    let defaultTableOid: number | null;
    if ('primitive' in column.columnType) {
        columnTypeStr = `primitive-${column.columnType.primitive}`;
    } else if ('object' in column.columnType) {
        columnTypeStr = 'object';
        defaultTableOid = column.columnType.object.tableOid;
    } else if ('select' in column.columnType) {
        columnTypeStr = 'select';
        defaultTableOid = column.columnType.select.tableOid;
    } else if ('multiselect' in column.columnType) {
        columnTypeStr = 'multiselect';
        defaultTableOid = column.columnType.multiselect.tableOid;
    } else if ('formula' in column.columnType) {
        columnTypeStr = 'formula';

        // Populate the pre-existing formula
        const formulaElem: HTMLTextAreaElement = document.getElementById('') as HTMLTextAreaElement;
        formulaElem.value = column.columnType.formula.formula;
    } else if ('subreport' in column.columnType) {
        columnTypeStr = 'subreport';
    } else {
        columnTypeStr = 'primitive-text'; // This should never happen
    }
    const defaultColumnTypeOption = document.querySelector(`#column-type option[value="${columnTypeStr}"]`) as HTMLOptionElement;
    defaultColumnTypeOption.selected = true;

    // Populate whether column is hidden
    const hiddenElem: HTMLInputElement = document.getElementById('column-hidden') as HTMLInputElement;
    hiddenElem.checked = column.hidden;

    // Populate whether column is a primary key
    const primaryKeyElem: HTMLInputElement = document.getElementById('column-is-primary-key') as HTMLInputElement;
    primaryKeyElem.checked = column.isPrimaryKey;

    // Populate default value
    const defaultValueElem: HTMLInputElement = document.getElementById('column-default-value') as HTMLInputElement;
    defaultValueElem.value = column.defaultValue ?? '';

    // Populate tables that can be referenced by Object/Select/Multiselect column type
    queryAsync({
        tables: {
            channel: new Channel<HierarchicalListItemMetadata>((table) => {
                
            })
        }
    });

    // Populate column style
    const columnStyleElem: HTMLTextAreaElement = document.getElementById('column-style') as HTMLTextAreaElement;
    columnStyleElem.value = column.style;
}

window.addEventListener("DOMContentLoaded", () => {
    // Populate in preexisting metadata, if any
    if (columnOid) {
        getColumnAsync(columnOid).then((column) => {
            populatePreexistingColumnMetadata(column);
        });
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