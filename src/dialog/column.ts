import { message } from "@tauri-apps/plugin-dialog";
import { Channel } from "@tauri-apps/api/core";
import { DropdownValue, getColumnAsync, HierarchicalListItemMetadata, queryAsync } from "../util/query";
import { FullMetadata as ColumnFullMetadata, ColumnType, Primitive } from "../util/column";
import { closeDialogAsync } from "../util/dialog";
import { executeAsync } from "../util/action";

const urlParams = new URLSearchParams(window.location.search);
const mode: 'table' | 'report' = urlParams.get('mode') as ('table' | 'report') ?? 'table';
const urlParamSchemaOid: string | null = urlParams.get('schema_oid');
let schemaOid: number | null = urlParamSchemaOid ? parseInt(urlParamSchemaOid) : null;
const urlParamColumnOrdering: string | null = urlParams.get('column_ordering');
let columnOrdering: number | null = urlParamColumnOrdering ? parseInt(urlParamColumnOrdering) : null;
const urlParamColumnOid: string | null = urlParams.get('column_oid');
const columnOid: number | null = urlParamColumnOid ? parseInt(urlParamColumnOid) : null;

/**
 * Populate fields in preparation for a new column.
 */
function populateNewColumnMetadata() {    
    // Populate tables that can be referenced by Object/Select/Multiselect column type
    const associatedTableOption: HTMLSelectElement = document.getElementById('column-associated-table') as HTMLSelectElement;
    queryAsync({
        columnAssociatedTables: {
            channel: new Channel<DropdownValue>((table) => {
                // Create OPTION element for the table
                const opt: HTMLOptionElement = document.createElement('option');
                opt.value = table.value.toString();
                opt.label = table.label;

                // Add option to the dropdown
                associatedTableOption.appendChild(opt);
            })
        }
    });
}

/**
 * Populate metadata from a pre-existing column.
 * @param column The column's metadata.
 */
function populatePreexistingColumnMetadata(column: ColumnFullMetadata) {
    schemaOid = column.schema.oid;
    columnOrdering = column.ordering;

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
        const formulaElem: HTMLTextAreaElement = document.getElementById('column-formula') as HTMLTextAreaElement;
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
    const associatedTableOption: HTMLSelectElement = document.getElementById('column-associated-table') as HTMLSelectElement;
    queryAsync({
        columnAssociatedTables: {
            channel: new Channel<DropdownValue>((table) => {
                // Create OPTION element for the table
                const opt: HTMLOptionElement = document.createElement('option');
                opt.value = table.value.toString();
                opt.label = table.label;

                // Auto-select if it is the currently-associated table
                if (table.value == defaultTableOid) {
                    opt.selected = true;
                }

                // Add option to the dropdown
                associatedTableOption.appendChild(opt);
            })
        }
    });

    // Populate column style
    const columnStyleElem: HTMLTextAreaElement = document.getElementById('column-style') as HTMLTextAreaElement;
    columnStyleElem.value = column.style;
}

/**
 * Compiles the column metadata from the entered fields.
 */
function compileColumn(): ColumnFullMetadata {
    // Extract name
    const columnNameElem: HTMLInputElement = document.getElementById('column-name') as HTMLInputElement;
    const columnName: string = columnNameElem.value;

    // Extract column type
    const selectedColumnTypeOption: HTMLOptionElement = document.querySelector('#column-type option:checked') as HTMLOptionElement;
    let columnType: ColumnType;
    if (selectedColumnTypeOption.value.startsWith('primitive-')) {
        columnType = {
            primitive: selectedColumnTypeOption.value.substring('primitive-'.length) as Primitive
        };
    } else if (selectedColumnTypeOption.value == 'object') {
        // Get the associated table
        const selectedAssociatedTableOption: HTMLOptionElement = document.querySelector('#column-associated-table option:checked') as HTMLOptionElement;
        const associatedTableOid: number = parseInt(selectedAssociatedTableOption.value);

        // Construct the column type
        columnType = {
            object: {
                oid: 0,
                tableOid: associatedTableOid
            }
        };
    } else if (selectedColumnTypeOption.value == 'select') {
        // Get the associated table
        const selectedAssociatedTableOption: HTMLOptionElement = document.querySelector('#column-associated-table option:checked') as HTMLOptionElement;
        const associatedTableOid: number = parseInt(selectedAssociatedTableOption.value);

        // Construct the column type
        columnType = {
            select: {
                oid: 0,
                tableOid: associatedTableOid
            }
        };
    } else if (selectedColumnTypeOption.value == 'multiselect') {
        // Get the associated table
        const selectedAssociatedTableOption: HTMLOptionElement = document.querySelector('#column-associated-table option:checked') as HTMLOptionElement;
        const associatedTableOid: number = parseInt(selectedAssociatedTableOption.value);

        // Construct the column type
        columnType = {
            multiselect: {
                oid: 0,
                tableOid: associatedTableOid
            }
        };
    } else if (selectedColumnTypeOption.value == 'formula') {
        // Clone the formula TEXTAREA and convert each parameter SPAN into a text value
        const formulaElem: HTMLTextAreaElement = document.getElementById('column-formula')?.cloneNode() as HTMLTextAreaElement;
        formulaElem.querySelectorAll('span[value]').forEach((span) => {
            span.replaceWith(document.createTextNode(span.getAttribute('value') ?? ''));
        });
        let formula = formulaElem.innerText;
        formulaElem.remove();

        // Construct the column type
        columnType = {
            formula: {
                oid: 0,
                formula: formula
            }
        };
    } else if (selectedColumnTypeOption.value == 'subreport') {
        // TODO
        columnType = {
            subreport: {
                oid: 0,
                reportOid: 0
            }
        };
    } else {
        throw new Error(`Unknown column type: ${selectedColumnTypeOption?.value}`);
    }

    // Extract whether column is hidden
    const hiddenElem: HTMLInputElement = document.getElementById('column-hidden') as HTMLInputElement;
    const hidden: boolean = hiddenElem.checked;

    // Extract whether column is a primary key
    const primaryKeyElem: HTMLInputElement = document.getElementById('column-is-primary-key') as HTMLInputElement;
    const isPrimaryKey: boolean = primaryKeyElem.checked;

    // Extract default value
    const defaultValueElem: HTMLInputElement = document.getElementById('column-default-value') as HTMLInputElement;
    const defaultValue: string = defaultValueElem.value;

    // Extract column style
    const columnStyleElem: HTMLTextAreaElement = document.getElementById('column-style') as HTMLTextAreaElement;
    const columnStyle: string = columnStyleElem.value;

    // Construct the column metadata
    return {
        oid: columnOid ?? 0,
        name: columnName,
        columnType: columnType,
        hidden: hidden,
        ordering: columnOrdering ?? -1,
        isPrimaryKey: isPrimaryKey,
        defaultValue: defaultValue,
        style: columnStyle,
        schema: {
            // Everything but the OID here is ignored
            oid: schemaOid ?? -1,
            name: '',
            masterSchemaOids: [],
            orderByColumnOids: []
        }
    };
}

window.addEventListener("DOMContentLoaded", () => {
    // Populate in preexisting metadata, if any
    if (columnOid) {
        getColumnAsync(columnOid).then((column) => {
            populatePreexistingColumnMetadata(column);
        });
    } else {
        populateNewColumnMetadata();
    }

    // Create listeners for the buttons
    document.getElementById('confirm-button')?.addEventListener("click", async (e) => {
        e.preventDefault();

        const column: ColumnFullMetadata = compileColumn();

        if (columnOid) {
            // Edit the column's metadata
            await executeAsync({
                editColumn: column
            })
            .then(closeDialogAsync)
            .catch(async (e) => {
                await message(e, {
                    title: 'An error occurred while editing column.',
                    kind: 'error'
                });
            });
        } else {
            // Create a new column
            await executeAsync({
                createColumn: column
            })
            .then(closeDialogAsync)
            .catch(async (e) => {
                await message(e, {
                    title: 'An error occurred while creating column.',
                    kind: 'error'
                });
            });
        }
    });
    document.getElementById('cancel-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        await closeDialogAsync();
    });
});