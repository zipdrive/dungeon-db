import { invoke } from "@tauri-apps/api/core";
import { message } from "@tauri-apps/plugin-dialog";

function showParameters() {
    // Turn off all parameters
    const selectedType = (document.getElementById('column-type') as HTMLInputElement)?.value;
    document.querySelectorAll('.dialog-form-table tr').forEach((varParamNode) => { (varParamNode as HTMLTableRowElement).style.display = 'none'; });

    // Turn on only the parameters for the specified type
    document.querySelectorAll(`.parameter-${selectedType}`).forEach((varParamNode) => { (varParamNode as HTMLTableRowElement).style.display = 'table-row'; });
}

// Add initial listeners
window.addEventListener("DOMContentLoaded", () => {
    // Turn on or off various parameters to match the necessary parameters for the chosen column type
    showParameters();
    document.getElementById('column-type')?.addEventListener('change', showParameters);

    // Create the column when Create Column is clicked
    document.querySelector('#create-table-column-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;

        const urlParams = new URLSearchParams(window.location.search);
        const tableOid = urlParams.get('table_oid');
        const columnOrdering = urlParams.get('column_ordering');
        if (!tableOid || !columnOrdering) {
            await message("Dialog window does not have expected GET parameters.", { title: "An error occurred while creating column.", kind: 'error' });
            return;
        }

        const columnName = (document.getElementById('column-name') as HTMLInputElement)?.value;
        if (!columnName) {
            await message("Unable to create a column with no name.", { title: "An error occurred while creating column.", kind: 'error' });
            return;
        }

        const columnTypeStr = (document.getElementById('column-type') as HTMLInputElement)?.value;
        const isNullable: boolean = (document.getElementById('column-is-nullable') as HTMLInputElement)?.checked;
        let isUnique: boolean = (document.getElementById('column-is-unique') as HTMLInputElement)?.checked;
        let isPrimaryKey: boolean = (document.getElementById('column-is-primary-key') as HTMLInputElement)?.checked;
        const columnWidth = parseFloat((document.getElementById('column-width') as HTMLInputElement)?.value);
        let columnType: { primitive: string } 
        | { singleSelectDropdown: number }
        | { multiSelectDropdown: number }
        | { reference: number } 
        | { childObject: number } 
        | { childTable: number };
        switch (columnTypeStr) {
            case 'bool':
                columnType = { primitive: "Boolean" };
                isUnique = false;
                break;
            case 'int':
                columnType = { primitive: "Integer" };
                break;
            case 'number':
                columnType = { primitive: "Number" };
                break;
            case 'date':
                columnType = { primitive: "Date" };
                break;
            case 'timestamp':
                columnType = { primitive: "Timestamp" };
                break;
            case 'text':
                columnType = { primitive: "Text" };
                break;
            case 'json':
                columnType = { primitive: "JSON" };
                break;
            case 'file':
                columnType = { primitive: "File" };
                isUnique = false;
                isPrimaryKey = false;
                break;
            case 'image':
                columnType = { primitive: "Image" };
                isUnique = false;
                isPrimaryKey = false;
                break;
            case 'reference':
                const referencedTableOid = (document.getElementById('column-type-oid-reference') as HTMLInputElement)?.value;
                if (!referencedTableOid) {
                    await message("You must select a referenced table for a column of type Reference.", { title: "An error occurred while creating column.", kind: 'error' });
                    return;
                }
                columnType = { reference: parseInt(referencedTableOid) };
                break;
            case 'object':
                const objTableOid = (document.getElementById('column-type-oid-reference') as HTMLInputElement)?.value;
                if (!objTableOid) {
                    await message("You must select a global data type for a column of type Global Data Type.", { title: "An error occurred while creating column.", kind: 'error' });
                    return;
                }
                columnType = { childObject: parseInt(objTableOid) };
                isUnique = false;
                isPrimaryKey = false;
                break;
            case 'childTable':
                columnType = { childTable: 0 };
                isUnique = false;
                isPrimaryKey = false;
                break;
            default:
                await message("Unknown column type.", { title: "An error occurred while creating column.", kind: 'error' });
                return;
        }

        // Create the column
        await invoke("create_table_column", {
            tableOid: parseInt(tableOid),
            columnName: columnName,
            columnType: columnType,
            columnWidth: columnWidth,
            columnOrdering: parseInt(columnOrdering),
            isNullable: isNullable,
            isUnique: isUnique,
            isPrimaryKey: isPrimaryKey
        })
        .then(async (_) => await invoke("dialog_close", {}))
        .catch(async (e) => {
            await message(e, {
                title: "An error occurred while creating table.",
                kind: 'error'
            });
        });
    });

    // Close the dialog when Cancel is clicked
    document.querySelector('#cancel-create-table-column-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;

        await invoke("dialog_close", {})
        .catch(async (e) => {
            await message(e, {
                title: "An error occurred while closing dialog box.",
                kind: 'error'
            });
        });
    });
});