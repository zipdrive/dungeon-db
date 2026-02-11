import { Channel } from "@tauri-apps/api/core";
import { closeDialogAsync, executeAsync, queryAsync, ToggledHierarchicalMetadata } from "./backendutils";
import { message } from "@tauri-apps/plugin-dialog";

const urlParams = new URLSearchParams(window.location.search);
const urlParamTableOid = urlParams.get('table_oid');
const tableOid: number | null = urlParamTableOid ? parseInt(urlParamTableOid) : null;
const urlParamMode = urlParams.get('mode');
const mode: number | null = urlParamMode ? parseInt(urlParamMode) : null;


/**
 * Populate the possible options for a master list, ensuring that no table that would cause an infinite loop of inheritance can be selected.
 */
async function refreshMasterList() {
    console.debug("Refreshing master list.");
    let masterListSelect: HTMLSelectElement = document.getElementById('master-table-select') as HTMLSelectElement;
    masterListSelect.innerHTML = '';

    // Set up the channel 
    const onReceiveOption = new Channel<ToggledHierarchicalMetadata>();
    onReceiveOption.onmessage = (opt) => {
        let optionNode: HTMLOptionElement = document.createElement('option');
        optionNode.value = opt.oid.toString();
        optionNode.innerText = (opt.hierarchyLevel > 0 ? '--'.repeat(opt.hierarchyLevel) + ' ' : '') + opt.name;
        optionNode.disabled = opt.isDisabled;
        masterListSelect.appendChild(optionNode);
    }

    // Run the query
    await queryAsync({
        invokeAction: 'get_master_list_option_dropdown_values',
        invokeParams: {
            tableOid: tableOid,
            allowInheritanceFromTables: true,
            optionChannel: onReceiveOption
        }
    });
}

/**
 * Creates a table or object type.
 */
async function createTableAsync() {
    // Get the table name
    let tableNameInput: HTMLInputElement = document.getElementById('table-name') as HTMLInputElement;
    let tableName = tableNameInput.value?.trim();
    if (!tableName) {
        await message("Name is a required field!", {
            title: "Unable to create table.",
            kind: 'warning'
        });
        return;
    }

    // Get the list of selected master tables
    let masterTableOid: number[] = [];
    document.querySelectorAll('#master-table-select option:checked').forEach((opt) => {
        masterTableOid.push(parseInt((opt as HTMLOptionElement).value));
    });

    // Determine what mode of table should be created
    if (mode == 4) {
        // Create an object type
        await executeAsync({
            createObjectType: {
                objTypeName: tableName,
                masterTableOidList: masterTableOid
            }
        })
        .then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while creating object type.',
                kind: 'error'
            });
        });
    } else {
        // Create a table
        await executeAsync({
            createTable: {
                tableName: tableName,
                masterTableOidList: masterTableOid
            }
        })
        .then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while creating table.',
                kind: 'error'
            });
        });
    }
}

/**
 * Edits the metadata for a table or object type.
 */
async function editTableAsync() {
    if (!tableOid)
        return;

    // Get the table name
    let tableNameInput: HTMLInputElement = document.getElementById('table-name') as HTMLInputElement;
    let tableName = tableNameInput.value?.trim();
    if (!tableName) {
        await message("Name is a required field!", {
            title: "Unable to create table.",
            kind: 'warning'
        });
        return;
    }

    // Get the list of selected master tables
    let masterTableOid: number[] = [];
    document.querySelectorAll('#master-table-select option:checked').forEach((opt) => {
        masterTableOid.push(parseInt((opt as HTMLOptionElement).value));
    });

    // Determine what mode of table should be created
    if (mode == 4) {
        // Edit the metadata of an object type
        await executeAsync({
            editObjectTypeMetadata: {
                objTypeOid: tableOid,
                objTypeName: tableName,
                masterTableOidList: masterTableOid
            }
        })
        .then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while editing object type metadata.',
                kind: 'error'
            });
        });
    } else {
        // Edit the metadata of a table
        await executeAsync({
            editTableMetadata: {
                tableOid: tableOid,
                tableName: tableName,
                masterTableOidList: masterTableOid
            }
        })
        .then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while editing table metadata.',
                kind: 'error'
            });
        });
    }
}

/**
 * Closes the dialog.
 */
function cancel() {
    closeDialogAsync()
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while closing dialog box.",
            kind: 'error'
        });
    });
}

// Add initial listeners
window.addEventListener("DOMContentLoaded", () => {
    refreshMasterList();

    // Set up event listeners for OK and Cancel
    document.getElementById('confirm-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;

        if (tableOid) {
            // Edit the table's metadata
            await editTableAsync();
        } else {
            // Create a new table
            await createTableAsync();
        }
    });
    document.getElementById('cancel-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;
        await cancel();
    });
});