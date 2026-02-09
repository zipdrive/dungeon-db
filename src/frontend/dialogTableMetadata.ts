import { Channel } from "@tauri-apps/api/core";
import { closeDialogAsync, executeAsync, queryAsync, ToggledHierarchicalMetadata } from "./backendutils";
import { message } from "@tauri-apps/plugin-dialog";

const urlParams = new URLSearchParams(window.location.search);
const urlParamTableOid = urlParams.get('table_oid');
const tableOid: number | null = urlParamTableOid ? parseInt(urlParamTableOid) : null;


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

async function createTable() {
    let tableNameInput: HTMLInputElement = document.getElementById('table-name') as HTMLInputElement;
    let tableName = tableNameInput.value;

    let masterTableOid: number[] = [];
    document.querySelectorAll('#master-table-select option:checked').forEach((opt) => {
        masterTableOid.push(parseInt((opt as HTMLOptionElement).value));
    });
    
    if (!tableName || !tableName.trim()) {
        message("Unable to create a table with no name.", {
            title: "An error occurred while creating table.",
            kind: 'error'
        });
    } else {
        await executeAsync({
            createTable: {
                tableName: tableName,
                masterTableOidList: masterTableOid
            }
        })
        //.then(closeDialogAsync)
        .catch(async (e) => {
            await message(e, {
                title: "An error occurred while creating table.",
                kind: 'error'
            });
        });
    }
}

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
    document.querySelector('#create-table-button')?.addEventListener("click", async (e) => {
        console.debug('createTable() called.');
        e.preventDefault();
        e.returnValue = false;
        await createTable();
    });
    document.querySelector('#cancel-create-table-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;
        await cancel();
    });
});