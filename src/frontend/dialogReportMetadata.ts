import { Channel } from "@tauri-apps/api/core";
import { BasicMetadata, closeDialogAsync, executeAsync, queryAsync } from "./backendutils";
import { message } from "@tauri-apps/plugin-dialog";

const urlParams = new URLSearchParams(window.location.search);
const urlParamReportOid = urlParams.get('report_oid');
const reportOid: number | null = urlParamReportOid ? parseInt(urlParamReportOid) : null;


async function populateMetadataAsync() {
    if (!reportOid) 
        return;

    // Query the database for previous metadata
    const reportMetadata: {
        oid: number,
        name: string,
        baseTableOid: number
    } = await queryAsync({
        invokeAction: 'get_report_metadata',
        invokeParams: {
            reportOid: reportOid
        }
    })
    .catch(async (e) => {
        await message(e, {
            title: `An error occurred while retrieving current metadata for report.`,
            kind: 'error'
        });
    });

    // Populate the table name
    let reportNameInput: HTMLInputElement = document.getElementById('report-name') as HTMLInputElement;
    reportNameInput.value = reportMetadata.name;

    // Populate the base table, and prevent it from being changed
    let reportBaseTableInput: HTMLSelectElement = document.getElementById('base-table-select') as HTMLSelectElement;
    reportBaseTableInput.value = reportMetadata.baseTableOid.toString();
    reportBaseTableInput.disabled = true;
}

/**
 * Populate the possible options for a master list, ensuring that no table that would cause an infinite loop of inheritance can be selected.
 */
async function refreshBaseListAsync() {
    let baseListSelect: HTMLSelectElement = document.getElementById('base-table-select') as HTMLSelectElement;
    baseListSelect.innerHTML = '';

    // Set up the channel 
    const onReceiveOption = new Channel<BasicMetadata>();
    onReceiveOption.onmessage = (opt) => {
        let optionNode: HTMLOptionElement = document.createElement('option');
        optionNode.value = opt.oid.toString();
        optionNode.innerText = opt.name;
        baseListSelect.appendChild(optionNode);
    }

    // Run the query
    await queryAsync({
        invokeAction: 'get_table_list',
        invokeParams: {
            tableChannel: onReceiveOption
        }
    });
}

/**
 * Creates a report.
 */
async function createReportAsync() {
    // Get the report name
    let reportNameInput: HTMLInputElement = document.getElementById('report-name') as HTMLInputElement;
    let reportName = reportNameInput.value?.trim();
    if (!reportName) {
        await message("Name is a required field!", {
            title: "Unable to create report.",
            kind: 'warning'
        });
        return;
    }

    // Get the base table for the report
    let reportBaseTableInput: HTMLSelectElement = document.getElementById('base-table-select') as HTMLSelectElement;
    const baseTableOid: number = parseInt(reportBaseTableInput.value);
    if (isNaN(baseTableOid)) {
        await message("Base Table is a required field!", {
            title: "Unable to create report.",
            kind: 'warning'
        });
        return;
    }

    // Send request to backend to create report
    await executeAsync({
        createReport: {
            reportName: reportName,
            baseTableOid: baseTableOid
        }
    })
    .then(closeDialogAsync)
    .catch(async (e) => {
        await message(e, {
            title: 'An error occurred while creating report.',
            kind: 'error'
        });
    });
}

/**
 * Edits the metadata for a table or object type.
 */
async function editReportAsync() {
    if (!reportOid)
        return;

    // Get the report name
    let reportNameInput: HTMLInputElement = document.getElementById('report-name') as HTMLInputElement;
    let reportName = reportNameInput.value?.trim();
    if (!reportName) {
        await message("Name is a required field!", {
            title: "Unable to create report.",
            kind: 'warning'
        });
        return;
    }

    // Update the metadata for the report
    await executeAsync({
        editReportMetadata: {
            reportOid: reportOid,
            reportName: reportName
        }
    })
    .then(closeDialogAsync)
    .catch(async (e) => {
        await message(e, {
            title: 'An error occurred while editing report metadata.',
            kind: 'error'
        });
    });
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
window.addEventListener("DOMContentLoaded", async () => {
    // Refresh the list of tables that the table can inherit from
    await refreshBaseListAsync();

    // Populate in the metadata from the table, if it already existed
    await populateMetadataAsync();

    // Set up event listeners for OK and Cancel
    document.getElementById('confirm-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;

        if (reportOid) {
            // Edit the table's metadata
            await editReportAsync();
        } else {
            // Create a new table
            await createReportAsync();
        }
    });
    document.getElementById('cancel-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;
        await cancel();
    });
});