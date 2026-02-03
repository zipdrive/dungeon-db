import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { Channel } from "@tauri-apps/api/core";
import { listen } from '@tauri-apps/api/event';
import { message } from "@tauri-apps/plugin-dialog";
import { TableCellChannelPacket, TableColumnMetadata, TableRowCellChannelPacket, executeAsync, openDialogAsync, queryAsync } from './backendutils';
import { addTableColumnCellToRow } from "./tableutils";


/**
 * Update the displayed list of tables.
 */
async function updateTableListAsync() {
  // Remove the tables in the sidebar that were present before
  document.querySelectorAll('.table-sidebar-button').forEach(element => {
    element.remove();
  });
  let addTableButtonWrapper: HTMLElement | null = document.querySelector('#add-new-table-button-wrapper');

  // Set up a channel
  const onReceiveUpdatedTable = new Channel<{ oid: number, name: string }>();
  onReceiveUpdatedTable.onmessage = (table) => {
    // Load in each table and create a button for that table
    let openTableButton: HTMLButtonElement = document.createElement('button');
    openTableButton.classList.add('list-object');
    openTableButton.innerText = table.name;
    
    // When clicked, open window containing table data
    openTableButton?.addEventListener("click", _ => {
      // Display the table
      console.debug(`table.html?table_oid=${encodeURIComponent(table.oid)}`);
      window.location.href = `table.html?table_oid=${encodeURIComponent(table.oid)}`;
    });

    // Add the button to the DOM tree
    addTableButtonWrapper?.insertAdjacentElement('beforebegin', openTableButton);
  };

  // Send a command to Rust to get the list of tables from the database
  await queryAsync({
    invokeAction: "get_table_list", 
    invokeParams: { tableChannel: onReceiveUpdatedTable }
  });
}

/**
 * Opens the dialog to create a new table.
 */
async function createTable() {
  await openDialogAsync({
    invokeAction: "dialog_create_table", 
    invokeParams: {}
  });
}

/**
 * Adds a table to the list of tables.
 */
function addTableToList(tablesList: HTMLElement, tableOid: number) {
  let tableRadioElem: HTMLInputElement = document.createElement('input');
  tableRadioElem.name = 'tables';
  tableRadioElem.type = 'radio';
  tableRadioElem.id = `table-button-${tableOid}`;
  tableRadioElem.classList.add('hidden');

  let tableElem: HTMLLabelElement = document.createElement('label');
  tableElem.htmlFor = tableRadioElem.id;
  tableElem.classList.add('list-item');
  tableElem.tabIndex = 0;
  tableElem.innerText = `Table${tableOid}`;
  
  tableRadioElem.addEventListener('input', (_) => {
    let openTableButton: HTMLButtonElement | null = document.getElementById('open-table-button') as HTMLButtonElement;
    if (openTableButton) {
      // Replace the old button with a clone that has no event listeners
      let openTableButtonClone: HTMLButtonElement = openTableButton.cloneNode(true) as HTMLButtonElement;
      openTableButton.replaceWith(openTableButtonClone);

      // Add event listener to the clone
      openTableButtonClone.disabled = false;
      openTableButtonClone.addEventListener('click', (_) => {
        // Open the table in a new window
      });
    }

    let editTableButton: HTMLButtonElement | null = document.getElementById('edit-table-button') as HTMLButtonElement;
    if (editTableButton) {
      // Replace the old button with a clone that has no event listeners
      let editTableButtonClone: HTMLButtonElement = editTableButton.cloneNode(true) as HTMLButtonElement;
      editTableButton.replaceWith(editTableButtonClone);

      // Add event listener to the clone
      editTableButtonClone.disabled = false;
      editTableButtonClone.addEventListener('click', (_) => {
        // Open a dialog to edit the table's metadata
      });
    }

    let deleteTableButton: HTMLButtonElement | null = document.getElementById('delete-table-button') as HTMLButtonElement;
    if (deleteTableButton) {
      // Replace the old button with a clone that has no event listeners
      let deleteTableButtonClone: HTMLButtonElement = deleteTableButton.cloneNode(true) as HTMLButtonElement;
      deleteTableButton.replaceWith(deleteTableButtonClone);

      // Add event listener to the clone
      deleteTableButtonClone.disabled = false;
      deleteTableButtonClone.addEventListener('click', (_) => {
        // Delete the table
      });
    }
  });

  tablesList.appendChild(tableRadioElem);
  tablesList.appendChild(tableElem);
}

/**
 * Loads the list of all tables from the database.
 */
async function loadTables() {
  let tablesList: HTMLElement | null = document.querySelector('#tables-container .list');
  if (tablesList) {
    let tableRadioElem: HTMLInputElement = document.createElement('input');
    tableRadioElem.name = 'tables';
    tableRadioElem.type = 'radio';
    tableRadioElem.checked = true;
    tableRadioElem.classList.add('hidden');
    tablesList.appendChild(tableRadioElem);

    addTableToList(tablesList, 10);
    addTableToList(tablesList, 11);
    addTableToList(tablesList, 12);
  }
}

/**
 * Loads the list of all reports from the database.
 */
async function loadReports() {
  let reportsList: HTMLElement | null = document.querySelector('#reports-container .list');
  if (reportsList) {
    let emptyRadioElem: HTMLInputElement = document.createElement('input');
    emptyRadioElem.name = 'reports';
    emptyRadioElem.type = 'radio';
    emptyRadioElem.checked = true;
    emptyRadioElem.classList.add('hidden');
    reportsList.appendChild(emptyRadioElem);

    let emptyElem: HTMLDivElement = document.createElement('div');
    emptyElem.classList.add('empty-list-item');
    emptyElem.innerText = 'Click "New" to Define a New Report';
    reportsList.appendChild(emptyElem);
  }
}

/**
 * Loads the list of all global data types from the database.
 */
async function loadGlobalDataTypes() {
  let globalDataTypesList: HTMLElement | null = document.querySelector('#global-types-container .list');
  if (globalDataTypesList) {
    let emptyRadioElem: HTMLInputElement = document.createElement('input');
    emptyRadioElem.name = 'globalDataTypes';
    emptyRadioElem.type = 'radio';
    emptyRadioElem.checked = true;
    emptyRadioElem.classList.add('hidden');
    globalDataTypesList.appendChild(emptyRadioElem);

    let emptyElem: HTMLDivElement = document.createElement('div');
    emptyElem.classList.add('empty-list-item');
    emptyElem.innerText = 'Click "New" to Define a New Global Data Type';
    globalDataTypesList.appendChild(emptyElem);
  }
}

// Add initial listeners
window.addEventListener("DOMContentLoaded", async () => {
  console.debug('Page loaded.');
  document.getElementById('new-table-button')?.addEventListener('click', (_) => {
    console.debug('New table dialog called.');
  });
  document.getElementById('new-report-button')?.addEventListener('click', (_) => {
    console.debug('New report dialog called.');
  });
  document.getElementById('new-global-type-button')?.addEventListener('click', (_) => {
    console.debug('New global data type dialog called.');
  });

  await loadTables();
  await loadReports();
  await loadGlobalDataTypes();
});

listen<any>("update-table-list", (_) => {
  navigator.locks.request('table-sidebar', async () => await updateTableListAsync());
});


window.addEventListener("DOMContentLoaded", () => {
  // TODO
});

