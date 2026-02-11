import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { Channel } from "@tauri-apps/api/core";
import { listen } from '@tauri-apps/api/event';
import { message } from "@tauri-apps/plugin-dialog";
import { BasicHierarchicalMetadata, BasicMetadata, TableCellChannelPacket, TableColumnMetadata, TableRowCellChannelPacket, executeAsync, openDialogAsync, queryAsync } from './backendutils';


/**
 * Update the displayed list of tables.
 */
async function updateTableListAsync() {
  // Remove the tables in the sidebar that were present before
  document.querySelectorAll('.table-sidebar-button').forEach(element => {
    element.remove();
  });
  let addTableButtonWrapper: HTMLElement | null = document.querySelector('#add-new-table-button-wrapper');

}

/**
 * Adds an item to the list of all tables.
 * @param tablesList 
 * @param tableMetadata 
 */
function addTableToList(tablesList: HTMLElement, tableMetadata: BasicMetadata) {
  async function openTableAsync() {
    await openDialogAsync({
      invokeAction: 'dialog_table_data',
      invokeParams: {
        tableOid: tableMetadata.oid,
        tableName: tableMetadata.name
      }
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while opening the table data.',
        kind: 'error'
      });
    });
  }

  async function editTableMetadataAsync() {
    await openDialogAsync({
      invokeAction: 'dialog_edit_table',
      invokeParams: {
        tableOid: tableMetadata.oid
      }
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while opening dialog to edit table metadata.',
        kind: 'error'
      });
    });
  }

  async function deleteTableAsync() {
    await executeAsync({
      deleteTable: {
        tableOid: tableMetadata.oid
      }
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while deleting the table.',
        kind: 'error'
      });
    });
  }

  let tableRadioElem: HTMLInputElement = document.createElement('input');
  tableRadioElem.name = 'tables';
  tableRadioElem.type = 'radio';
  tableRadioElem.id = `table-button-${tableMetadata.oid}`;
  tableRadioElem.classList.add('hidden');

  let tableElem: HTMLLabelElement = document.createElement('label');
  tableElem.htmlFor = tableRadioElem.id;
  tableElem.classList.add('list-item');
  tableElem.tabIndex = 0;
  tableElem.innerText = tableMetadata.name;
  
  tableRadioElem.addEventListener('input', (_) => {
    let openTableButton: HTMLButtonElement | null = document.getElementById('open-table-button') as HTMLButtonElement;
    if (openTableButton) {
      // Replace the old button with a clone that has no event listeners
      let openTableButtonClone: HTMLButtonElement = openTableButton.cloneNode(true) as HTMLButtonElement;
      openTableButton.replaceWith(openTableButtonClone);

      // Add event listener to the clone
      openTableButtonClone.disabled = false;
      openTableButtonClone.addEventListener('click', openTableAsync);
    }

    let editTableButton: HTMLButtonElement | null = document.getElementById('edit-table-button') as HTMLButtonElement;
    if (editTableButton) {
      // Replace the old button with a clone that has no event listeners
      let editTableButtonClone: HTMLButtonElement = editTableButton.cloneNode(true) as HTMLButtonElement;
      editTableButton.replaceWith(editTableButtonClone);

      // Add event listener to the clone
      editTableButtonClone.disabled = false;
      editTableButtonClone.addEventListener('click', editTableMetadataAsync);
    }

    let deleteTableButton: HTMLButtonElement | null = document.getElementById('delete-table-button') as HTMLButtonElement;
    if (deleteTableButton) {
      // Replace the old button with a clone that has no event listeners
      let deleteTableButtonClone: HTMLButtonElement = deleteTableButton.cloneNode(true) as HTMLButtonElement;
      deleteTableButton.replaceWith(deleteTableButtonClone);

      // Add event listener to the clone
      deleteTableButtonClone.disabled = false;
      deleteTableButtonClone.addEventListener('click', deleteTableAsync);
    }
  });

  // Add an event listener to the item in the list, causing the table to be opened when double-clicked
  tableElem.addEventListener('dblclick', openTableAsync);

  // Add to the DOM tree
  tablesList.appendChild(tableRadioElem);
  tablesList.appendChild(tableElem);
}

/**
 * Loads the list of all tables from the database.
 */
function loadTables() {
  navigator.locks.request('tables-container', async () => {
    let tablesList: HTMLElement | null = document.querySelector('#tables-container .list');
    if (tablesList) {
      // Clear out the list
      tablesList.innerHTML = '';

      // Set up a channel that adds a received table to the list
      const onReceiveUpdatedTable = new Channel<BasicMetadata>();
      onReceiveUpdatedTable.onmessage = (tableMetadata) => {
        addTableToList(tablesList, tableMetadata);
      };

      // Send a command to Rust to get the list of tables from the database
      await queryAsync({
        invokeAction: "get_table_list", 
        invokeParams: { tableChannel: onReceiveUpdatedTable }
      });

      if (tablesList.childElementCount > 0) {
        // If at least one table exists, add a hidden radio button to indicate that no table is selected
        let unselectedRadioElem: HTMLInputElement = document.createElement('input');
        unselectedRadioElem.name = 'tables';
        unselectedRadioElem.type = 'radio';
        unselectedRadioElem.checked = true;
        unselectedRadioElem.classList.add('hidden');
        tablesList.appendChild(unselectedRadioElem);
      } else {
        // If no tables exist, display a message saying that the user needs to click "New" to create one
        let emptyElem: HTMLDivElement = document.createElement('div');
        emptyElem.classList.add('empty-list-item');
        emptyElem.innerText = 'Click "New" to Define a New Table';
        tablesList.appendChild(emptyElem);
      }
    }
  });
}

/**
 * Adds an item to the list of all reports.
 * @param reportsList 
 * @param reportMetadata 
 */
function addReportToList(reportsList: HTMLElement, reportMetadata: BasicMetadata) {

}

/**
 * Loads the list of all reports from the database.
 */
function loadReports() {
  navigator.locks.request('reports-container', async () => {
    let reportsList: HTMLElement | null = document.querySelector('#reports-container .list');
    if (reportsList) {
      // Clear out the list
      reportsList.innerHTML = '';

      // Set up a channel that adds a received report to the list
      const onReceiveReport = new Channel<BasicMetadata>();
      onReceiveReport.onmessage = (reportMetadata) => {
        addReportToList(reportsList, reportMetadata);
      };

      // Send a command to Rust to get the list of reports from the database
      await queryAsync({
        invokeAction: "get_report_list", 
        invokeParams: { reportChannel: onReceiveReport }
      });

      if (reportsList.childElementCount > 0) {
        // If at least one report exists, add a hidden radio button to indicate that no report is selected
        let unselectedRadioElem: HTMLInputElement = document.createElement('input');
        unselectedRadioElem.name = 'reports';
        unselectedRadioElem.type = 'radio';
        unselectedRadioElem.checked = true;
        unselectedRadioElem.classList.add('hidden');
        reportsList.appendChild(unselectedRadioElem);
      } else {
        // If no reports exist, display a message saying that the user needs to click "New" to create one
        let emptyElem: HTMLDivElement = document.createElement('div');
        emptyElem.classList.add('empty-list-item');
        emptyElem.innerText = 'Click "New" to Define a New Report';
        reportsList.appendChild(emptyElem);
      }
    }
  });
}

/**
 * Adds an item to the list of all object types.
 * @param objectTypeList 
 * @param objectTypeMetadata 
 */
function addObjectTypeToList(objTypeList: HTMLElement, objTypeMetadata: BasicHierarchicalMetadata) {
  async function openObjTypeAsync() {
    await openDialogAsync({
      invokeAction: 'dialog_table_data',
      invokeParams: {
        tableOid: objTypeMetadata.oid,
        tableName: objTypeMetadata.name
      }
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while opening the object type data.',
        kind: 'error'
      });
    });
  }

  async function editObjTypeMetadataAsync() {
    await openDialogAsync({
      invokeAction: 'dialog_edit_object_type',
      invokeParams: {
        objTypeOid: objTypeMetadata.oid
      }
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while opening dialog to edit object type metadata.',
        kind: 'error'
      });
    });
  }

  async function deleteObjTypeAsync() {
    await executeAsync({
      deleteObjectType: {
        objTypeOid: objTypeMetadata.oid
      }
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while deleting the object type.',
        kind: 'error'
      });
    });
  }

  let objTypeRadioElem: HTMLInputElement = document.createElement('input');
  objTypeRadioElem.name = 'object-types';
  objTypeRadioElem.type = 'radio';
  objTypeRadioElem.id = `object-type-button-${objTypeMetadata.oid}`;
  objTypeRadioElem.classList.add('hidden');

  let objTypeElem: HTMLLabelElement = document.createElement('label');
  objTypeElem.htmlFor = objTypeRadioElem.id;
  objTypeElem.classList.add('list-item');
  objTypeElem.tabIndex = 0;
  objTypeElem.style.whiteSpace = 'pre';
  objTypeElem.innerText = ' '.repeat(objTypeMetadata.hierarchyLevel) + objTypeMetadata.name;
  
  objTypeRadioElem.addEventListener('input', (_) => {
    let openTableButton: HTMLButtonElement | null = document.getElementById('open-object-type-button') as HTMLButtonElement;
    if (openTableButton) {
      // Replace the old button with a clone that has no event listeners
      let openTableButtonClone: HTMLButtonElement = openTableButton.cloneNode(true) as HTMLButtonElement;
      openTableButton.replaceWith(openTableButtonClone);

      // Add event listener to the clone
      openTableButtonClone.disabled = false;
      openTableButtonClone.addEventListener('click', openObjTypeAsync);
    }

    let editTableButton: HTMLButtonElement | null = document.getElementById('edit-object-type-button') as HTMLButtonElement;
    if (editTableButton) {
      // Replace the old button with a clone that has no event listeners
      let editTableButtonClone: HTMLButtonElement = editTableButton.cloneNode(true) as HTMLButtonElement;
      editTableButton.replaceWith(editTableButtonClone);

      // Add event listener to the clone
      editTableButtonClone.disabled = false;
      editTableButtonClone.addEventListener('click', editObjTypeMetadataAsync);
    }

    let deleteTableButton: HTMLButtonElement | null = document.getElementById('delete-object-type-button') as HTMLButtonElement;
    if (deleteTableButton) {
      // Replace the old button with a clone that has no event listeners
      let deleteTableButtonClone: HTMLButtonElement = deleteTableButton.cloneNode(true) as HTMLButtonElement;
      deleteTableButton.replaceWith(deleteTableButtonClone);

      // Add event listener to the clone
      deleteTableButtonClone.disabled = false;
      deleteTableButtonClone.addEventListener('click', deleteObjTypeAsync);
    }
  });

  // Add an event listener to the item in the list, causing the table to be opened when double-clicked
  objTypeElem.addEventListener('dblclick', openObjTypeAsync);

  // Add to the DOM tree
  objTypeList.appendChild(objTypeRadioElem);
  objTypeList.appendChild(objTypeElem);
}

/**
 * Loads the list of all object types from the database.
 */
function loadObjectTypes() {
  console.debug('Received request to refresh object types.')
  navigator.locks.request('object-types-container', async () => {
    let objectTypesList: HTMLElement | null = document.querySelector('#object-types-container .list');
    if (objectTypesList) {
      // Clear out the list
      objectTypesList.innerHTML = '';

      // Set up a channel that adds a received type to the list
      const onReceiveObjectType = new Channel<BasicHierarchicalMetadata>();
      onReceiveObjectType.onmessage = (objectTypeMetadata) => {
        addObjectTypeToList(objectTypesList, objectTypeMetadata);
      };

      // Send a command to Rust to get the list of types from the database
      await queryAsync({
        invokeAction: "get_object_type_list", 
        invokeParams: { objectTypeChannel: onReceiveObjectType }
      });

      if (objectTypesList.childElementCount > 0) {
        // If at least one object type exists, add a hidden radio button to indicate that no report is selected
        let unselectedRadioElem: HTMLInputElement = document.createElement('input');
        unselectedRadioElem.name = 'object-types';
        unselectedRadioElem.type = 'radio';
        unselectedRadioElem.checked = true;
        unselectedRadioElem.classList.add('hidden');
        objectTypesList.appendChild(unselectedRadioElem);
      } else {
        // If no object types exist, display a message saying that the user needs to click "New" to create one
        let emptyElem: HTMLDivElement = document.createElement('div');
        emptyElem.classList.add('empty-list-item');
        emptyElem.innerText = 'Click "New" to Define a New Object Type';
        objectTypesList.appendChild(emptyElem);
      }
    }
  });
}

// Add initial listeners
window.addEventListener("DOMContentLoaded", () => {
  console.debug('Page loaded.');
  document.getElementById('new-table-button')?.addEventListener('click', async (_) => {
    await openDialogAsync({
      invokeAction: "dialog_create_table", 
      invokeParams: {}
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while opening dialog to create a new table.',
        kind: 'error'
      });
    });
  });
  document.getElementById('new-report-button')?.addEventListener('click', async (_) => {
    /*
    await openDialogAsync({
      invokeAction: "dialog_create_report", 
      invokeParams: {}
    });
    */
  });
  document.getElementById('new-object-type-button')?.addEventListener('click', async (_) => {
    await openDialogAsync({
      invokeAction: "dialog_create_object_type", 
      invokeParams: {}
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while opening dialog to create a new object type.',
        kind: 'error'
      });
    });
  });

  loadTables();
  loadReports();
  loadObjectTypes();
});

listen<any>("update-table-list", loadTables);
listen<any>("update-report-list", loadReports);
listen<any>("update-object-type-list", loadObjectTypes);
