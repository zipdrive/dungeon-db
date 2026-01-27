import { Menu } from "@tauri-apps/api/menu";
import { Channel, invoke } from "@tauri-apps/api/core";
import { listen } from '@tauri-apps/api/event';
import { message } from "@tauri-apps/plugin-dialog";

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
    addTableButtonWrapper?.insertAdjacentHTML('beforebegin', 
      `<button class="table-sidebar-button" id="table-sidebar-button-${table.oid}"></button>`
    );

    // Add functionality when clicked
    let tableSidebarButton: HTMLInputElement | null = document.querySelector(`#table-sidebar-button-${table.oid}`);
    if (tableSidebarButton != null) {
      tableSidebarButton.innerText = table.name;
      tableSidebarButton?.addEventListener("click", _ => {
        // Set every other table as inactive
        document.querySelectorAll('.table-sidebar-button').forEach(element => {
          element.classList.remove("active");
        });
        // Set this table as active
        tableSidebarButton?.classList.add("active");
        // Display the table
        displayTable(table.oid);
      });
    }
  };

  // Send a command to Rust to get the list of tables from the database
  await invoke("get_table_list", { tableChannel: onReceiveUpdatedTable });
}

/**
 * Opens the dialog to create a new table.
 */
export async function createTable() {
  await invoke("dialog_create_table", {})
    .catch(async e => {
      await message(e, {
        title: 'Error while opening dialog box to create table.',
        kind: 'error'
      });
    });
}

/**
 * Displays the data for a table.
 * @param tableOid The OID of the table.
 */
export async function displayTable(tableOid: number) {
  type TableColumn = {
    oid: number, 
    name: string,
    width: number,
    type_oid: number,
    type_mode: number,
    is_nullable: boolean,
    is_unique: boolean,
    is_primary_key: boolean,
  };

  type TableCell = {
    rowOid: number
  } | {
    columnOid: number,
    displayValue: string
  };

  // Strip the former contents of the table
  let tableNode: HTMLTableElement | null = document.querySelector('#table-content');
  if (tableNode)
    tableNode.innerHTML = '<thead><tr><th>OID</th></tr></thead><tbody></tbody><tfoot><tr><td id="add-new-row-button">Add New Row</td></tr></tfoot>';
  let tableHeaderRowNode: HTMLTableRowElement | null = document.querySelector('#table-content > thead > tr');
  let tableBodyNode: HTMLElement | null = document.querySelector('#table-content > tbody');

  // Set up a channel to populate the list of user-defined columns
  let tableColumnList: TableColumn[] = []
  const onReceiveColumn = new Channel<TableColumn>();
  onReceiveColumn.onmessage = (column) => {
    // Add the column to the list of columns
    tableColumnList.push(column);

    // Add a header for the column
    let tableHeaderNode: HTMLElement | null = document.createElement('th');
    if (tableHeaderNode != null) {
      tableHeaderNode.style.columnWidth = `${column.width}px`;
      tableHeaderNode.innerText = column.name;
      tableHeaderRowNode?.insertAdjacentElement('beforeend', tableHeaderNode);

      // TODO context menu for header
    }
  };

  // Send a command to Rust to get the list of table columns from the database
  await invoke("get_table_column_list", { tableOid: tableOid, columnChannel: onReceiveColumn })
    .catch(async e => {
      await message(e, {
        title: 'Error while retrieving list of columns for table.',
        kind: 'error'
      });
    });

  // Add a final column header that is a button to add a new column
  let tableAddColumnHeaderNode = document.createElement('th');
  if (tableAddColumnHeaderNode != null) {
    tableAddColumnHeaderNode.id = 'add-new-column-button';
    tableAddColumnHeaderNode.innerText = 'Add New Column';
    tableAddColumnHeaderNode.addEventListener('click', async (_) => {
      await invoke("dialog_create_table_column", {
        tableOid: tableOid,
        columnOrdering: tableColumnList.length
      }).catch(async e => {
          await message(e, {
            title: 'Error while opening dialog box to create table.',
            kind: 'error'
          });
        });
    });
    tableHeaderRowNode?.insertAdjacentElement('beforeend', tableAddColumnHeaderNode);
  }

  // Set the span of the footer
  let tableFooterCellNode: HTMLElement | null = document.querySelector('#add-new-row-button');
  if (tableFooterCellNode) {
    // Set the footer to span the entire row
    tableFooterCellNode.setAttribute('colspan', (tableColumnList.length + 2).toString());
    // Set what it should do on click
    tableFooterCellNode.addEventListener('click', (_) => {
      invoke('push_row', { tableOid: tableOid })
        .catch(async (e) => {
          await message(e, {
            title: 'Error while adding new row into table.',
            kind: 'error'
          });
        });
    });
  }

  // Set up a channel to populate the rows of the table
  let rowOids: number[] = [];
  const onReceiveRow = new Channel<TableCell>();
  let currentRowNode: HTMLTableRowElement | null = null;
  onReceiveRow.onmessage = (row) => {
    if ('rowOid' in row) {
      // New row
      rowOids.push(row.rowOid);
      currentRowNode = document.createElement('tr');
      currentRowNode.insertAdjacentHTML('beforeend', `<td style="text-align: center;">${row.rowOid}</td>`);
      tableBodyNode?.insertAdjacentElement('beforeend', currentRowNode);

      // TODO context menu for OID
    } else {
      // Add cell to current row
      if (currentRowNode != null) {
        let tableCellNode: HTMLElement = document.createElement('td');
        tableCellNode.innerText = row.displayValue;
        currentRowNode.insertAdjacentElement('beforeend', tableCellNode);

        // TODO add context menu for cell
      }
    }
  };

  // Send a command to Rust to get the list of rows from the database
  await invoke("get_table_data", { tableOid: tableOid, cellChannel: onReceiveRow })
    .catch(async e => {
      await message(e, {
        title: 'Error while retrieving rows of table.',
        kind: 'error'
      });
    });
}


// Add initial listeners
window.addEventListener("DOMContentLoaded", () => {
  document.querySelector('#add-new-table-button')?.addEventListener("click", createTable);

  updateTableListAsync();
});

listen<any>("update-table-list", updateTableListAsync);
listen<number>("update-table-data", (e) => displayTable(e.payload));