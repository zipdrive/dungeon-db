import { Menu, MenuItem } from "@tauri-apps/api/menu";
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
        displayTableAsync(table.oid);
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




let currentTableOid: number = NaN;

type ColumnType = { primitive: string } 
  | { singleSelectDropdown: number }
  | { multiSelectDropdown: number }
  | { reference: number } 
  | { childObject: number } 
  | { childTable: number };

/**
 * Adds a row to the current table.
 * @param tableBodyNode 
 * @param rowOid 
 */
function addRowToTable(tableBodyNode: HTMLElement, rowOid: number): HTMLTableRowElement {
  let tableRowNode: HTMLTableRowElement = document.createElement('tr');
  tableRowNode.id = `table-content-row-${rowOid}`;
  let tableRowOidNode = document.createElement('td');
  tableRowOidNode.style.textAlign = 'center';
  tableRowOidNode.innerText = rowOid.toString();
  tableRowNode.insertAdjacentElement('beforeend', tableRowOidNode);
  tableBodyNode?.insertAdjacentElement('beforeend', tableRowNode);

  // Add listener to pull up context menu
  tableRowOidNode.addEventListener('contextmenu', async (e) => {
    e.preventDefault();
    e.returnValue = false;

    const contextMenuItems = await Promise.all([
      MenuItem.new({
        text: 'Insert New Row',
        action: async () => {
          await invoke('insert_row', {
            tableOid: currentTableOid,
            rowOid: rowOid
          })
          .catch(async e => {
            await message(e, {
              title: 'Error while inserting row into table.',
              kind: 'error'
            });
          });
        }
      }),
      MenuItem.new({
        text: 'Delete Row',
        action: async () => {
          await invoke('delete_row', {
            tableOid: currentTableOid,
            rowOid: rowOid
          })
          .catch(async e => {
            await message(e, {
              title: 'Error while deleting row from table.',
              kind: 'error'
            });
          });
        }
      })
    ]);
    const contextMenu = await Menu.new({
      items: contextMenuItems
    });
    await contextMenu.popup()
      .catch(async e => {
        await message(e, {
          title: 'Error while displaying context menu for table column.',
          kind: 'error'
        });
      });
  });

  // Return the created row
  return tableRowNode;
}

/**
 * Adds a cell to a row in the current table.
 * @param currentRowNode 
 * @param rowOid 
 * @param cell 
 */
function addCellToTableRow(currentRowNode: HTMLElement, rowOid: number, cell: { columnOid: number, columnType: ColumnType, displayValue: string | null }) {
  const columnOid = cell.columnOid;

  // Insert cell node
  let tableCellNode: HTMLTableCellElement = document.createElement('td');
  if (cell.displayValue) {
    tableCellNode.innerText = cell.displayValue;
  } else {
    tableCellNode.classList.add('null-cell');
  }
  currentRowNode.insertAdjacentElement('beforeend', tableCellNode);

  // Add listener to start editing when clicked
  tableCellNode.addEventListener('click', async (_) => {
    if ('primitive' in cell.columnType) {
      // TODO
    } else if ('singleSelectDropdown' in cell.columnType) {
      // TODO
    } else if ('multiSelectDropdown' in cell.columnType) {
      // TODO
    } else if ('reference' in cell.columnType) {
      // TODO
    } else if ('')
  });

  // Add listener to pull up context menu
  tableCellNode.addEventListener('contextmenu', async (e) => {
    e.preventDefault();
    e.returnValue = false;

    const contextMenuItems = await Promise.all([
      MenuItem.new({
        text: 'Cut',
        action: async () => {
          
        }
      }),
      MenuItem.new({
        text: 'Copy',
        action: async () => {
          
        }
      }),
      MenuItem.new({
        text: 'Paste',
        action: async () => {
          
        }
      }),
      MenuItem.new({
        text: 'Edit Cell',
        action: async () => {
          
        }
      })
    ]);
    const contextMenu = await Menu.new({
      items: contextMenuItems
    });
    await contextMenu.popup()
      .catch(async e => {
        await message(e, {
          title: 'Error while displaying context menu for table column.',
          kind: 'error'
        });
      });
  });
}

/**
 * Displays the data for a table.
 * @param tableOid The OID of the table.
 */
export async function displayTableAsync(tableOid: number) {
  console.debug(`displayTable(${tableOid}) called.`);
  currentTableOid = tableOid;

  type TableColumn = {
    oid: number, 
    name: string,
    width: number,
    columnType: ColumnType,
    isNullable: boolean,
    isUnique: boolean,
    isPrimaryKey: boolean,
  };

  type TableCell = {
    rowOid: number
  } | {
    columnOid: number,
    columnType: ColumnType,
    displayValue: string | null
  };

  // Strip the former contents of the table
  let tableNode: HTMLTableElement | null = document.querySelector('#table-content');
  if (tableNode)
    tableNode.innerHTML = '<thead><tr><th></th></tr></thead><tbody></tbody><tfoot><tr></tr></tfoot>';
  let tableHeaderRowNode: HTMLTableRowElement | null = document.querySelector('#table-content > thead > tr');
  let tableBodyNode: HTMLElement | null = document.querySelector('#table-content > tbody');

  // Set up a channel to populate the list of user-defined columns
  let tableColumnList: TableColumn[] = []
  const onReceiveColumn = new Channel<TableColumn>();
  onReceiveColumn.onmessage = (column) => {
    // Add the column to the list of columns
    const columnOid = column.oid;
    tableColumnList.push(column);

    // Add a header for the column
    let tableHeaderNode: HTMLElement | null = document.createElement('th');
    if (tableHeaderNode != null) {
      tableHeaderNode.style.columnWidth = `${column.width}px`;
      tableHeaderNode.innerText = column.name;
      tableHeaderRowNode?.insertAdjacentElement('beforeend', tableHeaderNode);

      // Add listener to pull up context menu
      tableHeaderNode.addEventListener('contextmenu', async (e) => {
        e.preventDefault();
        e.returnValue = false;

        const contextMenuItems = await Promise.all([
          MenuItem.new({
            text: 'Insert New Column'
          }),
          MenuItem.new({
            text: 'Edit Column'
          }),
          MenuItem.new({
            text: 'Delete Column'
          })
        ]);
        const contextMenu = await Menu.new({
          items: contextMenuItems
        });
        await contextMenu.popup()
          .catch(async e => {
            await message(e, {
              title: 'Error while displaying context menu for table column.',
              kind: 'error'
            });
          });
      });
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
  const numColumns = tableColumnList.length;
  let tableAddColumnHeaderNode = document.createElement('th');
  if (tableAddColumnHeaderNode != null) {
    tableAddColumnHeaderNode.id = 'add-new-column-button';
    tableAddColumnHeaderNode.innerText = 'Add New Column';
    tableAddColumnHeaderNode.addEventListener('click', async (_) => {
      await invoke("dialog_create_table_column", {
        tableOid: tableOid,
        columnOrdering: numColumns
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
  let tableFooterRowNode: HTMLElement | null = document.querySelector('#table-content > tfoot > tr');
  let tableFooterCellNode = document.createElement('td');
  tableFooterCellNode.id = 'add-new-row-button';
  tableFooterCellNode.innerText = 'Add New Row';
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
  tableFooterRowNode?.insertAdjacentElement('beforeend', tableFooterCellNode);

  // Set up a channel to populate the rows of the table
  let rowOids: number[] = [];
  const onReceiveCell = new Channel<TableCell>();
  let currentRowNode: HTMLTableRowElement | null = null;
  onReceiveCell.onmessage = (cell) => {
    if ('rowOid' in cell) {
      // New row
      const rowOid = cell.rowOid;
      rowOids.push(rowOid);
      if (tableBodyNode)
        currentRowNode = addRowToTable(tableBodyNode, rowOid);
    } else {
      // Add cell to current row
      if (currentRowNode != null) {
        // Get current row and column OID
        const rowOid = rowOids[rowOids.length - 1];
        addCellToTableRow(currentRowNode, rowOid, cell);
      }
    }
  };

  // Send a command to Rust to get the list of rows from the database
  await invoke("get_table_data", { tableOid: tableOid, cellChannel: onReceiveCell })
    .catch(async e => {
      await message(e, {
        title: 'Error while retrieving rows of table.',
        kind: 'error'
      });
    });
}

/**
 * Updates a single row of the current table.
 * @param tableOid 
 * @param rowOid 
 * @returns 
 */
export async function updateRowAsync(tableOid: number, rowOid: number) {
  if (tableOid != currentTableOid) {
    await displayTableAsync(tableOid);
    return;
  }

  type TableCell = {
    rowExists: boolean
  } | {
    columnOid: number,
    columnType: ColumnType,
    displayValue: string | null
  };

  let tableRowNode: HTMLTableRowElement | null = document.getElementById(`table-content-row-${rowOid}`) as HTMLTableRowElement;

  // Set up a channel to populate the columns of the table
  const onReceiveCell = new Channel<TableCell>();
  onReceiveCell.onmessage = (cell) => {
    if ('rowExists' in cell) {
      if (cell.rowExists) {
        if (tableRowNode) {
          // Clear all columns from row, other than OID
          while (tableRowNode.lastElementChild && tableRowNode.childElementCount > 1) {
            tableRowNode.removeChild(tableRowNode.lastElementChild);
          }
        } else {
          let tableBodyNode: HTMLElement | null = document.querySelector('#table-content > tbody');
          if (tableBodyNode) {
            // Insert new row at end of table
            tableRowNode = addRowToTable(tableBodyNode, rowOid);

            // Rearrange rows so that it is in the correct position
            // TODO
          }
        }
      } else {
        // Delete row
        tableRowNode?.remove();
        tableRowNode = null;
      }
    } else {
      // Add cell to current row
      if (tableRowNode != null) {
        addCellToTableRow(tableRowNode, rowOid, cell);
      }
    }
  };

  // Send a command to Rust to get the list of rows from the database
  await invoke("get_table_row", { tableOid: tableOid, rowOid: rowOid, cellChannel: onReceiveCell })
    .catch(async e => {
      await message(e, {
        title: 'Error while retrieving row of table.',
        kind: 'error'
      });
    });
}


// Add initial listeners
window.addEventListener("DOMContentLoaded", () => {
  document.querySelector('#add-new-table-button')?.addEventListener("click", createTable);

  navigator.locks.request('table-sidebar', async () => await updateTableListAsync());
});

listen<any>("update-table-list", (_) => {
  navigator.locks.request('table-sidebar', async () => await updateTableListAsync());
});
listen<number>("update-table-data", (e) => {
  navigator.locks.request('table-content', async () => await displayTableAsync(e.payload));
});
listen<[number, number]>("update-table-row", (e) => {
  navigator.locks.request('table-content', async () => await updateRowAsync(e.payload[0], e.payload[1]));
});