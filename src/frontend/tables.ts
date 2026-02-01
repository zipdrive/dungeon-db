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
  await queryAsync({
    invokeAction: "get_table_list", 
    invokeParams: { tableChannel: onReceiveUpdatedTable }
  });
}

/**
 * Opens the dialog to create a new table.
 */
export async function createTable() {
  await openDialogAsync({
    invokeAction: "dialog_create_table", 
    invokeParams: {}
  });
}




let currentTableOid: number = NaN;

/**
 * Adds a row to the current table.
 * @param tableBodyNode 
 * @param rowOid 
 */
function addRowToTable(tableBodyNode: HTMLElement, rowOid: number, rowIndex: number): HTMLTableRowElement {
  let tableRowNode: HTMLTableRowElement = document.createElement('tr');
  tableRowNode.id = `table-content-row-${rowOid}`;
  let tableRowIndexNode = document.createElement('td');
  tableRowIndexNode.style.textAlign = 'center';
  tableRowIndexNode.style.padding = '2px 6px';
  tableRowIndexNode.innerText = rowIndex.toString();
  tableRowNode.insertAdjacentElement('beforeend', tableRowIndexNode);
  tableBodyNode?.insertAdjacentElement('beforeend', tableRowNode);

  // Add listener to pull up context menu
  tableRowIndexNode.addEventListener('contextmenu', async (e) => {
    e.preventDefault();
    e.returnValue = false;

    const contextMenuItems = await Promise.all([
      MenuItem.new({
        text: 'Insert New Row',
        action: async () => {
          await executeAsync({
            insertTableRow: {
              tableOid: currentTableOid,
              rowOid: rowOid
            }
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
          await executeAsync({
            deleteTableRow: {
              tableOid: currentTableOid,
              rowOid: rowOid
            }
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
 * Displays the data for a table.
 * @param tableOid The OID of the table.
 */
export async function displayTableAsync(tableOid: number) {
  currentTableOid = tableOid;

  // Strip the former contents of the table
  let tableNode: HTMLTableElement | null = document.querySelector('#table-content');
  if (tableNode)
    tableNode.innerHTML = '<colgroup><col span="1" style="min-width: 2em;"></colgroup><tbody></tbody><thead><tr><th></th></tr></thead><tfoot><tr></tr></tfoot>';
  let tableColgroupNode: HTMLElement | null = document.querySelector('#table-content > colgroup');
  let tableHeaderRowNode: HTMLTableRowElement | null = document.querySelector('#table-content > thead > tr');
  let tableBodyNode: HTMLElement | null = document.querySelector('#table-content > tbody');

  // Set up a channel to populate the list of user-defined columns
  let tableColumnList: TableColumnMetadata[] = []
  const onReceiveColumn = new Channel<TableColumnMetadata>();
  onReceiveColumn.onmessage = (column) => {
    // Add the column to the list of columns
    const columnOid = column.oid;
    const columnOrdering = tableColumnList.length;
    tableColumnList.push(column);

    // Add a header for the column
    let tableHeaderNode: HTMLElement | null = document.createElement('th');
    if (tableHeaderNode != null) {
      let tableColNode: HTMLElement = document.createElement('col');
      tableColNode.setAttribute('span', '1');
      tableColNode.setAttribute('style', column.columnStyle);
      tableColgroupNode?.insertAdjacentElement('beforeend', tableColNode);

      tableHeaderNode.innerText = column.name;
      tableHeaderRowNode?.insertAdjacentElement('beforeend', tableHeaderNode);

      // Add listener to pull up context menu
      tableHeaderNode.addEventListener('contextmenu', async (e) => {
        e.preventDefault();
        e.returnValue = false;

        const contextMenuItems = await Promise.all([
          MenuItem.new({
            text: 'Insert New Column',
            action: async () => {
              await openDialogAsync({
                invokeAction: 'dialog_create_table_column',
                invokeParams: {
                  tableOid: tableOid,
                  columnOrdering: columnOrdering
                }
              });
            }
          }),
          MenuItem.new({
            text: 'Edit Column',
            action: async () => {
              await openDialogAsync({
                invokeAction: 'dialog_edit_table_column',
                invokeParams: {
                  tableOid: tableOid,
                  columnOid: columnOid
                }
              });
            }
          }),
          MenuItem.new({
            text: 'Delete Column',
            action: async () => {
              await executeAsync({
                deleteTableColumn: {
                  tableOid: tableOid,
                  columnOid: columnOid
                }
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
    }
  };

  // Send a command to Rust to get the list of table columns from the database
  await queryAsync({
    invokeAction: "get_table_column_list", 
    invokeParams: {
      tableOid: tableOid, 
      columnChannel: onReceiveColumn 
    }
  });

  // Add a final column header that is a button to add a new column
  const numColumns = tableColumnList.length;
  let tableAddColumnHeaderNode = document.createElement('th');
  if (tableAddColumnHeaderNode != null) {
    tableAddColumnHeaderNode.id = 'add-new-column-button';
    tableAddColumnHeaderNode.innerText = 'Add New Column';
    tableAddColumnHeaderNode.addEventListener('click', async (_) => {
      await openDialogAsync({
        invokeAction: "dialog_create_table_column", 
        invokeParams: {
          tableOid: tableOid,
          columnOrdering: numColumns
        }
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
  tableFooterCellNode.addEventListener('click', async (_) => {
    await executeAsync({
      pushTableRow: {
        tableOid: tableOid 
      }
    })
    .catch(async (e) => {
      await message(e, {
        title: 'Error while adding new row into table.',
        kind: 'error'
      });
    });
  });
  tableFooterRowNode?.insertAdjacentElement('beforeend', tableFooterCellNode);

  // Set up a channel to populate the rows of the table
  let rowIndex: number = 1;
  let rowOids: number[] = [];
  const onReceiveCell = new Channel<TableCellChannelPacket>();
  let currentRowNode: HTMLTableRowElement | null = null;
  onReceiveCell.onmessage = (cell) => {
    if ('rowOid' in cell) {
      // New row
      const rowOid = cell.rowOid;
      rowOids.push(rowOid);
      if (tableBodyNode) {
        currentRowNode = addRowToTable(tableBodyNode, rowOid, rowIndex);
        rowIndex += 1;
      }
    } else {
      // Add cell to current row
      if (currentRowNode != null) {
        // Get current row and column OID
        const rowOid = rowOids[rowOids.length - 1];
        addTableColumnCellToRow(currentRowNode, tableOid, rowOid, cell);
      }
    }
  };

  // Send a command to Rust to get the list of rows from the database
  await queryAsync({
    invokeAction: "get_table_data",
    invokeParams: {
      tableOid: tableOid, 
      cellChannel: onReceiveCell 
    }
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

  let tableRowNode: HTMLTableRowElement | null = document.getElementById(`table-content-row-${rowOid}`) as HTMLTableRowElement;

  // Set up a channel to populate the columns of the table
  const onReceiveCell = new Channel<TableRowCellChannelPacket>();
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
            tableRowNode = addRowToTable(tableBodyNode, rowOid, Infinity);

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
        addTableColumnCellToRow(tableRowNode, tableOid, rowOid, cell);
      }
    }
  };

  // Send a command to Rust to get the list of rows from the database
  await queryAsync({
    invokeAction: "get_table_row", 
    invokeParams: {
      tableOid: tableOid, 
      rowOid: rowOid, 
      cellChannel: onReceiveCell 
    }
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