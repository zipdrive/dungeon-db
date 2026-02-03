import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { Channel } from "@tauri-apps/api/core";
import { listen } from '@tauri-apps/api/event';
import { message } from "@tauri-apps/plugin-dialog";
import { TableCellChannelPacket, TableColumnMetadata, TableRowCellChannelPacket, executeAsync, openDialogAsync, queryAsync } from './backendutils';
import { addTableColumnCellToRow } from "./tableutils";

const urlParams = new URLSearchParams(window.location.search);
const urlParamTableOid = urlParams.get('table_oid');
console.debug(`table.html page loaded with table_oid=${urlParamTableOid ?? 'NULL'}`);





if (urlParamTableOid) {

  const tableOid: number = parseInt(urlParamTableOid);
  const urlParamTablePageNum = urlParams.get('page_num') ?? '1';
  const pageNum = parseInt(urlParamTablePageNum);
  const urlParamTablePageSize = urlParams.get('page_size') ?? '1000';
  const pageSize = parseInt(urlParamTablePageSize);

  /**
   * Adds a row to the current table.
   * @param tableBodyNode 
   * @param rowOid 
   */
  function addRowToTable(tableBodyNode: HTMLElement, rowOid: number, rowIndex: number): HTMLTableRowElement {
    let tableRowNode: HTMLTableRowElement = document.createElement('tr');
    tableRowNode.id = `table-content-row-${rowOid}`;
    let tableRowIndexNode = document.createElement('td');
    tableRowIndexNode.style.position = 'sticky';
    tableRowIndexNode.style.left = '0';
    tableRowIndexNode.style.textAlign = 'center';
    tableRowIndexNode.style.padding = '2px 6px';
    tableRowIndexNode.style.zIndex = '1';
    tableRowIndexNode.colSpan = 2;
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
                tableOid: tableOid,
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
                tableOid: tableOid,
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
  async function refreshTableAsync() {
    // Strip the former contents of the table
    let tableNode: HTMLTableElement | null = document.querySelector('#table-content');
    if (tableNode)
      tableNode.innerHTML = '<colgroup><col span="1" style="width: 2em;"><col span="1"></colgroup><tbody></tbody><thead><tr><th colspan="2" style="position: sticky; left: 0px; z-index: 1;"></th></tr></thead><tfoot><tr></tr></tfoot>';
    let tableColgroupNode: HTMLElement | null = document.querySelector('#table-content > colgroup');
    let tableHeaderRowNode: HTMLTableRowElement | null = document.querySelector('#table-content > thead > tr');
    let tableBodyNode: HTMLElement | null = document.querySelector('#table-content > tbody');

    // Set up a channel to populate the list of user-defined columns
    let tableColumnList: TableColumnMetadata[] = []
    const onReceiveColumn = new Channel<TableColumnMetadata>();
    onReceiveColumn.onmessage = (column) => {
      // Add the column to the list of columns
      const columnOid = column.oid;
      const columnOrdering = column.columnOrdering;
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
    let tableAddColumnHeaderNode = document.createElement('th');
    if (tableAddColumnHeaderNode != null) {
      tableAddColumnHeaderNode.id = 'add-new-column-button';
      tableAddColumnHeaderNode.innerText = 'Add New Column';
      tableAddColumnHeaderNode.addEventListener('click', async (_) => {
        await openDialogAsync({
          invokeAction: "dialog_create_table_column", 
          invokeParams: {
            tableOid: tableOid,
            columnOrdering: null
          }
        });
      });
      tableHeaderRowNode?.insertAdjacentElement('beforeend', tableAddColumnHeaderNode);
    }

    // Set the span of the footer
    let tableFooterRowNode: HTMLElement | null = document.querySelector('#table-content > tfoot > tr');
    let tableFooterCellNode = document.createElement('td');
    tableFooterCellNode.id = 'add-new-row-button';
    tableFooterCellNode.innerHTML = '<div style="position: sticky; left: 0; right: 0;">Add New Row</div>';
    // Set the footer to span the entire row
    tableFooterCellNode.setAttribute('colspan', (tableColumnList.length + 3).toString());
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
    const onReceiveCell = new Channel<TableCellChannelPacket>();
    let currentRowNode: HTMLTableRowElement | null = null;
    let currentRowOid: number | null = null;
    onReceiveCell.onmessage = (cell) => {
      if ('rowOid' in cell) {
        // New row
        const rowOid = cell.rowOid;
        const rowIndex = cell.rowIndex;
        currentRowOid = rowOid;
        if (tableBodyNode) {
          currentRowNode = addRowToTable(tableBodyNode, rowOid, rowIndex);
        }
      } else {
        // Add cell to current row
        if (currentRowNode && currentRowOid) {
          // Get current row and column OID
          addTableColumnCellToRow(currentRowNode, tableOid, currentRowOid, cell);
        }
      }
    };

    // Send a command to Rust to get the list of rows from the database
    await queryAsync({
      invokeAction: "get_table_data",
      invokeParams: {
        tableOid: tableOid, 
        pageNum: pageNum,
        pageSize: pageSize,
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
  async function updateRowAsync(rowOid: number) {
    let tableRowNode: HTMLTableRowElement | null = document.getElementById(`table-content-row-${rowOid}`) as HTMLTableRowElement;

    // Set up a channel to populate the columns of the table
    const onReceiveCell = new Channel<TableRowCellChannelPacket>();
    onReceiveCell.onmessage = (cell) => {
      if ('rowExists' in cell) {
        if (cell.rowExists) {
          if (tableRowNode) {
            // Clear all columns from row, other than Index
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
        if (tableRowNode) {
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
  window.addEventListener("DOMContentLoaded", async () => {
    refreshTableAsync();
  });

  listen<number>("update-table-data", (e) => {
    navigator.locks.request('table-content', async () => {
      if (e.payload == tableOid) {
        await refreshTableAsync();
      }
    });
  });
  listen<[number, number]>("update-table-row", (e) => {
    const updateTableOid = e.payload[0];
    const updateRowOid = e.payload[1];
    if (updateTableOid == tableOid) {
      navigator.locks.request('table-content', async () => await updateRowAsync(updateRowOid));
    }
  });

}