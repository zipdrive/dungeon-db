import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { Channel } from "@tauri-apps/api/core";
import { listen } from '@tauri-apps/api/event';
import { message } from "@tauri-apps/plugin-dialog";
import { TableCellChannelPacket, TableColumnMetadata, TableRowCellChannelPacket, executeAsync, openDialogAsync, queryAsync } from './backendutils';
import { addTableColumnCellToRow } from "./tableutils";
import { makeColumnsReorderable, makeColumnsResizable } from "./frontendutils";

const urlParams = new URLSearchParams(window.location.search);
const urlParamTableOid = urlParams.get('table_oid');
console.debug(`table.html page loaded with table_oid=${urlParamTableOid ?? 'NULL'}`);





if (urlParamTableOid) {

  const tableOid: number = parseInt(urlParamTableOid);
  const urlParamParentRowOid = urlParams.get('parent_oid');
  const parentRowOid: number | null = urlParamParentRowOid ? parseInt(urlParamParentRowOid) : null;
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
    tableRowNode.classList.add('reorderable-row');
    let tableRowIndexNode = document.createElement('td');
    tableRowIndexNode.style.position = 'sticky';
    tableRowIndexNode.style.left = '0';
    tableRowIndexNode.style.textAlign = 'center';
    tableRowIndexNode.style.padding = '2px 0';
    tableRowIndexNode.style.zIndex = '1';
    tableRowIndexNode.innerText = rowIndex.toString();
    tableRowNode.insertAdjacentElement('beforeend', tableRowIndexNode);
    tableBodyNode?.insertAdjacentElement('beforeend', tableRowNode);

    // Add listener to pull up context menu
    tableRowIndexNode.addEventListener('contextmenu', async (e) => {
      e.preventDefault();
      e.returnValue = false;

      const contextMenuItems = await Promise.all([
        MenuItem.new({
          text: 'Edit Row',
          action: async () => {
            await openDialogAsync({
              invokeAction: 'dialog_object_data',
              invokeParams: {
                tableOid: tableOid,
                rowOid: rowOid,
                title: 'Edit Row'
              }
            })
          }
        }),
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
    console.debug("Now refreshing the entire contents of the table.");

    // Record the old scroll position
    let pageNode: HTMLDivElement = document.getElementById('page') as HTMLDivElement;
    const scrollHorizontalPosition: number = pageNode.scrollLeft;
    const scrollVerticalPosition: number = pageNode.scrollTop;

    // Set up a stylesheet
    let tableColStyleNode: HTMLStyleElement = document.getElementById('column-stylesheet') as HTMLStyleElement;
    tableColStyleNode.innerHTML = '';

    // Strip the former contents of the table
    let tableNode: HTMLTableElement | null = document.querySelector('#table-content');
    if (tableNode)
      tableNode.innerHTML = '<tbody></tbody><thead><tr><th style="position: sticky; left: 0px; z-index: 1; width: 3em;"></th></tr></thead><tfoot><tr></tr></tfoot>';
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
        // Create a style class for the column
        tableColStyleNode.insertAdjacentHTML('beforeend', `.table-content-column${columnOid} { ${column.columnStyle} } `);

        // Add a label to the column header
        tableHeaderNode.innerText = column.name;
        tableHeaderNode.classList.add(`table-content-column${columnOid}`);
        tableHeaderNode.classList.add('resizable-column');
        tableHeaderNode.classList.add('reorderable-column');
        tableHeaderNode.dataset.columnOid = columnOid.toString();
        tableHeaderNode.dataset.columnOrdering = columnOrdering.toString();
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

    // Allow columns to be reordered
    if (tableHeaderRowNode) {
      makeColumnsReorderable(tableHeaderRowNode, 
        (reorderedColumnHeader, columnHeaderToImmediateLeft) => {
          // Move the column's data in subsequent rows without refreshing
        },
        async (reorderedColumnHeader, columnHeaderToImmediateRight) => {
          // Readjust the column ordering
          if (reorderedColumnHeader && reorderedColumnHeader.dataset.columnOid && reorderedColumnHeader.dataset.columnOrdering) {
            const reorderedColumnOid: number = parseInt(reorderedColumnHeader.dataset.columnOid);
            const oldColumnOrdering: number = parseInt(reorderedColumnHeader.dataset.columnOrdering);
            const newColumnOrdering: number | null = columnHeaderToImmediateRight && columnHeaderToImmediateRight.dataset.columnOrdering ? parseInt(columnHeaderToImmediateRight.dataset.columnOrdering) : null;
            await executeAsync({
              reorderTableColumn: {
                tableOid: tableOid,
                columnOid: reorderedColumnOid,
                oldColumnOrdering: oldColumnOrdering,
                newColumnOrdering: newColumnOrdering
              }
            });
          }
        }
      );
    }

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
      if ('rowIndex' in cell) {
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
          addTableColumnCellToRow(currentRowNode, cell);
        }
      }
    };

    // Send a command to Rust to get the list of rows from the database
    await queryAsync({
      invokeAction: "get_table_data",
      invokeParams: {
        tableOid: tableOid, 
        parentRowOid: parentRowOid,
        pageNum: pageNum,
        pageSize: pageSize,
        cellChannel: onReceiveCell 
      }
    });

    // Make the columns of the table resizable
    makeColumnsResizable(
      (resizedCell, newColumnWidth) => {

      }
    );

    // Set the scrolling position back to what it was previously
    pageNode.scrollLeft = scrollHorizontalPosition;
    pageNode.scrollTop = scrollVerticalPosition;
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
          addTableColumnCellToRow(tableRowNode, cell);
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
      const updateTableOid = e.payload;
      if (updateTableOid == tableOid) {
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