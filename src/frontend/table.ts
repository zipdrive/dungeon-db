import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { Channel } from "@tauri-apps/api/core";
import { listen } from '@tauri-apps/api/event';
import { message } from "@tauri-apps/plugin-dialog";
import { TableCellChannelPacket, TableColumnMetadata, TableRowCellChannelPacket, executeAsync, openDialogAsync, queryAsync } from './backendutils';
import { addTableColumnCellToRow, updateTableColumnCell } from "./tableutils";
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
    tableRowIndexNode.classList.add('resizable-column');
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
   * Does a shallow refresh of all cells.
   * Should be used when an action has been performed that may alter table validations.
   */
  async function refreshAllCellsAsync() {
    // Record the old scroll position
    let pageNode: HTMLDivElement = document.getElementById('page') as HTMLDivElement;
    const scrollHorizontalPosition: number = pageNode.scrollLeft;
    const scrollVerticalPosition: number = pageNode.scrollTop;

    // Set up a channel to populate the rows of the table
    const onReceiveCell = new Channel<TableCellChannelPacket>();
    onReceiveCell.onmessage = async (cell) => {
      if ('rowIndex' in cell) {
        // Ignore
      } else {
        // Retrieve the cell
        let tableCellElement: HTMLTableCellElement | null = document.getElementById(`table-content-column${cell.columnOid}-row${cell.rowOid}`) as HTMLTableCellElement;
        if (tableCellElement) {
          updateTableColumnCell(tableCellElement, cell, true);
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

    // Set the scrolling position back to what it was previously
    pageNode.scrollLeft = scrollHorizontalPosition;
    pageNode.scrollTop = scrollVerticalPosition;
  }

  /**
   * Does a full refresh of the table.
   * Should be used when a row or column has been altered.
   */
  async function refreshTableAsync() {
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
      tableNode.innerHTML = '<colgroup><col id="table-content-index-widthcontrol" span="1" style="width: 3em;"></colgroup><tbody></tbody><thead><tr><th class="resizable-column" style="position: sticky; left: 0px; z-index: 1;"></th></tr></thead><tfoot><tr></tr></tfoot>';
    let tableHeaderRowNode: HTMLTableRowElement | null = tableNode?.querySelector('thead > tr') || null;
    let tableBodyNode: HTMLElement | null = tableNode?.querySelector('tbody') || null;

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

        // Add a col for the column, to control the width
        tableNode?.querySelector('colgroup')?.insertAdjacentHTML('beforeend', `<col span="1" id="table-content-column${columnOid}-widthcontrol">`);

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
    const onReceiveCell = new Channel<TableCellChannelPacket>();
    let currentRowNode: HTMLTableRowElement | null = null;
    onReceiveCell.onmessage = async (cell) => {
      if ('rowIndex' in cell) {
        // New row
        const rowOid = cell.rowOid;
        const rowIndex = cell.rowIndex;
        if (tableBodyNode) {
          currentRowNode = addRowToTable(tableBodyNode, rowOid, rowIndex);
        }
      } else {
        // Add cell to current row
        if (currentRowNode) {
          let tableCellNode: HTMLTableCellElement = document.createElement('td');
          tableCellNode.id = `table-content-column${cell.columnOid}-row${cell.rowOid}`;
          tableCellNode.classList.add(`table-content-column${cell.columnOid}`);
          tableCellNode.classList.add('resizable-column');
          tableCellNode.dataset.tableOid = cell.tableOid.toString();
          tableCellNode.dataset.columnOid = cell.columnOid.toString();
          tableCellNode.dataset.rowOid = cell.rowOid.toString();
          currentRowNode.appendChild(tableCellNode);

          // Insert an input into the td element
          updateTableColumnCell(tableCellNode, cell, true);
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
        console.debug(`columnOid: ${resizedCell.dataset.columnOid}`);
        let widthcontrolCol: HTMLElement | null = document.getElementById(resizedCell.dataset.columnOid ? `table-content-column${resizedCell.dataset.columnOid}-widthcontrol` : `table-content-index-widthcontrol`);
        if (widthcontrolCol) {
          widthcontrolCol.style.width = `${newColumnWidth}px`;
        }
      },
      async (resizedCell, newColumnWidth) => {
        // Update the column CSS style to incorporate the new width
        if (resizedCell.dataset.columnOid) {
          await executeAsync({
            editTableColumnWidth: {
              tableOid: tableOid,
              columnOid: parseInt(resizedCell.dataset.columnOid),
              columnWidth: Math.round(newColumnWidth)
            }
          })
          .catch(async (e) => {
            await message(e, {
              title: 'An error occurred while adjusting column width.',
              kind: 'error'
            })
          });
        }
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

  listen<number>("update-table-data-deep", (e) => {
    navigator.locks.request('table-content', async () => {
      const updateTableOid = e.payload;
      if (updateTableOid == tableOid) {
        await refreshTableAsync();
      }
    });
  });
  listen<number>("update-table-data-shallow", (e) => {
    navigator.locks.request('table-content', async () => {
      const updateTableOid = e.payload;
      if (updateTableOid == tableOid) {
        await refreshAllCellsAsync();
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