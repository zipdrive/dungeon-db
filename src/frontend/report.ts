import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { Channel } from "@tauri-apps/api/core";
import { listen } from '@tauri-apps/api/event';
import { message } from "@tauri-apps/plugin-dialog";
import { ReportCellChannelPacket, ReportColumnMetadata, ReportRowCellChannelPacket, executeAsync, openDialogAsync, queryAsync, queryStreamAsync } from './backendutils';
import { updateTableColumnCell } from "./tableutils";

const urlParams = new URLSearchParams(window.location.search);
const urlParamReportOid = urlParams.get('report_oid');





if (urlParamReportOid) {

  const reportOid: number = parseInt(urlParamReportOid);
  const urlParamReportPageNum = urlParams.get('page_num') ?? '1';
  const pageNum = parseInt(urlParamReportPageNum);
  const urlParamReportPageSize = urlParams.get('page_size') ?? '1000';
  const pageSize = parseInt(urlParamReportPageSize);

  /**
   * Adds a row to the current table.
   * @param tableBodyNode 
   * @param rowOid 
   */
  function addRowToTable(tableBodyNode: HTMLElement, rowOid: number, rowIndex: number): HTMLTableRowElement {
    let tableRowNode: HTMLTableRowElement = document.createElement('tr');
    tableRowNode.id = `report-content-row-${rowOid}`;
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
          text: 'Delete Row',
          action: async () => {
            await executeAsync({
              deleteTableRow: {
                tableOid: reportOid, // TODO
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
  async function refreshReportAsync() {
    // Strip the former contents of the table
    let tableNode: HTMLTableElement | null = document.querySelector('#report-content');
    if (tableNode)
      tableNode.innerHTML = '<colgroup><col span="1" style="width: 2em;"><col span="1"></colgroup><tbody></tbody><thead><tr><th colspan="2" style="position: sticky; left: 0px; z-index: 1;"></th></tr></thead><tfoot><tr></tr></tfoot>';
    let reportColgroupNode: HTMLElement | null = document.querySelector('#report-content > colgroup');
    let reportHeaderRowNode: HTMLTableRowElement | null = document.querySelector('#report-content > thead > tr');
    let reportBodyNode: HTMLElement | null = document.querySelector('#report-content > tbody');

    // Set up a channel to populate the list of user-defined columns
    let reportColumnList: ReportColumnMetadata[] = []
    const onReceiveColumn = new Channel<ReportColumnMetadata>();
    onReceiveColumn.onmessage = (column) => {
      let columnOid: number;
      let columnOrdering: number;
      let columnName: string;
      let columnStyle: string;
      if ('formula' in column) {
        columnOid = column.formula.oid;
        columnOrdering = column.formula.columnOrdering;
        columnName = column.formula.name;
        columnStyle = column.formula.columnStyle;
      } else {
        columnOid = column.subreport.oid;
        columnOrdering = column.subreport.columnOrdering;
        columnName = column.subreport.name;
        columnStyle = column.subreport.columnStyle;
      }
      // Add the column to the list of columns
      reportColumnList.push(column);

      // Add a header for the column
      let reportHeaderNode: HTMLElement | null = document.createElement('th');
      if (reportHeaderNode != null) {
        let reportColNode: HTMLElement = document.createElement('col');
        reportColNode.setAttribute('span', '1');
        reportColNode.setAttribute('style', columnStyle);
        reportColgroupNode?.appendChild(reportColNode);

        reportHeaderNode.innerText = columnName;
        reportHeaderRowNode?.appendChild(reportHeaderNode);

        // Add listener to pull up context menu
        reportHeaderNode.addEventListener('contextmenu', async (e) => {
          e.preventDefault();
          e.returnValue = false;

          const contextMenuItems = await Promise.all([
            MenuItem.new({
              text: 'Insert New Column',
              action: async () => {
                await openDialogAsync({
                  createReportColumn: {
                    reportOid: reportOid,
                    columnOrdering: columnOrdering
                  }
                });
              }
            }),
            MenuItem.new({
              text: 'Edit Column',
              action: async () => {
                await openDialogAsync({
                  editReportColumnMetadata: {
                    reportOid: reportOid,
                    columnOid: columnOid
                  }
                });
              }
            }),
            MenuItem.new({
              text: 'Delete Column',
              action: async () => {
                await executeAsync({
                  deleteReportColumn: {
                    reportOid: reportOid,
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
                title: 'Error while displaying context menu for report column.',
                kind: 'error'
              });
            });
        });
      }
    };

    // Send a command to Rust to get the list of table columns from the database
    await queryStreamAsync([{
      reportColumns: {
        reportOid: reportOid
      }
    }, onReceiveColumn]);

    // Add a final column header that is a button to add a new column
    let tableAddColumnHeaderNode = document.createElement('th');
    if (tableAddColumnHeaderNode != null) {
      tableAddColumnHeaderNode.id = 'add-new-column-button';
      tableAddColumnHeaderNode.innerText = 'Add New Column';
      tableAddColumnHeaderNode.addEventListener('click', async (_) => {
        await openDialogAsync({
          createReportColumn: {
            reportOid: reportOid,
            columnOrdering: null
          }
        });
      });
      reportHeaderRowNode?.appendChild(tableAddColumnHeaderNode);
    }

    // Set the span of the footer
    let tableFooterRowNode: HTMLElement | null = document.querySelector('#report-content > tfoot > tr');
    let tableFooterCellNode = document.createElement('td');
    tableFooterCellNode.id = 'add-new-row-button';
    tableFooterCellNode.innerHTML = '<div style="position: sticky; left: 0; right: 0;">Add New Row</div>';
    // Set the footer to span the entire row
    tableFooterCellNode.setAttribute('colspan', (reportColumnList.length + 3).toString());
    // Set what it should do on click
    tableFooterCellNode.addEventListener('click', async (_) => {
      // TODO push a row into the base table
      /*
      await executeAsync({
        pushTableRow: {
          tableOid: reportOid
        }
      })
      .catch(async (e) => {
        await message(e, {
          title: 'Error while adding new row into table.',
          kind: 'error'
        });
      });
      */
    });
    tableFooterRowNode?.appendChild(tableFooterCellNode);

    // Set up a channel to populate the rows of the table
    const onReceiveCell = new Channel<ReportCellChannelPacket>();
    let currentRowNode: HTMLTableRowElement | null = null;
    let currentRowOid: number | null = null;
    onReceiveCell.onmessage = (cell) => {
      if ('rowStart' in cell) {
        // New row
        const rowOid = cell.rowStart.rowOid;
        const rowIndex = cell.rowStart.rowIndex;
        currentRowOid = rowOid;
        if (reportBodyNode) {
          currentRowNode = addRowToTable(reportBodyNode, rowOid, rowIndex);
        }
      } else if ('columnValue' in cell) {
        // Add editable cell to current row
        if (currentRowNode) {
          let tableCellNode: HTMLTableCellElement = document.createElement('td');
          tableCellNode.id = `table-content-column${cell.columnValue.columnOid}-row${cell.columnValue.rowOid}`;
          tableCellNode.classList.add(`table-content-column${cell.columnValue.columnOid}`);
          tableCellNode.classList.add('resizable-column');
          tableCellNode.dataset.tableOid = cell.columnValue.tableOid.toString();
          tableCellNode.dataset.columnOid = cell.columnValue.columnOid.toString();
          tableCellNode.dataset.rowOid = cell.columnValue.rowOid.toString();
          currentRowNode.appendChild(tableCellNode);

          // Insert an input into the td element
          updateTableColumnCell(tableCellNode, cell.columnValue, true);
        }
      } else if ('readOnlyValue' in cell) {
        // Add readonly cell to current row
        if (currentRowNode) {
          let readOnlyNode: HTMLTableCellElement = document.createElement('td');
          readOnlyNode.id = `report-content-column-row`;
          readOnlyNode.classList.add('cell-readonly');
          readOnlyNode.classList.add('resizable-column');
          readOnlyNode.innerText = cell.readOnlyValue.displayValue ?? '';
          currentRowNode.appendChild(readOnlyNode);
        }
      } else {
        // Add subreport link to cell
        if (currentRowNode) {
          let subreportNode: HTMLTableCellElement = document.createElement('td');
          subreportNode.id = `report-content-column-row`;
          subreportNode.classList.add('clickable-text');
          subreportNode.classList.add('resizable-column');
          subreportNode.innerText = 'Subreport';
          currentRowNode.appendChild(subreportNode);

          // Open the subreport when clicked
          subreportNode.addEventListener('click', async (_) => {
            await openDialogAsync({
              report: {
                reportOid: cell.subreport.subreportOid,
                reportName: ''
              }
            })
            .catch(async (e) => {
              await message(e, {
                title: 'An error occurred while opening subreport.',
                kind: 'error'
              });
            });
          });
        }
      }
    };

    // Send a command to Rust to get the list of rows from the database
    await queryStreamAsync([{
      reportPageCells: {
        reportOid: reportOid, 
        parentRowOid: null, // TODO
        pageNum: pageNum,
        pageSize: pageSize
      }
    }, onReceiveCell]);
  }


  // Add initial listeners
  window.addEventListener("DOMContentLoaded", async () => {
    refreshReportAsync();
  });

}