import { Channel, invoke } from "@tauri-apps/api/core";
import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { message } from "@tauri-apps/plugin-dialog";

type ColumnType = { primitive: string } 
  | { singleSelectDropdown: number }
  | { multiSelectDropdown: number }
  | { reference: number } 
  | { childObject: number } 
  | { childTable: number };

type ColumnCellInfo = { 
    columnOid: number, 
    columnType: ColumnType, 
    displayValue: string | null 
};

/**
 * Adds a cell representing a table cell to the end of a row.
 * @param rowNode The row of the table to insert the cell into.
 * @param tableOid The OID of the table that the cell belongs to.
 * @param rowOid The OID of the row of the table that the cell belongs to.
 * @param cell Information about the cell itself.
 */
export function addTableCellToRow(rowNode: HTMLTableRowElement, tableOid: number, rowOid: number, cell: ColumnCellInfo) {
  const columnOid = cell.columnOid;

  // Insert cell node
  let tableCellNode: HTMLTableCellElement = document.createElement('td');

  /**
   * Sets the cell to enter view mode.
   */
  function setViewMode() {
    if (cell.displayValue) {
        tableCellNode.innerText = cell.displayValue;
    } else {
        tableCellNode.innerText = '';
        tableCellNode.classList.add('null-cell');
    }
  }

  /**
   * Sets the cell to enter edit mode.
   */
  function setEditMode() {
    tableCellNode.classList.remove('null-cell');
    if ('primitive' in cell.columnType) {
      // TODO
    } else if ('singleSelectDropdown' in cell.columnType) {
      // TODO
    } else if ('multiSelectDropdown' in cell.columnType) {
      // TODO
    } else if ('reference' in cell.columnType) {
      // TODO
    } else if ('childObject' in cell.columnType) {
      // TODO
    } else if ('childTable' in cell.columnType) {
      // TODO
    }
  }

  // Add the cell to the row
  setViewMode();
  rowNode.insertAdjacentElement('beforeend', tableCellNode);

  // Add listener to start editing when clicked
  tableCellNode.addEventListener('click', async (_) => {
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