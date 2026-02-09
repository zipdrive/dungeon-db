import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { message } from "@tauri-apps/plugin-dialog";
import { DropdownValue, TableColumnCell, executeAsync, openDialogAsync, queryAsync } from './backendutils';
import { Channel } from "@tauri-apps/api/core";


/**
 * Adds a cell representing a table cell to the end of a row.
 * @param rowNode The row of the table to insert the cell into.
 * @param tableOid The OID of the table that the cell belongs to.
 * @param rowOid The OID of the row of the table that the cell belongs to.
 * @param cell Information about the cell itself.
 */
export async function addTableColumnCellToRow(rowNode: HTMLTableRowElement, cell: TableColumnCell): Promise<HTMLElement> {
  const tableOid = cell.tableOid;
  const rowOid = cell.rowOid;
  const columnOid = cell.columnOid;

  // Insert cell node
  let tableCellNode: HTMLTableCellElement = document.createElement('td');

  // Add null class for CSS
  if (!cell.displayValue) {
    tableCellNode.classList.add('cell-null');
  }

  // Add validation errors
  if (cell.failedValidations.length > 0) {
    tableCellNode.classList.add('cell-error');

    let failureMsgTooltipNode = document.createElement('div');
    failureMsgTooltipNode.classList.add('cell-error-tooltip');
    failureMsgTooltipNode.innerText = cell.failedValidations.map((failure) => failure.description).join('\n');
    tableCellNode.insertAdjacentElement('beforeend', failureMsgTooltipNode);
  }

  // Add the cell to the row
  rowNode.insertAdjacentElement('beforeend', tableCellNode);

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
  
  // Differentiate based on the column type
  if ('primitive' in cell.columnType) {
    switch (cell.columnType.primitive) {
      case 'Text':
      case 'JSON':
      case 'Number':
      case 'Integer':
      case 'Date':
      case 'Timestamp': {
        // Set cell to be editable
        let editableDivNode: HTMLDivElement = document.createElement('div');
        editableDivNode.contentEditable = 'true';
        if (cell.displayValue) {
          editableDivNode.innerText = cell.displayValue;
        } else {
          editableDivNode.setAttribute('placeholder', '— NULL —');
        }

        const primitiveType = cell.columnType.primitive;

        // Set up an event listener for when the value is changed
        editableDivNode.addEventListener('focusout', async (_) => {
          let newPrimitiveValue: string | null = editableDivNode.innerText.trimEnd();

          // If necessary, convert value into a regularized format before uploading to database
          switch (primitiveType) {
            case 'Date':
              let date: number = Date.parse(newPrimitiveValue);
              if (!isNaN(date)) {
                newPrimitiveValue = new Date(date).toISOString();
              }
              break;
            case 'Timestamp':
              let timestamp: number = Date.parse(newPrimitiveValue);
              if (!isNaN(timestamp)) {
                newPrimitiveValue = new Date(timestamp).toISOString();
              }
              break;
          }

          await executeAsync({
            updateTableCellStoredAsPrimitiveValue: {
              tableOid: tableOid,
              rowOid: rowOid,
              columnOid: columnOid,
              value: newPrimitiveValue == '' ? null : newPrimitiveValue
            }
          })
          .catch(async e => {
            await message(e, {
              title: "Unable to update value.",
              kind: 'warning'
            });
          });
        });

        // Add the div to the cell
        tableCellNode.insertAdjacentElement('beforeend', editableDivNode);
        return editableDivNode;
      }
      case 'Boolean': {
        let inputNode: HTMLInputElement = document.createElement('input');
        inputNode.type = 'checkbox';
        inputNode.checked = cell.displayValue == '1';
        tableCellNode.insertAdjacentElement('beforeend', inputNode);
        return inputNode;
      }
      case 'File': {
        // Show primary key of object, cut off by ellipsis if too long
        tableCellNode.innerText = cell.displayValue ?? '';
        tableCellNode.tabIndex = 0;
        return tableCellNode;
      }
      case 'Image': {
        // TODO display image thumbnail
        tableCellNode.innerText = cell.displayValue ?? '';
        tableCellNode.tabIndex = 0;
        return tableCellNode;
      }
      default:
        return tableCellNode;
    }
  } else if ('singleSelectDropdown' in cell.columnType || 'reference' in cell.columnType) {
    console.debug(JSON.stringify(cell));

    let selectNode: HTMLSelectElement = document.createElement('select');
    selectNode.insertAdjacentHTML('beforeend', '<option value="">— NULL —</option>');

    // Retrieve dropdown values from database to populate dropdown
    const onReceiveDropdownValue = new Channel<DropdownValue>();
    onReceiveDropdownValue.onmessage = (dropdownValue) => {
      // Create option node in dropdown list
      let optionNode: HTMLOptionElement = document.createElement('option');
      optionNode.value = dropdownValue.trueValue ?? '';
      optionNode.innerText = dropdownValue.displayValue ?? '';
      selectNode.insertAdjacentElement('beforeend', optionNode);
    };
    await queryAsync({
      invokeAction: 'get_table_column_dropdown_values',
      invokeParams: {
        columnOid: columnOid,
        dropdownValueChannel: onReceiveDropdownValue
      }
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while retrieving dropdown values from database.',
        kind: 'error'
      });
    });

    // Add event listener for when the value is changed
    selectNode.addEventListener('change', async (_) => {
      const newPrimitiveValue = selectNode.value;

      await executeAsync({
        updateTableCellStoredAsPrimitiveValue: {
          tableOid: tableOid,
          rowOid: rowOid,
          columnOid: columnOid,
          value: newPrimitiveValue == '' ? null : newPrimitiveValue
        }
      })
      .catch(async e => {
        await message(e, {
          title: "Unable to update value.",
          kind: 'warning'
        });
      });
    });

    // Add the select node to the cell
    tableCellNode.insertAdjacentElement('beforeend', selectNode);

    // Set the value of the dropdown
    selectNode.value = cell.trueValue ?? '';
    return selectNode;
  } else if ('multiSelectDropdown' in cell.columnType) {
    let selectNode: HTMLSelectElement = document.createElement('select');
    selectNode.multiple = true;
    selectNode.insertAdjacentHTML('beforeend', '<option value="">— NULL —</option>');

    // Retrieve dropdown values from database to populate dropdown
    const onReceiveDropdownValue = new Channel<DropdownValue>();
    onReceiveDropdownValue.onmessage = (dropdownValue) => {
      // Create option node in dropdown list
      let optionNode: HTMLOptionElement = document.createElement('option');
      optionNode.value = dropdownValue.trueValue ?? '';
      optionNode.innerText = dropdownValue.displayValue ?? '';
      selectNode.insertAdjacentElement('beforeend', optionNode);
    };
    await queryAsync({
      invokeAction: 'get_table_column_dropdown_values',
      invokeParams: {
        columnOid: columnOid,
        dropdownValueChannel: onReceiveDropdownValue
      }
    })
    .catch(async (e) => {
      await message(e, {
        title: 'An error occurred while retrieving dropdown values from database.',
        kind: 'error'
      });
    });

    // Add event listener for when the value is changed
    selectNode.addEventListener('change', async (_) => {
      // TODO
    });

    // Add the select node to the cell
    tableCellNode.insertAdjacentElement('beforeend', selectNode);

    // Set the value of the dropdown
    selectNode.value = cell.trueValue ?? '';
    return selectNode;
  } else if ('childObject' in cell.columnType) {
    const objectTableOid: number = cell.columnType.childObject;
    const objectRowOid: number | null = cell.trueValue ? parseInt(cell.trueValue) : null;

    // Show primary key of object, cut off by ellipsis if too long
    tableCellNode.innerText = cell.displayValue ?? '';
    tableCellNode.classList.add('clickable-text');
    tableCellNode.tabIndex = 0;

    // Add event listener to open the object when double-clicked
    function openObject() {
      if (objectRowOid) {
        // Open existing object
        openDialogAsync({
          invokeAction: 'dialog_object_data',
          invokeParams: {
            tableOid: objectTableOid,
            rowOid: objectRowOid,
            title: cell.columnName
          }
        });
      } else {
        // Create new object
        executeAsync({
          setTableObjectCell: {
            tableOid: tableOid,
            columnOid: columnOid,
            rowOid: rowOid,
            objTypeOid: null,
            objRowOid: null
          }
        });
      }
    }
    tableCellNode.addEventListener('dblclick', openObject);
    tableCellNode.addEventListener('click', (e) => {
      e.preventDefault();
      e.returnValue = false;

      if (tableCellNode == document.activeElement) {
        openObject();
      } else {
        tableCellNode.focus();
      }
    });
    return tableCellNode;
  } else {
    const childTableOid: number = cell.columnType.childTable;

    // Show primary key of child table rows, cut off by ellipsis if too long
    tableCellNode.innerText = cell.displayValue ?? '';
    tableCellNode.classList.add('clickable-text');
    tableCellNode.tabIndex = 0;
    
    // Add event listener to open the table when double-clicked
    function openChildTable() {
      openDialogAsync({
        invokeAction: 'dialog_child_table_data',
        invokeParams: {
          tableOid: childTableOid,
          parentRowOid: rowOid,
          tableName: cell.columnName
        }
      });
    }
    tableCellNode.addEventListener('dblclick', openChildTable);
    tableCellNode.addEventListener('click', (e) => {
      e.preventDefault();
      e.returnValue = false;

      if (tableCellNode == document.activeElement) {
        openChildTable();
      } else {
        tableCellNode.focus();
      }
    })
    return tableCellNode;
  }
}