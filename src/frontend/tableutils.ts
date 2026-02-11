import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { message } from "@tauri-apps/plugin-dialog";
import { DropdownValue, TableColumnCell, executeAsync, openDialogAsync, queryAsync } from './backendutils';
import { Channel } from "@tauri-apps/api/core";


let lastActiveElement: HTMLElement | null = null;


export async function attachColumnContextMenu(tableHeaderNode: HTMLElement, tableOid: number, columnOid: number, columnOrdering: number) {
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

export async function updateTableColumnCell(node: HTMLTableCellElement, cell: TableColumnCell, isTable: boolean = true) {
  const tableOid = cell.tableOid;
  const rowOid = cell.rowOid;
  const columnOid = cell.columnOid;

  // Clear the contents of the node
  node.innerHTML = '';

  // Add null class for CSS
  if (!cell.displayValue) {
    node.classList.add('cell-null');
  } else {
    node.classList.remove('cell-null');
  }

  // Add validation errors
  if (cell.failedValidations.length > 0) {
    node.classList.add('cell-error');

    let failureMsgTooltipNode = document.createElement('div');
    failureMsgTooltipNode.classList.add('cell-error-tooltip');
    failureMsgTooltipNode.innerText = cell.failedValidations.map((failure) => failure.description).join('\n');
    node.appendChild(failureMsgTooltipNode);
  } else {
    node.classList.remove('cell-error');
  }

  // Add listener to pull up context menu
  node.addEventListener('contextmenu', async (e) => {
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
  let inputNode: HTMLElement;
  if ('primitive' in cell.columnType) {
    switch (cell.columnType.primitive) {
      case 'Text':
      case 'JSON':
      case 'Number':
      case 'Integer':
      case 'Date':
      case 'Timestamp': {
        const primitiveType = cell.columnType.primitive;
        async function setCellPrimitiveValueAsync(newPrimitiveValue: string | null) {
          // If necessary, convert value into a regularized format before uploading to database
          if (newPrimitiveValue) {
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
        }

        if (isTable) {
          // Create an editable div
          inputNode = node;
          inputNode.contentEditable = 'true';
          if (cell.displayValue) {
            inputNode.innerText = cell.displayValue;
          } else {
            inputNode.setAttribute('placeholder', '— NULL —');
          }

          // Set up an event listener for when the value is changed
          inputNode.addEventListener('focusout', async () => {
            let newPrimitiveValue: string | null = inputNode.innerText.trimEnd();
            await setCellPrimitiveValueAsync(newPrimitiveValue);
          });
          
          // Add the div to the cell
          node.insertAdjacentElement('beforeend', inputNode);
          return inputNode;
        } else {
          // Create a text input
          let cellInput: HTMLInputElement = document.createElement('input');
          cellInput.classList.add('input');
          cellInput.inputMode = 'text';
          cellInput.value = cell.displayValue ?? '';
          cellInput.placeholder = '— NULL —';

          // Set up an event listener for when the value is changed
          cellInput.addEventListener('change', async () => {
            let newPrimitiveValue: string | null = cellInput.value.trimEnd();
            await setCellPrimitiveValueAsync(newPrimitiveValue);
          });

          // Add the input to the cell
          node.insertAdjacentElement('beforeend', cellInput);
          return cellInput;
        }
      }
      case 'Boolean': {
        let inputNode: HTMLInputElement = document.createElement('input');
        inputNode.type = 'checkbox';
        inputNode.checked = cell.displayValue == '1';
        node.insertAdjacentElement('beforeend', inputNode);
        return inputNode;
      }
      case 'File': {
        // Show primary key of object, cut off by ellipsis if too long
        node.innerText = cell.displayValue ?? '';
        node.tabIndex = 0;
        return node;
      }
      case 'Image': {
        // TODO display image thumbnail
        node.innerText = cell.displayValue ?? '';
        node.tabIndex = 0;
        return node;
      }
      default:
        return node;
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
    node.insertAdjacentElement('beforeend', selectNode);

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
    node.insertAdjacentElement('beforeend', selectNode);

    // Set the value of the dropdown
    selectNode.value = cell.trueValue ?? '';
    return selectNode;
  } else if ('childObject' in cell.columnType) {
    const objectTableOid: number = cell.columnType.childObject;
    const objectRowOid: number | null = cell.trueValue ? parseInt(cell.trueValue) : null;

    // Show primary key of object, cut off by ellipsis if too long
    node.innerText = cell.displayValue ?? '';
    node.setAttribute('placeholder', '— NULL —');
    node.classList.add('clickable-text');
    node.tabIndex = 0;

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
    node.addEventListener('click', (e) => {
      e.preventDefault();
      e.returnValue = false;

      if (node == lastActiveElement) {
        openObject();
      } else {
        lastActiveElement = node;
      }
    });
    return node;
  } else {
    const childTableOid: number = cell.columnType.childTable;

    // Show primary key of child table rows, cut off by ellipsis if too long
    node.innerText = cell.displayValue ?? '';
    node.setAttribute('placeholder', '— NULL —');
    node.classList.add('clickable-text');
    node.tabIndex = 0;
    
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
    node.addEventListener('click', (e) => {
      e.preventDefault();
      e.returnValue = false;

      if (node == lastActiveElement) {
        openChildTable();
      } else {
        lastActiveElement = node;
      }
    })
    return node;
  }
}


// Add initial listeners
window.addEventListener("DOMContentLoaded", async () => {
  document.addEventListener('focusout', (e) => {
    lastActiveElement = e.target as HTMLElement;
  })
});