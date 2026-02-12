import { Menu, MenuItem } from "@tauri-apps/api/menu";
import { message, save, open } from "@tauri-apps/plugin-dialog";
import { DropdownValue, TableColumnCell, executeAsync, openDialogAsync, queryAsync } from './backendutils';
import { Channel } from "@tauri-apps/api/core";
import { fileTypeFromBlob, fileTypeFromBuffer, FileTypeResult } from "file-type";


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

  // Clear the contents and event listeners of the node
  let clonedNode: HTMLTableCellElement = node.cloneNode() as HTMLTableCellElement;
  clonedNode.innerHTML = '';
  node.replaceWith(clonedNode);
  node = clonedNode;

  // Add null class for CSS
  if (!cell.displayValue) {
    node.classList.add('cell-null');
  } else {
    node.classList.remove('cell-null');
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
          inputNode = cellInput;
        }
        break;
      }
      case 'Boolean': {
        node.classList.add('clickable-text');

        if (cell.displayValue) {
          // Add a checkbox
          let checkboxNode: HTMLInputElement = document.createElement('input');
          checkboxNode.type = 'checkbox';
          checkboxNode.checked = cell.displayValue == '1';
          node.appendChild(checkboxNode);

          node.addEventListener('click', (_) => {
            checkboxNode.checked = !checkboxNode.checked;
            checkboxNode.dispatchEvent(new Event('input'));
          });
          checkboxNode.addEventListener('click', (e) => {
            // Prevent the checkbox from getting triggered twice in a row
            e.stopPropagation();
          });

          // Add event listener to change the value in the database
          checkboxNode.addEventListener('input', async (_) => {
            await executeAsync({
              updateTableCellStoredAsPrimitiveValue: {
                tableOid: tableOid,
                rowOid: rowOid,
                columnOid: columnOid,
                value: checkboxNode.checked ? '1' : '0'
              }
            })
            .catch(async e => {
              await message(e, {
                title: "An error occurred while updating value.",
                kind: 'error'
              });
            });
          });
        } else {
          // Null placeholder, set TRUE on click
          node.setAttribute('placeholder', '— NULL —');
          node.addEventListener('click', async (_) => {
            await executeAsync({
              updateTableCellStoredAsPrimitiveValue: {
                tableOid: tableOid,
                rowOid: rowOid,
                columnOid: columnOid,
                value: '1'
              }
            })
            .catch(async e => {
              await message(e, {
                title: "An error occurred while updating value.",
                kind: 'error'
              });
            });
          });
        }

        inputNode = node;
        break;
      }
      case 'File': {
        // Show primary key of object, cut off by ellipsis if too long
        inputNode = node;
        inputNode.style.display = 'grid';
        inputNode.style.height = '100%';
        inputNode.style.gridTemplateColumns = '1fr auto auto';

        /**
         * Uploads a file to the cell from the local filesystem.
         */
        async function uploadFileAsync() {
          const filePath = await open({
            title: 'Upload File to DungeonDB'
          });
          if (filePath) {
            await executeAsync({
              updateTableCellStoredAsBlob: {
                tableOid: tableOid,
                columnOid: columnOid,
                rowOid: rowOid,
                filePath: filePath
              }
            })
            .catch(async (e) => {
              await message(e, {
                title: 'An error occurred while uploading file.',
                kind: 'error'
              });
            });
          }
        }

        /**
         * Downloads the file from the cell to the local filesystem.
         */
        async function downloadFileAsync() {
          const filePath = await save({
            title: 'Download File from DungeonDB'
          });
          if (filePath) {
            await queryAsync({
              invokeAction: 'download_blob_value',
              invokeParams: {
                tableOid: tableOid,
                rowOid: rowOid,
                columnOid: columnOid,
                filePath: filePath
              }
            })
            .catch(async (e) => {
              await message(e, {
                title: 'An error occurred while downloading file.',
                kind: 'error'
              });
            });
          }
        }

        if (cell.displayValue) {
          // Display the size of the file
          let fileDescNode: HTMLSpanElement = document.createElement('span');
          fileDescNode.innerText = cell.displayValue;
          inputNode.appendChild(fileDescNode);

          // Button for uploading a file
          let fileUploadNode: HTMLImageElement = document.createElement('img');
          fileUploadNode.classList.add('clickable-text');
          fileUploadNode.alt = 'Upload';
          fileUploadNode.src = '/src-tauri/icons/upload.png';
          fileUploadNode.addEventListener('click', uploadFileAsync);
          fileUploadNode.tabIndex = 0;
          inputNode.appendChild(fileUploadNode);

          // Button for downloading the file that's in the database
          let fileDownloadNode: HTMLImageElement = document.createElement('img');
          fileDownloadNode.classList.add('clickable-text');
          fileDownloadNode.alt = 'Download';
          fileDownloadNode.src = '/src-tauri/icons/download.png';
          fileDownloadNode.addEventListener('click', downloadFileAsync);
          fileDownloadNode.tabIndex = 0;
          inputNode.appendChild(fileDownloadNode);
        } else {
          // Set a placeholder
          inputNode.classList.add('clickable-text');
          inputNode.setAttribute('placeholder', '— NULL —');

          // On click, upload file
          inputNode.addEventListener('click', uploadFileAsync);
        }

        break;
      }
      case 'Image': {
        /**
         * Uploads a file to the cell from the local filesystem.
         */
        async function uploadImageAsync() {
          const filePath = await open({
            title: 'Upload Image to DungeonDB'
          });
          if (filePath) {
            await executeAsync({
              updateTableCellStoredAsBlob: {
                tableOid: tableOid,
                columnOid: columnOid,
                rowOid: rowOid,
                filePath: filePath
              }
            })
            .catch(async (e) => {
              await message(e, {
                title: 'An error occurred while uploading image.',
                kind: 'error'
              });
            });
          }
        }

        inputNode = node;
        inputNode.tabIndex = 0;
        if (cell.displayValue) {
          // Acquire the image buffer from the database in base64
          const imgBase64: string = '';
          const imgBinary: string = atob(imgBase64);
          const encoder = new TextEncoder();
          const mimeType: FileTypeResult | undefined = await fileTypeFromBuffer(encoder.encode(imgBinary).buffer);
          if (!mimeType || !mimeType.mime.startsWith('image/')) {
            // Display a warning icon instead of a thumbnail
            inputNode.innerText = '⚠';

            // Add an error message
            cell.failedValidations.push({
              description: 'Unable to detect image type.'
            });
          } else {
            // Display image thumbnail
            let thumbnailNode: HTMLImageElement = document.createElement('img');
            thumbnailNode.src = `data:${mimeType.mime};base64 ${imgBase64}`;
            inputNode.appendChild(thumbnailNode);
          }
        } else {
          // Set a placeholder
          inputNode.classList.add('clickable-text');
          inputNode.setAttribute('placeholder', '— NULL —');

          // On click, upload image
          inputNode.addEventListener('click', uploadImageAsync);
        }
        break;
      }
      default:
        inputNode = node;
    }
  } else if ('singleSelectDropdown' in cell.columnType || 'reference' in cell.columnType) {
    /**
     * SINGLE-SELECT DROPDOWN CELL
     * REFERENCE TO OTHER TABLE
     */

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
    inputNode = selectNode;
  } else if ('multiSelectDropdown' in cell.columnType) {
    /**
     * MULTI-SELECT DROPDOWN CELL
     */

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
    inputNode = selectNode;
  } else if ('childObject' in cell.columnType) {
    /**
     * OBJECT
     */

    const objectTableOid: number = cell.columnType.childObject;
    const objectRowOid: number | null = cell.trueValue ? parseInt(cell.trueValue) : null;

    // Show primary key of object, cut off by ellipsis if too long
    inputNode = node;
    inputNode.innerText = cell.displayValue ?? '';
    inputNode.setAttribute('placeholder', '— NULL —');
    inputNode.classList.add('clickable-text');
    inputNode.tabIndex = 0;

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

    inputNode.addEventListener('click', (_) => {
      openObject();
    });
  } else {
    /**
     * CHILD TABLE
     */

    const childTableOid: number = cell.columnType.childTable;

    // Show primary key of child table rows, cut off by ellipsis if too long
    inputNode = node;
    inputNode.innerText = cell.displayValue ?? '';
    inputNode.setAttribute('placeholder', '— NULL —');
    inputNode.classList.add('clickable-text');
    inputNode.tabIndex = 0;
    
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

    inputNode.addEventListener('click', (_) => {
      openChildTable();
    });
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

  inputNode.classList.add('focusable');
  return inputNode;
}
