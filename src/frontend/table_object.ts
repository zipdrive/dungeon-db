import { Channel } from "@tauri-apps/api/core";
import { BasicHierarchicalMetadata, closeDialogAsync, executeAsync, queryAsync, TableRowCellChannelPacket } from "./backendutils";
import { attachColumnContextMenu, updateTableColumnCell } from "./tableutils";
import { listen } from "@tauri-apps/api/event";
import { makeColumnsResizable } from "./frontendutils";
import { message } from "@tauri-apps/plugin-dialog";

const urlParams = new URLSearchParams(window.location.search);
const urlParamTableOid = urlParams.get('table_oid');
const urlParamObjOid = urlParams.get('obj_oid');

if (urlParamTableOid && urlParamObjOid) {
  const tableOid: number = parseInt(urlParamTableOid);
  const objOid: number = parseInt(urlParamObjOid);

  async function refreshObjectAsync() {
    // Record the old scroll position
    let pageNode: HTMLDivElement = document.getElementById('page') as HTMLDivElement;
    const scrollPosition: number = pageNode.scrollTop;
    
    // Strip the former contents of the table
    let tableNode: HTMLTableElement | null = document.querySelector('#object-content');
    if (tableNode)
      tableNode.innerHTML = '<colgroup><col span="1" class="field-name-cell"><col span="1" class="field-input-cell"></colgroup><tbody></tbody>';
    let tableBodyNode: HTMLElement | null = document.querySelector('#object-content > tbody');

    // Set up a dropdown for the object type, if there are possible subtypes
    let subtypeDropdownRow: HTMLTableRowElement = document.createElement('tr');
    subtypeDropdownRow.innerHTML = '<td class="resizable-column"><label for="object-content-subtype-dropdown">Object Type:</label></td><td><select id="object-content-subtype-dropdown"></select></td>';
    let subtypeDropdown: HTMLSelectElement = subtypeDropdownRow.querySelector('#object-content-subtype-dropdown') as HTMLSelectElement;
    subtypeDropdown.classList.add('input');

    // Set up a channel to receive all possible subtypes
    const onReceiveSubtype: Channel<BasicHierarchicalMetadata> = new Channel<BasicHierarchicalMetadata>();
    onReceiveSubtype.onmessage = (subtype) => {
      console.debug(`Subtype received: ${JSON.stringify(subtype)}`);
      let subtypeOption: HTMLOptionElement = document.createElement('option');
      subtypeOption.value = subtype.oid.toString();
      subtypeOption.innerText = (subtype.hierarchyLevel > 0 ? ' '.repeat(subtype.hierarchyLevel) : '') + subtype.name;
      subtypeDropdown.appendChild(subtypeOption);
    };

    // Send the query to receive all possible subtypes
    await queryAsync({
      invokeAction: 'get_subtype_list',
      invokeParams: {
        tableOid: tableOid,
        objectTypeChannel: onReceiveSubtype
      }
    });

    // Only add the subtype dropdown if there is at least one subtype
    if (subtypeDropdown.childElementCount > 1) {
      tableBodyNode?.appendChild(subtypeDropdownRow);
      tableBodyNode?.insertAdjacentHTML('beforeend', '<tr><td colspan="2"><hr><hr></td></tr>')

      // Change the object's subtype and refresh when the dropdown value is changed
      subtypeDropdown.addEventListener('change', async (_) => {
        await executeAsync({
          retypeTableRow: {
            baseTypeOid: tableOid,
            baseRowOid: objOid,
            newSubtypeOid: parseInt(subtypeDropdown.value)
          }
        })
        .catch(async (e) => {
          await message(e, {
            title: 'An error occurred while changing the type of the object.',
            kind: 'error'
          });
        });
      });
    }

    // Set up a channel to create an input for each cell
    const onReceiveCell: Channel<TableRowCellChannelPacket> = new Channel<TableRowCellChannelPacket>();
    onReceiveCell.onmessage = (cell) => {
      if ('rowExists' in cell) {
        if (cell.rowExists) {
          subtypeDropdown.value = cell.tableOid.toString();
        } else {
          closeDialogAsync();
        }
      } else {
        let cellRow: HTMLTableRowElement = document.createElement('tr');
        let cellLabelContainer: HTMLTableCellElement = document.createElement('td');
        cellLabelContainer.classList.add('field-name-cell');
        cellLabelContainer.classList.add('resizable-column');
        attachColumnContextMenu(cellLabelContainer, tableOid, cell.columnOid, cell.columnOrdering);
        cellRow.appendChild(cellLabelContainer);

        // Create a label for the cell
        let columnLabel: HTMLLabelElement = document.createElement('label');
        columnLabel.htmlFor = `table-content-column${cell.columnOid}`;
        columnLabel.innerText = `${cell.columnName}:`;
        cellLabelContainer.appendChild(columnLabel);

        // Create an input for the cell
        let columnValueCell: HTMLTableCellElement = document.createElement('td');
        columnValueCell.id = `table-content-column${cell.columnOid}-row${cell.rowOid}`;
        columnValueCell.classList.add('field-value-cell');
        columnValueCell.dataset.tableOid = cell.tableOid.toString();
        columnValueCell.dataset.columnOid = cell.columnOid.toString();
        columnValueCell.dataset.rowOid = cell.rowOid.toString();
        cellRow.appendChild(columnValueCell);

        updateTableColumnCell(columnValueCell, cell, false);

        // Add the row to the table
        tableBodyNode?.appendChild(cellRow);
      }
    };

    await queryAsync({
      invokeAction: 'get_object_data',
      invokeParams: {
        objTypeOid: tableOid,
        objRowOid: objOid,
        objDataChannel: onReceiveCell
      }
    });

    // Allow field name column to be resized
    makeColumnsResizable(
      (_, newColumnWidth) => {
        let fieldNameWidthStylesheet: HTMLStyleElement = document.getElementById('field-name-width-stylesheet') as HTMLStyleElement;
        fieldNameWidthStylesheet.innerHTML = `.field-name-cell { width: ${newColumnWidth}px; }`;
      },
      () => {}
    );

    // Set the scroll position back to what it was before
    pageNode.scrollTop = scrollPosition;
  }

  // Add initial listeners
  window.addEventListener("DOMContentLoaded", async () => {
    navigator.locks.request('object-content', refreshObjectAsync);
  });
  
  listen<number>("update-table-data-deep", (e) => {
    const updateTableOid = e.payload;
    if (updateTableOid == tableOid) {
      navigator.locks.request('object-content', refreshObjectAsync);
    }
  });
  listen<number>("update-table-data-shallow", (e) => {
    const updateTableOid = e.payload;
    if (updateTableOid == tableOid) {
      navigator.locks.request('object-content', refreshObjectAsync);
    }
  });
  listen<[number, number]>("update-table-row", (e) => {
    const updateTableOid = e.payload[0];
    const updateRowOid = e.payload[1];
    if (updateTableOid == tableOid && updateRowOid == objOid) {
      navigator.locks.request('object-content', refreshObjectAsync);
    }
  });
}