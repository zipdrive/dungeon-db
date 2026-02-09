import { Channel, invoke } from "@tauri-apps/api/core";
import { BasicHierarchicalMetadata, queryAsync, TableRowCellChannelPacket } from "./backendutils";
import { addTableColumnCellToRow } from "./tableutils";

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
    let tableBodyNode: HTMLElement | null = document.querySelector('#table-content > tbody');

    // Set up a dropdown for the object type, if there are possible subtypes
    let subtypeDropdownRow: HTMLTableRowElement = document.createElement('tr');
    subtypeDropdownRow.innerHTML = '<td><label for="object-content-subtype-dropdown">Object Type:</label></td><td><select id="object-content-subtype-dropdown"></select></td>';
    let subtypeDropdown: HTMLSelectElement = subtypeDropdownRow.querySelector('#object-content-subtype-dropdown') as HTMLSelectElement;

    // Set up a channel to receive all possible subtypes
    const onReceiveSubtype: Channel<BasicHierarchicalMetadata> = new Channel<BasicHierarchicalMetadata>();
    onReceiveSubtype.onmessage = (subtype) => {
      let subtypeOption: HTMLOptionElement = document.createElement('option');
      subtypeOption.value = subtype.oid.toString();
      subtypeOption.innerText = (subtype.hierarchyLevel > 0 ? '--'.repeat(subtype.hierarchyLevel) + ' ' : '') + subtype.name;
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

      // Change the object's subtype and refresh when the dropdown value is changed
      subtypeDropdown.addEventListener('change', (_) => {
        // TODO
      });
    }

    // Set up a channel to create an input for each cell
    const onReceiveCell: Channel<TableRowCellChannelPacket> = new Channel<TableRowCellChannelPacket>();
    onReceiveCell.onmessage = (cell) => {
      if ('rowExists' in cell) {
        subtypeDropdown.value = cell.tableOid.toString();
      } else {
        let cellRow: HTMLTableRowElement = document.createElement('tr');
        let cellLabelContainer: HTMLTableCellElement = document.createElement('td');
        cellRow.appendChild(cellLabelContainer);

        // Create a label for the cell
        let columnLabel: HTMLLabelElement = document.createElement('label');
        columnLabel.htmlFor = `object-content-column${cell.columnOid}`;
        columnLabel.innerText = `${cell.columnName}:`;
        cellLabelContainer.appendChild(columnLabel);

        // Create an input for the cell
        addTableColumnCellToRow(cellRow, cell);
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

    // Set the scroll position back to what it was before
    pageNode.scrollTop = scrollPosition;
  }

}