import { message } from "@tauri-apps/plugin-dialog";
import { closeDialogAsync } from "../util/dialog";
import { DatasourceDropdownValue, DropdownValue, getReportMetadataAsync, getTableMetadataAsync, ParameterDropdownValue, queryAsync, ToggledHierarchicalListItemMetadata } from "../util/query";
import { FullMetadata as SchemaFullMetadata } from "../util/schema";
import { FullMetadata as TableFullMetadata } from "../util/table";
import { FullMetadata as ReportFullMetadata } from "../util/report";
import { executeAsync } from "../util/action";
import { emit, listen } from "@tauri-apps/api/event";
import { Channel } from "@tauri-apps/api/core";
import { Datasource } from "../util/datasource";
import drop from "@interactjs/actions/drop/plugin";

const urlParams = new URLSearchParams(window.location.search);
const urlParamId: string | null = urlParams.get('id');
const paramId: number = urlParamId ? parseInt(urlParamId) : 0;
const urlParamSchemaOid: string | null = urlParams.get('schema_oid');
const schemaOid: number | null = urlParamSchemaOid ? parseInt(urlParamSchemaOid) : null;

async function setDatasourceOptions(selectElem: HTMLSelectElement, parentDatasource: Datasource) {
    selectElem.innerHTML = '<option value="">...</option>';
    await queryAsync({
        linkedDatasources: {
            parentDatasource: parentDatasource,
            channel: new Channel<DatasourceDropdownValue>((dropdownValue) => {
                const optionElem: HTMLOptionElement = document.createElement('option');
                optionElem.value = JSON.stringify(dropdownValue.value);
                optionElem.innerText = dropdownValue.label;
                selectElem.appendChild(optionElem);
            })
        }
    });
}

async function setDatasourceSelectListener(index: number, selectElem: HTMLSelectElement) {
    // Whenever a datasource is selected, change what parameters can be selected
    selectElem.addEventListener('input', async () => {
        document.querySelectorAll(`#param-path > tbody > tr:has(#param-link${index}) ~ tr`).forEach((elem) => elem.remove());

        if (selectElem.value) {
            const datasource: Datasource = JSON.parse(selectElem.value);
            console.debug(`Datasource: ${JSON.stringify(datasource)}`);

            const nextRow: HTMLTableRowElement = await constructLinkedDatasourceSelect(index + 1, datasource);
            document.querySelector('#param-path > tbody')?.appendChild(nextRow);

            setParameters(datasource);
        } else {
            setParameters(JSON.parse((document.getElementById(`param-link${index - 1}`) as HTMLSelectElement).value));
        }
    });
}

async function constructLinkedDatasourceSelect(index: number, parentDatasource: Datasource): Promise<HTMLTableRowElement> {
    // Query for datasources
    const selectElem: HTMLSelectElement = document.createElement('select');
    selectElem.id = `param-link${index}`;
    selectElem.classList.add('input');
    await setDatasourceOptions(selectElem, parentDatasource);
    setDatasourceSelectListener(index, selectElem);

    // Construct the table row
    const row: HTMLTableRowElement = document.createElement('tr');
    row.innerHTML = '<td></td>';
    const cell: HTMLTableCellElement = document.createElement('td');
    cell.appendChild(selectElem);
    row.appendChild(cell);
    return row;
}

function setParameters(datasource: Datasource) {
    const paramSelectElem: HTMLSelectElement = document.getElementById('param-column') as HTMLSelectElement;
    paramSelectElem.innerHTML = '';
    queryAsync({
        parameters: {
            parentDatasource: datasource,
            channel: new Channel<ParameterDropdownValue>((dropdownValue) => {
                const optionElem: HTMLOptionElement = document.createElement('option');
                optionElem.value = dropdownValue.value;
                optionElem.innerText = dropdownValue.label;
                paramSelectElem.appendChild(optionElem);
            })
        }
    });
}


window.addEventListener("DOMContentLoaded", () => {
    // Load the root datasources
    const rootSelectElem: HTMLSelectElement = document.getElementById('param-link0') as HTMLSelectElement;
    const link1SelectElem: HTMLSelectElement = document.getElementById('param-link1') as HTMLSelectElement;

    queryAsync({
        rootDatasources: {
            channel: new Channel<DatasourceDropdownValue>((dropdownValue) => {
                const optionElem: HTMLOptionElement = document.createElement('option');
                optionElem.value = JSON.stringify(dropdownValue.value);
                optionElem.innerText = dropdownValue.label;
                if ('table' in dropdownValue.value && dropdownValue.value.table.tableOid == schemaOid) {
                    optionElem.selected = true;
                }
                rootSelectElem.appendChild(optionElem);
            })
        }
    })
    .then(() => {
        if (rootSelectElem.value) {
            const rootDatasource: Datasource = JSON.parse(rootSelectElem.value);
            setDatasourceOptions(link1SelectElem, rootDatasource);
            setParameters(rootDatasource);
        }
    });

    // When the root datasource is altered, wipe everything that's been entered already
    rootSelectElem.addEventListener('input', () => {
        if (rootSelectElem.value) {
            document.querySelectorAll(`#param-path > tbody > tr:has(#param-link1) ~ tr`).forEach((elem) => elem.remove());

            const rootDatasource: Datasource = JSON.parse(rootSelectElem.value);
            setDatasourceOptions(link1SelectElem, rootDatasource);
            setParameters(rootDatasource);
        }
    });
    setDatasourceSelectListener(1, link1SelectElem);

    document.getElementById('confirm-button')?.addEventListener("click", async (e) => {
        e.preventDefault();

        // Emit signal to add the parameter to the requested location
        const paramSelectElem: HTMLSelectElement = document.getElementById('param-column') as HTMLSelectElement;
        await emit('add-parameter', [paramId, paramSelectElem.value, paramSelectElem.options[paramSelectElem.selectedIndex].innerText]);

        // Close the dialog
        await closeDialogAsync();
    });
    document.getElementById('cancel-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        await closeDialogAsync();
    });
});