import { message } from "@tauri-apps/plugin-dialog";
import { closeDialogAsync } from "../util/dialog";
import { DropdownValue, getReportMetadataAsync, getTableMetadataAsync, queryAsync, ToggledHierarchicalListItemMetadata } from "../util/query";
import { FullMetadata as SchemaFullMetadata } from "../util/schema";
import { FullMetadata as TableFullMetadata } from "../util/table";
import { FullMetadata as ReportFullMetadata } from "../util/report";
import { executeAsync } from "../util/action";
import { emit, listen } from "@tauri-apps/api/event";
import { Channel } from "@tauri-apps/api/core";
import { Datasource } from "../util/datasource";

const urlParams = new URLSearchParams(window.location.search);
const urlParamSchemaOid: string | null = urlParams.get('schema_oid');
const schemaOid: number | null = urlParamSchemaOid ? parseInt(urlParamSchemaOid) : null;


window.addEventListener("DOMContentLoaded", () => {
    // Load the root datasources
    const rootSelectElem: HTMLSelectElement = document.getElementById('param-root-datasource') as HTMLSelectElement;
    queryAsync({
        rootDatasources: {
            channel: new Channel<[Datasource, string]>(([datasource, label]) => {
                const optionElem: HTMLOptionElement = document.createElement('option');
                optionElem.value = JSON.stringify(datasource);
                optionElem.innerText = label;
                if ('table' in datasource && datasource.table.tableOid == schemaOid) {
                    optionElem.selected = true;
                }
                rootSelectElem.appendChild(optionElem);
            })
        }
    });

    // 
    rootSelectElem.addEventListener('input', () => {
        
    });

    document.getElementById('confirm-button')?.addEventListener("click", async (e) => {
        e.preventDefault();

        
    });
    document.getElementById('cancel-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        await closeDialogAsync();
    });
});