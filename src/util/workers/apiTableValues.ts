import { fetch } from "@tauri-apps/plugin-http"

onmessage = async function (event) {
    const response = await fetch(`/api/table/values?table_oid=${event.data.tableOid}`);
    const dropdownValues = await response.json();
    for (const dropdownValue of dropdownValues) {
        postMessage(dropdownValue);
    }
}