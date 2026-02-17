import { Channel } from "@tauri-apps/api/core";
import { ReportVirtualParameter, queryStreamAsync } from "./backendutils";
import { message } from "@tauri-apps/plugin-dialog";

const urlParams = new URLSearchParams(window.location.search);
const urlParamBaseTableOid: string | null = urlParams.get('base_table_oid');
if (urlParamBaseTableOid) {
    const baseTableOid: number = parseInt(urlParamBaseTableOid);

    async function loadVirtualParameters(selectNode: HTMLSelectElement, baseTableOid: number) {
        const onReceiveVirtualParameter = new Channel<ReportVirtualParameter>();
        onReceiveVirtualParameter.onmessage = (vparam) => {
            let optNode: HTMLOptionElement = document.createElement('option');
            optNode.value = JSON.stringify(vparam);
            if ('column' in vparam) {
                // Column in the base table
                optNode.innerText = `${(vparam.column.isManyToOne ? '* ' : '')}${vparam.column.sourceName} / ${vparam.column.columnName}`;
            } else if ('masterList' in vparam) {
                optNode.innerText = `Inheritance from ${vparam.masterList.masterTableName}`;
            } else if ('reference' in vparam) {
                optNode.innerText = `* ${vparam.reference.sourceName} / ${vparam.reference.columnName}`;
            } else {
                optNode.innerText = `* Inherited by ${vparam.inheritance.inheritorTableName}`;
            }
            selectNode.appendChild(optNode);
        };

        await queryStreamAsync([{
            reportParameters: {
                baseTableOid: baseTableOid
            }
        }, onReceiveVirtualParameter])
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while retrieving possible parameters.',
                kind: 'error'
            });
        });

        selectNode.addEventListener('change', async (_) => {
            // Delete the rows for all deeper levels that were dependent on the value of this level
            let rowNode: HTMLElement | null = selectNode.parentElement ?? null;
            while (rowNode && rowNode.nextElementSibling) {
                rowNode.nextElementSibling.remove();
            }

            if (selectNode.value) {
                let vparam: ReportVirtualParameter = JSON.parse(selectNode.value);
                let deeperTableOid: number;
                if ('column' in vparam) {
                    deeperTableOid = vparam.column.linkedTableOid;
                } else if ('masterList' in vparam) {
                    deeperTableOid = vparam.masterList.masterTableOid;
                } else if ('reference' in vparam) {
                    deeperTableOid = vparam.reference.linkedTableOid;
                } else {
                    deeperTableOid = vparam.inheritance.inheritorTableOid;
                }

                // Add new row for the next level
                rowNode?.insertAdjacentHTML('afterend', '<tr><td><select></select></td></tr>');
                await loadVirtualParameters(rowNode?.nextElementSibling?.querySelector('select') as HTMLSelectElement, deeperTableOid);
            }
        });
    }

    // Set up the window
    window.addEventListener("DOMContentLoaded", async () => {
        await loadVirtualParameters(document.querySelector('select') as HTMLSelectElement, baseTableOid);
    });
}