import { listen } from "@tauri-apps/api/event";
import { openDialogAsync } from "./util/dialog";
import { HierarchicalListItemMetadata, queryAsync } from "./util/query";
import { FullMetadata as TableFullMetadata } from "./util/table";
import { FullMetadata as ReportFullMetadata } from "./util/report";
import { Channel } from "@tauri-apps/api/core";
import { executeAsync } from "./util/action";
import { message } from "@tauri-apps/plugin-dialog";

function loadTables() {
    // Disable the open/edit/delete buttons
    const openButton: HTMLButtonElement = document.getElementById('open-table-button') as HTMLButtonElement;
    openButton.disabled = true;
    const editButton: HTMLButtonElement = document.getElementById('edit-table-button') as HTMLButtonElement;
    editButton.disabled = true;
    const deleteButton: HTMLButtonElement = document.getElementById('delete-table-button') as HTMLButtonElement;
    deleteButton.disabled = true;

    // Clear the list of tables
    let tablesList: HTMLDivElement = document.querySelector('#tables-container .list') as HTMLDivElement;
    tablesList.innerHTML = '<div class="empty-list-item">Click "New" to Define a New Table</div>';

    // Query the list of tables
    queryAsync({
        tables: {
            channel: new Channel<HierarchicalListItemMetadata>((table) => {
                const tableId: string = `table${table.oid}-master${table.masterOid}`;
                console.debug(JSON.stringify(table));
                
                const tableElem: HTMLLabelElement = document.createElement('label');
                tableElem.classList.add('list-item');
                tableElem.innerText = `${'  '.repeat(table.level)}${table.name}`;
                tableElem.insertAdjacentHTML('afterbegin', `<input type="radio" name="tables" id="${tableId}" value="${table.oid}:${table.masterOid}">`);
                tableElem.htmlFor = tableId;

                tableElem.addEventListener('input', () => {
                    openButton.disabled = false;
                    editButton.disabled = false;
                    deleteButton.disabled = false;
                });

                tablesList.appendChild(tableElem);
            })
        }
    });
}

function getSelectedTableOid(): number | null {
    const selectedTableOption: HTMLOptionElement | null = document.querySelector('[name=tables]:checked') as HTMLOptionElement;
    if (selectedTableOption) {
        const selectedTableOid: number = parseInt(selectedTableOption.value.split(':')[0]);
        if (!isNaN(selectedTableOid)) {
            return selectedTableOid;
        }
    }
    return null;
}

function loadReports() {
    // Disable the open/edit/delete buttons
    const openButton: HTMLButtonElement = document.getElementById('open-report-button') as HTMLButtonElement;
    openButton.disabled = true;
    const editButton: HTMLButtonElement = document.getElementById('edit-report-button') as HTMLButtonElement;
    editButton.disabled = true;
    const deleteButton: HTMLButtonElement = document.getElementById('delete-report-button') as HTMLButtonElement;
    deleteButton.disabled = true;

    // Clear the list of reports
    let reportsList: HTMLDivElement = document.querySelector('#reports-container .list') as HTMLDivElement;
    reportsList.innerHTML = '<div class="empty-list-item">Click "New" to Define a New Report</div>';

    // Query the list of reports
    queryAsync({
        reports: {
            channel: new Channel<HierarchicalListItemMetadata>((report) => {
                const reportId: string = `report${report.oid}-master${report.masterOid}`;
        
                const reportElem: HTMLLabelElement = document.createElement('label');
                reportElem.classList.add('list-item');
                reportElem.innerText = `${'  '.repeat(report.level)}${report.name}`;
                reportElem.insertAdjacentHTML('afterbegin', `<input type="radio" name="reports" id="${reportId}" value="${report.oid}:${report.masterOid}">`);
                reportElem.htmlFor = reportId;

                reportElem.addEventListener('input', () => {
                    openButton.disabled = false;
                    editButton.disabled = false;
                    deleteButton.disabled = false;
                });

                reportsList.appendChild(reportElem);
            })
        }
    });
}

function getSelectedReportOid(): number | null {
    const selectedReportOption: HTMLOptionElement | null = document.querySelector('[name=reports]:checked') as HTMLOptionElement;
    if (selectedReportOption) {
        const selectedReportOid: number = parseInt(selectedReportOption.value.split(':')[0]);
        if (!isNaN(selectedReportOid)) {
            return selectedReportOid;
        }
    }
    return null;
}



window.addEventListener("DOMContentLoaded", () => {
    // Add button listeners
    document.getElementById('new-table-button')?.addEventListener('click', async (_) => {
        await openDialogAsync({
            createTable: null
        });
    });
    document.getElementById('open-table-button')?.addEventListener('click', async (_) => {
        const selectedTableOption: HTMLOptionElement | null = document.querySelector('[name=tables]:checked') as HTMLOptionElement;
        if (selectedTableOption) {
            const selectedTableOid: number = parseInt(selectedTableOption.value.split(':')[0]);
            const selectedTableName: string = selectedTableOption.innerText.trim();
            if (!isNaN(selectedTableOid)) {
                await openDialogAsync({
                    schema: {
                        title: selectedTableName,
                        queryString: `schema_oid=${selectedTableOid}`
                    }
                });
            }
        }
    });
    document.getElementById('edit-table-button')?.addEventListener('click', async (_) => {
        const selectedTableOid: number | null = getSelectedTableOid();
        if (selectedTableOid) {
            await openDialogAsync({
                editTable: {
                    tableOid: selectedTableOid
                }
            });
        }
    });
    document.getElementById('delete-table-button')?.addEventListener('click', async (_) => {
        const selectedTableOid: number | null = getSelectedTableOid();
        if (selectedTableOid) {
            await executeAsync({
                trashSchema: selectedTableOid
            })
            .catch(async (e) => {
                await message(e, {
                    title: 'An error occurred while deleting table.',
                    kind: 'error'
                });
            });
        }
    });
    document.getElementById('new-report-button')?.addEventListener('click', async (_) => {
        await openDialogAsync({
            createReport: null
        });
    });
    document.getElementById('open-report-button')?.addEventListener('click', async (_) => {
        const selectedReportOption: HTMLOptionElement | null = document.querySelector('[name=reports]:checked') as HTMLOptionElement;
        if (selectedReportOption) {
            const selectedReportOid: number = parseInt(selectedReportOption.value.split(':')[0]);
            const selectedReportName: string = selectedReportOption.innerText.trim();
            if (!isNaN(selectedReportOid)) {
                await openDialogAsync({
                    schema: {
                        title: selectedReportName,
                        queryString: `schema_oid=${selectedReportOid}`
                    }
                });
            }
        }
    });
    document.getElementById('edit-report-button')?.addEventListener('click', async (_) => {
        const selectedReportOid: number | null = getSelectedReportOid();
        if (selectedReportOid) {
            await openDialogAsync({
                editReport: {
                    reportOid: selectedReportOid
                }
            });
        }
    });
    document.getElementById('delete-report-button')?.addEventListener('click', async (_) => {
        const selectedSchemaOid: number | null = getSelectedReportOid();
        if (selectedSchemaOid) {
            await executeAsync({
                trashSchema: selectedSchemaOid
            })
            .catch(async (e) => {
                await message(e, {
                    title: 'An error occurred while deleting report.',
                    kind: 'error'
                });
            });
        }
    });

    // Load in the tables and reports
    loadTables();
    loadReports();
});


listen<number>('schema', () => {
    loadTables();
    loadReports();
});
listen<number>('table', loadTables);
listen<number>('report', loadReports);