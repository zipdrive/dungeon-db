import { openDialogAsync } from "./util/dialog";
import { HierarchicalListItemMetadata } from "./util/query";

function loadTables() {
    // Clear the list of tables
    let tablesList: HTMLDivElement = document.querySelector('#tables-container .list') as HTMLDivElement;
    tablesList.innerHTML = '<div class="empty-list-item">Click "New" to Define a New Table</div>';

    // Query the list of tables
    let worker: Worker = new Worker('./util/workers/queryTables');
    worker.onmessage = function (event) {
        const table: HierarchicalListItemMetadata = event.data;
        const tableId: string = `table${table.oid}-master${table.masterOid}`;
        
        const tableElem: HTMLLabelElement = document.createElement('label');
        tableElem.classList.add('list-item');
        tableElem.innerText = `${'&nbsp;&nbsp;'.repeat(table.level)}${table.name}`;
        tableElem.insertAdjacentHTML('afterbegin', `<input type="radio" name="tables" id="${tableId}" value="${table.oid}:${table.masterOid}">`);
        tableElem.htmlFor = tableId;

        tablesList.appendChild(tableElem);
    };
    worker.postMessage(null);
}

function loadReports() {
    // Clear the list of tables
    let reportsList: HTMLDivElement = document.querySelector('#reports-container .list') as HTMLDivElement;
    reportsList.innerHTML = '<div class="empty-list-item">Click "New" to Define a New Report</div>';

    // Query the list of tables
    let worker: Worker = new Worker('./util/workers/queryReports');
    worker.onmessage = function (event) {
        const report: HierarchicalListItemMetadata = event.data;
        const reportId: string = `report${report.oid}-master${report.masterOid}`;
        
        const reportElem: HTMLLabelElement = document.createElement('label');
        reportElem.classList.add('list-item');
        reportElem.innerText = `${'&nbsp;&nbsp;'.repeat(report.level)}${report.name}`;
        reportElem.insertAdjacentHTML('afterbegin', `<input type="radio" name="reports" id="${reportId}" value="${report.oid}:${report.masterOid}">`);
        reportElem.htmlFor = reportId;

        reportsList.appendChild(reportElem);
    };
    worker.postMessage(null);
}



window.addEventListener("DOMContentLoaded", () => {
    // Add button listeners
    document.getElementById('new-table-button')?.addEventListener('click', async (_) => {
        await openDialogAsync({
        createTable: null
        });
    });
    document.getElementById('new-report-button')?.addEventListener('click', async (_) => {
        await openDialogAsync({
        createReport: null
        });
    });

    // Load in the tables and reports
    loadTables();
    loadReports();
});