import { listen } from "@tauri-apps/api/event";
import { Primitive } from "./column";
import { openDialogAsync } from "./dialog";
import { executeAsync } from "./action";
import { open, save, message, ask } from "@tauri-apps/plugin-dialog";
import { getFileBase64Async } from "./query";
import { fileTypeFromBuffer, FileTypeResult } from "file-type";

type ValidationFailures = {
    message: string
}[];

type CellOid = {
    schemaOid: number,
    rowOid: number,
    columnOid: number
};

export type File = {
    path: {
        oid: number,
        path: string 
    }
} | {
    blob: {
        oid: number
    }
};

type RowCell = {
    schemaOid: number,
    rowOid: number,
    index: number,
    validationFailures: ValidationFailures
};
type ReadonlyCell = {
    cellOid: CellOid,
    label: string | null,
    validationFailures: ValidationFailures
};
type SubreportCell = {
    cellOid: CellOid,
    label: string,
    schemaQueryString: string,
    validationFailures: ValidationFailures 
};
type PrimitiveEntryCell = {
    cellOid: CellOid,
    valueOid: CellOid,
    label: string | null,
    primitiveType: Primitive,
    validationFailures: ValidationFailures 
};
type FileEntryCell = {
    cellOid: CellOid,
    valueOid: CellOid,
    label: string | null,
    fileOid: number | null,
    validationFailures: ValidationFailures
};
type ObjectCell = {
    cellOid: CellOid,
    valueOid: CellOid,
    label: string | null,
    objectSchemaOid: number,
    objectQueryString: string | null,
    validationFailures: ValidationFailures
};
type SelectEntryCell = {
    cellOid: CellOid,
    valueOid: CellOid,
    selectSchemaOid: number,
    selectRowOid: number | null,
    validationFailures: ValidationFailures
};
type MultiselectEntryCell = {
    cellOid: CellOid,
    valueOid: CellOid,
    label: string | null,
    multiselectSchemaOid: number,
    multiselectRowOid: number[],
    validationFailures: ValidationFailures
};
type AddNewRowButtonCell = {
    tableOid: number,
    columnSpan: number
};

export type Cell = { row: RowCell } 
| { readonly: ReadonlyCell } 
| { subreport: SubreportCell } 
| { primitiveEntry: PrimitiveEntryCell } 
| { fileEntry: FileEntryCell }
| { object: ObjectCell } 
| { selectEntry: SelectEntryCell } 
| { multiselectEntry: MultiselectEntryCell }
| { addNewRowButton: AddNewRowButtonCell };



let columnValueWorkers: {[key: number]: Worker} = {};

function getColumnValueWorker(schemaOid: number) {
    if (!columnValueWorkers[schemaOid]) {
        columnValueWorkers[schemaOid] = new Worker('./workers/queryColumnValues');
    }
    return columnValueWorkers[schemaOid];
}



/**
 * Add a tooltip to an HTML element.
 * @param elem The HTML element.
 * @param tooltip The tooltip to append.
 */
function addTooltip(elem: HTMLElement, tooltip: string) {
    const existingTooltip: string | null = elem.getAttribute('tooltip');
    elem.setAttribute('tooltip', existingTooltip ? `${existingTooltip} ${tooltip}` : tooltip);
}

function addValidationFailureTooltips(elem: HTMLElement, validationFailures: ValidationFailures) {
    if (validationFailures.length > 0) {
        elem.classList.add('cell-error');
        validationFailures.forEach((validationFailure) => addTooltip(elem, validationFailure.message));
    }
}



function updateRowIndexCell(cell: RowCell, elem: HTMLTableCellElement) {
    elem.innerText = `${cell.index}`;
}

function updateReadonlyCell(cell: ReadonlyCell, elem: HTMLTableCellElement) {
    elem.classList.add('cell-readonly');
    if (cell.label == null)
        elem.classList.add('cell-null');

    elem.innerText = cell.label ?? '';
    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updateSubreportCell(cell: SubreportCell, elem: HTMLTableCellElement) {
    elem.classList.add('clickable-text');
    elem.innerText = cell.label;
    elem.addEventListener('click', async () => {
        // Open subreport using the provided query string
        await openDialogAsync({
            schema: {
                title: cell.label,
                queryString: cell.schemaQueryString
            }
        });
    });
    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updatePrimitiveEntryCell(cell: PrimitiveEntryCell, elem: HTMLTableCellElement, isTable: boolean) {
    if (cell.label == null)
        elem.classList.add('cell-null');

    if (cell.primitiveType == 'checkbox') {
        // Add a checkbox
        let checkboxNode: HTMLInputElement = document.createElement('input');
        checkboxNode.type = 'checkbox';
        checkboxNode.checked = cell.label == '1';
        elem.appendChild(checkboxNode);

        elem.addEventListener('click', (_) => {
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
                editCellContents: {
                    primitiveEntry: {
                        cellOid: cell.cellOid,
                        valueOid: cell.valueOid,
                        label: checkboxNode.checked ? '1' : '0',
                        primitiveType: cell.primitiveType,
                        validationFailures: []
                    }
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
        if (isTable) { // Make contents of table cell editable
            elem.contentEditable = 'true';
            elem.innerText = cell.label ?? '';

            // Set up an event listener for when the value is changed
            elem.addEventListener('focusout', async () => {
                const newPrimitiveValue: string | null = elem.innerText.trimEnd();
                await executeAsync({
                    editCellContents: {
                        primitiveEntry: {
                            cellOid: cell.cellOid,
                            valueOid: cell.valueOid,
                            label: newPrimitiveValue,
                            primitiveType: cell.primitiveType,
                            validationFailures: []
                        }
                    }
                })
                .catch(async e => {
                    await message(e, {
                        title: "Unable to update value.",
                        kind: 'warning'
                    });
                });
            });
        } else { // Display as text field
            let input: HTMLInputElement = document.createElement('input');
            input.classList.add('input');
            input.inputMode = 'text';
            input.value = cell.label ?? '';
            input.placeholder = '— NULL —';

            // Set up an event listener for when the value is changed
            input.addEventListener('change', async () => {
                const newPrimitiveValue: string | null = input.value.trimEnd();
                await executeAsync({
                    editCellContents: {
                        primitiveEntry: {
                            cellOid: cell.cellOid,
                            valueOid: cell.valueOid,
                            label: newPrimitiveValue,
                            primitiveType: cell.primitiveType,
                            validationFailures: []
                        }
                    }
                })
                .catch(async e => {
                    await message(e, {
                        title: "Unable to update value.",
                        kind: 'warning'
                    });
                });
            });

            // Add the input to the cell
            elem.appendChild(input);
        }
    }

    // Add validation failure tooltips
    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updateFileEntryCell(cell: FileEntryCell, elem: HTMLTableCellElement) {
    /**
     * Uploads a file from the filesystem to the backend.
     */
    async function uploadFileAsync() {
        const filepath = await open({
            title: 'Upload File to DungeonDB'
        });
        if (filepath) {
            let isSoftLink: boolean = await ask(
                'Do you want this file to be a soft link that references the location of this file?', {
                    title: 'Upload File as Soft Link?'
                }
            );

            let worker: Worker = new Worker('./workers/uploadFile.ts', { type: 'module' });
            worker.onmessage = async function (event) {
                const fileOid: number = event.data;
                await executeAsync({
                    editCellContents: {
                        fileEntry: {
                            cellOid: cell.cellOid,
                            valueOid: cell.valueOid,
                            label: cell.label,
                            fileOid: fileOid,
                            validationFailures: []
                        }
                    }
                })
                .catch(async (e) => {
                    await message(e, {
                        title: 'An error occurred while updating cell with new file content.',
                        kind: 'error'
                    });
                });
            };
            worker.onerror = async function (event) {
                await message(event.error, {
                    title: 'An error occurred while uploading file.',
                    kind: 'error'
                });
            };
            worker.postMessage({
                file: isSoftLink ? {
                    path: {
                        oid: 0,
                        path: filepath
                    }
                } : {
                    blob: {
                        oid: 0
                    }
                },
                uploadFromPath: filepath
            });
        }
    }

    /**
     * Downloads the data from the backend to a location in the filesystem.
     */
    async function downloadFileAsync() {
        const filepath = await save({
            title: 'Download File from DungeonDB'
        });
        if (filepath) {
            let worker: Worker = new Worker('./workers/downloadBlob.ts', { type: 'module' });
            worker.onmessage = async function () {
                await message(`The file was successfully downloaded to "${filepath}".`, {
                    title: 'Download completed successfully.',
                    kind: 'info'
                });
            };
            worker.onerror = async function (event) {
                await message(event.error, {
                    title: 'An error occurred while downloading file.',
                    kind: 'error'
                });
            }
            worker.postMessage({ 
                fileOid: cell.fileOid,
                downloadToPath: filepath
            });
        }
    }

    if (cell.fileOid == null) {
        // Display a simple link to upload a file
        elem.classList.add('clickable-text');
        elem.innerText = 'Upload File';
        elem.addEventListener('click', uploadFileAsync);
    } else { // Display upload and download buttons + current BLOB size
        elem.classList.add('file-text');

        let gridDiv: HTMLDivElement = document.createElement('div');
        gridDiv.style.display = 'grid';
        gridDiv.style.gridTemplateColumns = '1fr auto auto';
        elem.appendChild(gridDiv);

        // Display the size of the file
        let fileDescNode: HTMLSpanElement = document.createElement('span');
        fileDescNode.innerText = cell.label ?? '';
        gridDiv.appendChild(fileDescNode);

        // Button for uploading a file
        let fileUploadNode: HTMLImageElement = document.createElement('img');
        fileUploadNode.classList.add('clickable-text');
        fileUploadNode.alt = 'Upload';
        fileUploadNode.src = '/src-tauri/icons/upload.png';
        fileUploadNode.addEventListener('click', uploadFileAsync);
        fileUploadNode.tabIndex = 0;
        gridDiv.appendChild(fileUploadNode);

        // Button for downloading the file that's in the database
        let fileDownloadNode: HTMLImageElement = document.createElement('img');
        fileDownloadNode.classList.add('clickable-text');
        fileDownloadNode.alt = 'Download';
        fileDownloadNode.src = '/src-tauri/icons/download.png';
        fileDownloadNode.addEventListener('click', downloadFileAsync);
        fileDownloadNode.tabIndex = 0;
        gridDiv.appendChild(fileDownloadNode);

        // Attempt to display image, if an image MIME type is detected
        let base64Worker: Worker = new Worker('./workers/getFileBase64.ts', { type: 'module' });
        base64Worker.onmessage = async function (event) {
            const imgBase64: string = event.data;
            const imgBinary: Uint8Array = Uint8Array.fromBase64(imgBase64);
            const mimeType: FileTypeResult | undefined = await fileTypeFromBuffer(imgBinary);
            if (mimeType && mimeType.mime.startsWith('image/')) {
                // Move buttons to upload/download image to the bottom-right corner
                let flexDiv: HTMLDivElement = document.createElement('div');
                flexDiv.style.position = 'absolute';
                flexDiv.style.left = '0';
                flexDiv.style.right = '0';
                flexDiv.style.bottom = '0';
                flexDiv.style.display = 'flex';
                flexDiv.style.justifyContent = 'flex-end';
                flexDiv.style.alignItems = 'flex-end';
                flexDiv.appendChild(fileUploadNode);
                flexDiv.appendChild(fileDownloadNode);
                elem.appendChild(flexDiv);

                // Display image thumbnail
                let img: HTMLImageElement = document.createElement('img');
                img.src = `data:${mimeType.mime};base64,${imgBase64}`;
                img.alt = cell.label ?? '';
                gridDiv.replaceWith(img);
            }
        };
        base64Worker.postMessage({ fileOid: cell.fileOid });
    }

    // Add validation failure tooltips
    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updateObjectCell(cell: ObjectCell, elem: HTMLTableCellElement) {
    elem.classList.add('clickable-text');
    if (cell.objectQueryString == null)
        elem.classList.add('cell-null');
    elem.innerText = cell.label ?? '';

    elem.addEventListener('click', async () => {
        if (cell.objectQueryString) {
            // Open object dialog
            await openDialogAsync({
                object: {
                    title: cell.label ?? '',
                    queryString: `schema_oid=${cell.objectSchemaOid}&${cell.objectQueryString}`
                }
            });
        } else {
            // If no object has been created for this cell, create an object for this cell
            await executeAsync({
                editCellContents: {
                    object: {
                        cellOid: cell.cellOid,
                        valueOid: cell.valueOid,
                        label: null,
                        objectSchemaOid: cell.objectSchemaOid,
                        objectQueryString: '',
                        validationFailures: []
                    }
                }
            })
            .catch(async e => {
                await message(e, {
                    title: "Unable to create object.",
                    kind: 'error'
                });
            });
        }
    });

    // Add validation failure tooltips
    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updateSelectEntryCell(cell: SelectEntryCell, elem: HTMLTableCellElement) {
    const selectNode: HTMLSelectElement = document.createElement('select');
    
    // Add validation failure tooltips
    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updateMultiselectEntryCell(cell: MultiselectEntryCell, elem: HTMLTableCellElement) {
    

    // Add validation failure tooltips
    addValidationFailureTooltips(elem, cell.validationFailures);
}

export function createCell(cell: Cell, isTable: boolean, filters: [string, number][]): HTMLTableCellElement | HTMLTableRowElement {
    function createCellElement(cellOid: CellOid, callbackFn: (e: HTMLTableCellElement) => void) {
        const id: string = `schema${cellOid.schemaOid}-row${cellOid.rowOid}-column${cellOid.columnOid}`;
        const elem: HTMLTableCellElement = document.createElement('td');
        elem.id = id;
        navigator.locks.request(id, () => {
            callbackFn(elem);
        });
        return elem;
    }

    if ('readonly' in cell) {
        return createCellElement(cell.readonly.cellOid, (elem) => {
            updateReadonlyCell(cell.readonly, elem);
        });
    } else if ('subreport' in cell) {
        return createCellElement(cell.subreport.cellOid, (elem) => {
            updateSubreportCell(cell.subreport, elem);
        });
    } else if ('primitiveEntry' in cell) {
        return createCellElement(cell.primitiveEntry.cellOid, (elem) => {
            updatePrimitiveEntryCell(cell.primitiveEntry, elem, isTable);
        });
    } else if ('fileEntry' in cell) {
        return createCellElement(cell.fileEntry.cellOid, (elem) => {
            updateFileEntryCell(cell.fileEntry, elem);
        })
    } else if ('object' in cell) {
        return createCellElement(cell.object.cellOid, (elem) => {
            updateObjectCell(cell.object, elem);
        });
    } else if ('selectEntry' in cell) {
        return createCellElement(cell.selectEntry.cellOid, (elem) => {
            updateSelectEntryCell(cell.selectEntry, elem);
        });
    } else if ('multiselectEntry' in cell) {
        return createCellElement(cell.multiselectEntry.cellOid, (elem) => {
            updateMultiselectEntryCell(cell.multiselectEntry, elem);
        });
    } else if ('addNewRowButton' in cell) {
        const row: HTMLTableRowElement = document.createElement('tr');
        const elem: HTMLTableCellElement = document.createElement('td');
        elem.innerText = 'Add New Row';
        elem.classList.add('clickable-text');
        elem.style.whiteSpace = 'nowrap';
        elem.colSpan = 2 + cell.addNewRowButton.columnSpan;
        elem.addEventListener('click', () => {
            executeAsync({
                createRow: {
                    tableOid: cell.addNewRowButton.tableOid,
                    rowOid: null
                }
            })
            .catch(async (e) => {
                await message(e, {
                    title: 'An error occurred while adding new row.',
                    kind: 'error'
                });
            })
        });
        row.appendChild(elem);
        return row;
    } else {
        const row: HTMLTableRowElement = document.createElement('tr');
        const id: string = `schema${cell.row.schemaOid}-row${cell.row.rowOid}-index`;
        const elem: HTMLTableCellElement = document.createElement('td');
        elem.id = id;
        navigator.locks.request(id, () => {
            updateRowIndexCell(cell.row, elem);
        });
        row.appendChild(elem);
        return row;
    }
}

export function updateCell(cell: Cell, isTable: boolean) {
    function getCellElement(cellOid: CellOid, callbackFn: (e: HTMLTableCellElement) => void) {
        const id: string = `schema${cellOid.schemaOid}-row${cellOid.rowOid}-column${cellOid.columnOid}`;
        navigator.locks.request(id, () => {
            const prevElem: HTMLTableCellElement | null = document.getElementById(id) as HTMLTableCellElement;
            if (prevElem) {
                const elem: HTMLTableCellElement = document.createElement('td');
                elem.id = id;
                prevElem.replaceWith(elem);
                callbackFn(elem);
            }
        });
    }

    if ('readonly' in cell) {
        getCellElement(cell.readonly.cellOid, (elem) => {
            updateReadonlyCell(cell.readonly, elem);
        });
    } else if ('subreport' in cell) {
        getCellElement(cell.subreport.cellOid, (elem) => {
            updateSubreportCell(cell.subreport, elem);
        });
    } else if ('primitiveEntry' in cell) {
        getCellElement(cell.primitiveEntry.cellOid, (elem) => {
            updatePrimitiveEntryCell(cell.primitiveEntry, elem, isTable);
        });
    } else if ('fileEntry' in cell) {
        getCellElement(cell.fileEntry.cellOid, (elem) => {
            updateFileEntryCell(cell.fileEntry, elem);
        });
    } else if ('object' in cell) {
        getCellElement(cell.object.cellOid, (elem) => {
            updateObjectCell(cell.object, elem);
        });
    } else if ('selectEntry' in cell) {
        getCellElement(cell.selectEntry.cellOid, (elem) => {
            updateSelectEntryCell(cell.selectEntry, elem);
        });
    } else if ('multiselectEntry' in cell) {
        getCellElement(cell.multiselectEntry.cellOid, (elem) => {
            updateMultiselectEntryCell(cell.multiselectEntry, elem);
        });
    } else if ('addNewRowButton' in cell) {
        // Ignore. An update should not make any changes to this cell.
    } else {
        const id: string = `schema${cell.row.schemaOid}-row${cell.row.rowOid}-index`;
        navigator.locks.request(id, () => {
            const prevElem: HTMLTableCellElement | null = document.getElementById(id) as HTMLTableCellElement;
            if (prevElem) {
                const elem: HTMLTableCellElement = document.createElement('td');
                elem.id = id;
                prevElem.replaceWith(elem);
                updateRowIndexCell(cell.row, elem);
            }
        });
    }
}
