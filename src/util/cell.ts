import { listen } from "@tauri-apps/api/event";
import { Primitive } from "./column";
import { openDialogAsync } from "./dialog";
import { executeAsync } from "./action";
import { open, save, message } from "@tauri-apps/plugin-dialog";
import { getBlobBase64Async } from "./query";
import { fileTypeFromBuffer, FileTypeResult } from "file-type";

type ValidationFailures = {
    message: string
}[];

type CellOid = {
    schemaOid: number,
    rowOid: number,
    columnOid: number
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
type ObjectCell = {
    cellOid: CellOid,
    valueOid: CellOid,
    label: string | null,
    objectSchemaOid: number,
    objectRowOid: number | null,
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

export type Cell = { row: RowCell } 
| { readonly: ReadonlyCell } 
| { subreport: SubreportCell } 
| { primitiveEntry: PrimitiveEntryCell } 
| { object: ObjectCell } 
| { selectEntry: SelectEntryCell } 
| { multiselectEntry: MultiselectEntryCell };

export type Blob = { blobOid: CellOid };

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

    } else if (cell.primitiveType == 'file') {
        /**
         * Uploads a file from the filesystem to the backend.
         */
        async function uploadFileAsync() {
            const filepath = await open({
                title: 'Upload File to DungeonDB'
            });
            if (filepath) {
                let worker: Worker = new Worker('./workers/action');
                worker.onerror = async function (event) {
                    await message(event.error, {
                        title: 'An error occurred while uploading file.',
                        kind: 'error'
                    });
                };
                worker.postMessage({
                    uploadBlob: {
                        blob: {
                            blobOid: cell.valueOid
                        },
                        filepath: filepath
                    }
                });
            }
        }

        /**
         * Downloads the data from the backend to a location in the filesystem.
         */
        async function downloadFileAsync() {
            const filePath = await save({
                title: 'Download File from DungeonDB'
            });
            if (filePath) {
                let worker: Worker = new Worker('./workers/downloadBlob');
                worker.onerror = async function (event) {
                    await message(event.error, {
                        title: 'An error occurred while downloading file.',
                        kind: 'error'
                    });
                }
                worker.postMessage({ blobOid: cell.valueOid });
            }
        }

        if (cell.label == null) {
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
            fileDescNode.innerText = cell.label;
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
        }

    } else if (cell.primitiveType == 'image') {
        /**
         * Uploads an image from the filesystem to the backend.
         */
        async function uploadImageAsync() {
            const filepath = await open({
                title: 'Upload Image to DungeonDB',
                pickerMode: 'image'
            });
            if (filepath) {
                let worker: Worker = new Worker('./workers/action');
                worker.onerror = async function (event) {
                    await message(event.error, {
                        title: 'An error occurred while uploading file.',
                        kind: 'error'
                    });
                };
                worker.postMessage({
                    uploadBlob: {
                        blob: {
                            blobOid: cell.valueOid
                        },
                        filepath: filepath
                    }
                });
            }
        }

        elem.classList.add('clickable-text');
        if (cell.label == null) {
            // Display a simple link to upload an image
            elem.innerText = 'Upload Image';
        } else {
            // Attempt to display image
            let base64Worker: Worker = new Worker('./workers/getBlobBase64');
            base64Worker.onmessage = async function (event) {
                const imgBase64: string = event.data;
                const imgBinary: Uint8Array = Uint8Array.fromBase64(imgBase64);
                const mimeType: FileTypeResult | undefined = await fileTypeFromBuffer(imgBinary);
                if (mimeType && mimeType.mime.startsWith('image/')) {
                    // Display image thumbnail
                    let img: HTMLImageElement = document.createElement('img');
                    img.src = `data:${mimeType.mime};base64,${imgBase64}`;
                    elem.appendChild(img);
                } else {
                    // Display warning that file is not an image
                    elem.classList.add('cell-error');
                    elem.innerText = '⚠';
                    addTooltip(elem, mimeType ? `Detected non-image MIME type "${mimeType.mime}."` : 'Unable to detect image type.');
                }
            };
            base64Worker.postMessage({ blobOid: cell.valueOid });
        }

        // When the cell is clicked, prompt for an image to upload
        elem.addEventListener('click', uploadImageAsync);

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

    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updateObjectCell(cell: ObjectCell, elem: HTMLTableCellElement) {
    elem.classList.add('clickable-text');
    if (cell.objectRowOid == null)
        elem.classList.add('cell-null');
    elem.innerText = cell.label ?? '';

    elem.addEventListener('click', async () => {
        // Open object dialog
        await openDialogAsync({
            object: {
                title: cell.label ?? '',
                queryString: `schema_oid=${cell.objectSchemaOid}&`
            }
        });
    });

    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updateSelectEntryCell(cell: SelectEntryCell, elem: HTMLTableCellElement) {


    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updateMultiselectEntryCell(cell: MultiselectEntryCell, elem: HTMLTableCellElement) {


    addValidationFailureTooltips(elem, cell.validationFailures);
}

export function createCell(cell: Cell, isTable: boolean): HTMLTableCellElement | HTMLTableRowElement {
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

function updateCell(cell: Cell, isTable: boolean) {
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

listen<Cell>('cell', (e) => {
    updateCell(e.payload);
});