import { listen } from "@tauri-apps/api/event";
import { FullMetadata as ColumnFullMetadata, Primitive, ColumnType } from "./column";
import { openDialogAsync } from "./dialog";
import { executeAsync } from "./action";
import { open, save, message, ask } from "@tauri-apps/plugin-dialog";
import { DropdownValue, getCellAsync, getFileBase64Async, queryAsync, SelectedHierarchicalListItemMetadata, uploadFileAsync } from "./query";
import { fileTypeFromBuffer, FileTypeResult } from "file-type";
import { Channel } from "@tauri-apps/api/core";
import { Menu, MenuItem } from "@tauri-apps/api/menu";


/**
 * Clipboard data for cells.
 */
export type CellClipboardData = { rows: CellContent[], columnType: ColumnType } 
    | { rows: {[key: number]: CellContent}[] };




type ValidationFailures = {
    message: string
}[];

type CellDependency = {
    tableOid: number,
    columnOid: number,
    rowOid: number
};

export type CellIdentifier = {
    tableOid: number,
    columnOid: number,
    rowOid: number
} | {
    columnOid: number,
    queryFilter: string,
    isolatedCellDependencies: CellDependency[],
    fullReloadCellDependencies: CellDependency[]
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

type SchemaRow = {
    rowIdentifier: [number, number] | null,
    index: number,
    fixedParentDatasource: [number, number, ColumnFullMetadata] | null,
    validationFailures: ValidationFailures
};
type AddNewRowButton = {
    tableOid: number,
    fixedParentDatasource: [number, number, ColumnFullMetadata] | null,
    columnSpan: number
};


type CellContentTextFormat = 'plain' | 'jSON';

type ReadonlyCellContent = {
    cellIdentifier: CellIdentifier,
    label: string | null,
    format: CellContentTextFormat,
    validationFailures: ValidationFailures
};
type TextEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    format: CellContentTextFormat,
    validationFailures: ValidationFailures 
};
type IntegerEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    value: number | null,
    validationFailures: ValidationFailures 
};
type NumberEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    value: number | null,
    validationFailures: ValidationFailures 
};
type DateEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    validationFailures: ValidationFailures 
};
type DatetimeEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    validationFailures: ValidationFailures 
};
type CheckboxEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    isChecked: boolean | null,
    validationFailures: ValidationFailures 
};
type FileEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    fileOid: number | null,
    validationFailures: ValidationFailures
};
type ImageEntryCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    fileOid: number | null,
    label: string | null,
    fileSrc: string | null,
    validationFailures: ValidationFailures
};
type SchemaLinkCellContent = {
    cellIdentifier: CellIdentifier,
    label: string | null,
    linkSchemaOid: number,
    linkQueryFilter: string | null,
    validationFailures: ValidationFailures
};
type ObjectLinkCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    linkSchemaOid: number,
    linkQueryFilter: string | null,
    validationFailures: ValidationFailures
};
type SingleSelectDropdownCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    dropdownTableOid: number,
    dropdownRowOid: number | null,
    validationFailures: ValidationFailures
};
type MultiSelectDropdownCellContent = {
    cellIdentifier: CellIdentifier,
    dataTableOid: number,
    dataColumnOid: number,
    dataRowOid: number,
    label: string | null,
    dropdownTableOid: number,
    dropdownRowOid: number[],
    validationFailures: ValidationFailures
};

export type CellContent = { readonly: ReadonlyCellContent } 
| { textEntry: TextEntryCellContent }
| { integerEntry: IntegerEntryCellContent }
| { numberEntry: NumberEntryCellContent }
| { dateEntry: DateEntryCellContent }
| { datetimeEntry: DatetimeEntryCellContent }
| { checkboxEntry: CheckboxEntryCellContent } 
| { fileEntry: FileEntryCellContent }
| { imageEntry: ImageEntryCellContent }
| { schemaLink: SchemaLinkCellContent }
| { objectLink: ObjectLinkCellContent } 
| { singleSelectDropdown: SingleSelectDropdownCellContent } 
| { multiSelectDropdown: MultiSelectDropdownCellContent };
export type CellStream = { cell: [CellContent] }
| { row: SchemaRow } 
| { addNewRowButton: AddNewRowButton };


export type DataCellEntry = {
    tableOid: number,
    columnOid: number,
    rowOid: number,
    value: {
        text: {
            label: string | null 
        }
    } | {
        integer: {
            value: number | null 
        }
    } | {
        number: {
            value: number | null 
        }
    } | {
        boolean: {
            value: boolean | null 
        }
    } | {
        date: {
            label: string | null 
        } 
    } | {
        datetime: {
            label: string | null 
        }
    } | {
        file: {
            fileOid: number | null 
        }
    } | {
        object: {
            linkedRowOid: number | null 
        }
    } | {
        select: {
            linkedRowOid: number | null 
        }
    } | {
        multiselect: {
            linkedRowOid: number | null
        }
    }
};


export class Cell {
    elem: HTMLTableCellElement;
    content: CellContent;

    /**
     * Begins editing the cell.
     */
    #startEditingAsync: () => Promise<void> = async () => {};

    /**
     * A callback function for when the user starts editing the cell.
     */
    #startEditingCallbackFn: (cell: Cell) => void;

    /**
     * Stops editing the cell.
     * @returns The Cell that should replace this one.
     */
    #stopEditingAsync: () => Promise<void> = async () => {};

    /**
     * A callback function for when the user stops editing the cell.
     */
    #stopEditingCallbackFn: (cell: Cell) => void;

    constructor(cwd: Document, content: CellContent, startEditingCallbackFn: (cell: Cell) => void, stopEditingCallbackFn: (cell: Cell) => void) {
        this.content = content;
        this.#startEditingCallbackFn = startEditingCallbackFn;
        this.#stopEditingCallbackFn = stopEditingCallbackFn;

        // Construct the HTMLElement
        if ('textEntry' in content) {
            this.elem = this.#constructTextEntryCell(cwd, content.textEntry);
        } else if ('integerEntry' in content) {
            this.elem = this.#constructIntegerEntryCell(cwd, content.integerEntry);
        } else if ('numberEntry' in content) {
            this.elem = this.#constructNumberEntryCell(cwd, content.numberEntry);
        } else if ('dateEntry' in content) {
            this.elem = this.#constructDateEntryCell(cwd, content.dateEntry);
        } else if ('datetimeEntry' in content) {
            this.elem = this.#constructDatetimeEntryCell(cwd, content.datetimeEntry);
        } else if ('checkboxEntry' in content) {
            this.elem = this.#constructCheckboxEntryCell(cwd, content.checkboxEntry);
        } else if ('fileEntry' in content) {
            this.elem = this.#constructFileEntryCell(cwd, content.fileEntry);
        } else if ('imageEntry' in content) {
            this.elem = this.#constructImageEntryCell(cwd, content.imageEntry);
        } else if ('schemaLink' in content) {
            this.elem = this.#constructSchemaLinkCell(cwd, content.schemaLink);
        } else if ('objectLink' in content) {
            this.elem = this.#constructObjectLinkCell(cwd, content.objectLink);
        } else if ('singleSelectDropdown' in content) {
            this.elem = this.#constructSingleSelectDropdownCell(cwd, content.singleSelectDropdown);
        } else if ('multiSelectDropdown' in content) {
            this.elem = this.#constructMultiSelectDropdownCell(cwd, content.multiSelectDropdown);
        } else {
            this.elem = this.#constructReadonlyCell(cwd, content.readonly)
        }
    }

    /**
     * The user starts editing the cell.
     */
    async startEditingAsync(): Promise<void> {
        this.elem.classList.add('editing');
        this.#startEditingCallbackFn(this);
        await this.#startEditingAsync();
    }

    /**
     * The user stops editing the cell.
     */
    async stopEditingAsync(): Promise<void> {
        this.elem.classList.remove('editing');
        this.#stopEditingCallbackFn(this);
        await this.#stopEditingAsync();
    }



    /**
     * Add a tooltip to an HTML element.
     * @param elem The HTML element.
     * @param tooltip The tooltip to append.
     */
    #addTooltip(elem: HTMLElement, tooltip: string) {
        const existingTooltip: string | null = elem.getAttribute('tooltip');
        elem.setAttribute('tooltip', existingTooltip ? `${existingTooltip} ${tooltip}` : tooltip);
    }

    /**
     * Add tooltips to indicate that there has been a failure in one of the column's validation checks.
     * @param elem The HTMLElement for the cell.
     * @param validationFailures The column's failed validation checks.
     */
    #addValidationFailureTooltips(elem: HTMLElement, validationFailures: ValidationFailures) {
        if (validationFailures.length > 0) {
            elem.classList.add('error');
            this.#addTooltip(elem, validationFailures.map(f => f.message).reduce((acc, m) => `${acc} ${m}`));
        }
    }



    /**
     * Constructs a DIV to display readonly text.
     * For JSON text, color-codes and beautifies the JSON.
     * @param cwd 
     * @param elem The parent HTMLElement.
     * @param label The text to display.
     * @param format The text format.
     * @returns 
     */
    #constructLabel(cwd: Document, elem: HTMLElement, label: string, format: CellContentTextFormat): HTMLDivElement {
        const div: HTMLDivElement = cwd.createElement('div');
        div.classList.add('label');

        if (format == 'plain') {
            elem.classList.add('plain');
            div.innerText = label;
        } else if (format == 'jSON') {
            elem.classList.add('json');
            if (label) {
                try {
                    function constructSpan(parent: HTMLElement, obj: any, level: number) {
                        const span: HTMLSpanElement = cwd.createElement('span');
                        span.setAttribute('level', level.toString());
                        if (Array.isArray(obj)) {
                            span.classList.add('array');

                            const openDelimiter: HTMLSpanElement = cwd.createElement('span');
                            openDelimiter.classList.add('delimiter');
                            openDelimiter.innerText = '[';
                            span.appendChild(openDelimiter);

                            const firstBreak = cwd.createTextNode('\n\t');
                            span.appendChild(firstBreak);

                            for (let k = 0; k < obj.length; ++k) {
                                const value = obj[k];
                                constructSpan(span, value, level + 1);

                                if (k < obj.length - 1) {
                                    const commaBreak = cwd.createTextNode(',\n\t');
                                    span.appendChild(commaBreak);
                                } else {
                                    const noCommaBreak = cwd.createTextNode('\n');
                                    span.appendChild(noCommaBreak);
                                }
                            }

                            const closeDelimiter: HTMLSpanElement = cwd.createElement('span');
                            closeDelimiter.classList.add('delimiter');
                            closeDelimiter.innerText = ']';
                            span.appendChild(closeDelimiter);
                        } else if (typeof obj === 'string' || obj instanceof String) {
                            span.classList.add('string');
                            span.innerText = `"${obj.replace('\\', '\\\\').replace('"', '\\"')}"`;
                        } else if (typeof obj === 'boolean') {
                            span.classList.add('boolean');
                            span.innerText = obj ? 'true' : 'false';
                        } else if (typeof obj === 'number' || typeof obj === 'bigint') {
                            span.classList.add('number');
                            span.innerText = obj.toString();
                        } else {
                            span.classList.add('object');

                            const openDelimiter: HTMLSpanElement = cwd.createElement('span');
                            openDelimiter.classList.add('delimiter');
                            openDelimiter.innerText = '{';
                            span.appendChild(openDelimiter);

                            const firstBreak = cwd.createTextNode('\n\t');
                            span.appendChild(firstBreak);

                            const entries = Object.entries(obj);
                            for (let k = 0; k < entries.length; ++k) {
                                const [key, value] = entries[k];
                                constructSpan(span, key, level + 1);
                                const colonBreak = cwd.createTextNode(': ');
                                span.appendChild(colonBreak);
                                constructSpan(span, value, level + 1);

                                if (k < entries.length - 1) {
                                    const commaBreak = cwd.createTextNode(',\n\t');
                                    span.appendChild(commaBreak);
                                } else {
                                    const noCommaBreak = cwd.createTextNode('\n');
                                    span.appendChild(noCommaBreak);
                                }
                            }

                            const closeDelimiter: HTMLSpanElement = cwd.createElement('span');
                            closeDelimiter.classList.add('delimiter');
                            closeDelimiter.innerText = '}';
                            span.appendChild(closeDelimiter);
                        }
                        parent.appendChild(span);
                    }

                    const parsedObj = JSON.parse(label);
                    constructSpan(div, parsedObj, 1);
                } catch (e) {
                    div.innerText = label;
                    this.#addValidationFailureTooltips(elem, [{ message: `${e}` }]);
                }
            }
        }
        elem.appendChild(div);
        return div;
    }


    /**
     * Construct a cell for free-text entry.
     * @param cwd 
     * @param content The content of a text entry cell.
     */
    #constructTextEntryCell(cwd: Document, content: TextEntryCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', content.format);
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'text';
        input.addEventListener('blur', this.stopEditingAsync);
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            input.value = content.label || '';

            // Remove the readonly text, insert the input
            elem.removeChild(readonly);
            elem.appendChild(input);
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            const rawLabel: string | null = input.value || null;
            let label: string | null;
            if (content.format == 'jSON' && rawLabel) {
                // Automatically attempt to beautify the JSON
                try {
                    label = JSON.stringify(JSON.parse(rawLabel), null, '\t');
                } catch (e) {
                    label = rawLabel;
                }
            } else {
                label = rawLabel;
            }

            if (label !== content.label) {
                // Update the cell contents in the database
                await executeAsync({
                    editCellContents: {
                        tableOid: content.dataTableOid,
                        columnOid: content.dataColumnOid,
                        rowOid: content.dataRowOid,
                        value: {
                            text: {
                                label: label
                            }
                        }
                    }
                }).catch(async (e) => {
                    await message(e, {
                        title: 'Unable to update cell contents.',
                        kind: 'error'
                    });
                });
            }

            // Remove the input, restore the readonly text
            elem.removeChild(input);
            elem.appendChild(readonly);
        };

        return elem;
    }

    /**
     * Construct a cell for free-text integer entry.
     * @param cwd 
     * @param content The content of an integer entry cell.
     */
    #constructIntegerEntryCell(cwd: Document, content: IntegerEntryCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.value?.toString() || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.value?.toString() || '', 'plain');
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'number';
        input.step = '1';
        input.addEventListener('blur', this.stopEditingAsync);
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            input.value = content.value?.toString() || '';

            // Remove the readonly text, insert the input
            elem.removeChild(readonly);
            elem.appendChild(input);
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            // Validate the entered value
            const parsedValue: number = parseInt(input.value);
            if (Number.isNaN(parsedValue) && input.value) {
                await message(`Entered value is not an integer!`, {
                    kind: 'warning'
                });
                return;
            }

            const value: number | null = Number.isNaN(parsedValue) ? null : parsedValue;
            if (value !== content.value) {
                // Update the cell contents in the database
                await executeAsync({
                    editCellContents: {
                        tableOid: content.dataTableOid,
                        columnOid: content.dataColumnOid,
                        rowOid: content.dataRowOid,
                        value: {
                            integer: {
                                value: value
                            }
                        }
                    }
                }).catch(async (e) => {
                    await message(e, {
                        title: 'Unable to update cell contents.',
                        kind: 'error'
                    });
                });
            }

            // Remove the input, restore the readonly text
            elem.removeChild(input);
            elem.appendChild(readonly);
        };

        return elem;
    }

    /**
     * Construct a cell for free-text number entry.
     * @param cwd 
     * @param content The content of a number entry cell.
     */
    #constructNumberEntryCell(cwd: Document, content: NumberEntryCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.value?.toString() || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.value?.toString() || '', 'plain');
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'number';
        input.addEventListener('blur', this.stopEditingAsync);
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            input.value = content.value?.toString() || '';

            // Remove the readonly text, insert the input
            elem.removeChild(readonly);
            elem.appendChild(input);
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            // Validate the entered value
            const parsedValue: number = parseFloat(input.value);
            if (Number.isNaN(parsedValue) && input.value) {
                await message(`Entered value is not a number!`, {
                    kind: 'warning'
                });
                return;
            }

            const value: number | null = Number.isNaN(parsedValue) ? null : parsedValue;
            if (value !== content.value) {
                // Update the cell contents in the database
                await executeAsync({
                    editCellContents: {
                        tableOid: content.dataTableOid,
                        columnOid: content.dataColumnOid,
                        rowOid: content.dataRowOid,
                        value: {
                            number: {
                                value: value
                            }
                        }
                    }
                }).catch(async (e) => {
                    await message(e, {
                        title: 'Unable to update cell contents.',
                        kind: 'error'
                    });
                });
            }

            // Remove the input, restore the readonly text
            elem.removeChild(input);
            elem.appendChild(readonly);
        };

        return elem;
    }



    /**
     * Construct a cell for date entry.
     * @param cwd 
     * @param content The content of a date entry cell.
     */
    #constructDateEntryCell(cwd: Document, content: DateEntryCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'date';
        input.addEventListener('blur', this.stopEditingAsync);
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            input.value = content.label || '';

            // Remove the readonly text, insert the input
            elem.removeChild(readonly);
            elem.appendChild(input);
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            const label: string | null = input.value || null;
            if (label !== content.label) {
                // Update the cell contents in the database
                await executeAsync({
                    editCellContents: {
                        tableOid: content.dataTableOid,
                        columnOid: content.dataColumnOid,
                        rowOid: content.dataRowOid,
                        value: {
                            date: {
                                label: label
                            }
                        }
                    }
                }).catch(async (e) => {
                    await message(e, {
                        title: 'Unable to update cell contents.',
                        kind: 'error'
                    });
                });
            }

            // Remove the input, restore the readonly text
            elem.removeChild(input);
            elem.appendChild(readonly);
        };

        return elem;
    }

    /**
     * Construct a cell for datetime entry.
     * @param cwd 
     * @param content The content of a datetime entry cell.
     */
    #constructDatetimeEntryCell(cwd: Document, content: DatetimeEntryCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        const input: HTMLInputElement = cwd.createElement('input');
        input.type = 'datetime-local';
        input.addEventListener('blur', this.stopEditingAsync);
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            input.value = content.label || '';

            // Remove the readonly text, insert the input
            elem.removeChild(readonly);
            elem.appendChild(input);
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            const label: string | null = input.value || null;
            if (label !== content.label) {
                // Update the cell contents in the database
                await executeAsync({
                    editCellContents: {
                        tableOid: content.dataTableOid,
                        columnOid: content.dataColumnOid,
                        rowOid: content.dataRowOid,
                        value: {
                            date: {
                                label: label
                            }
                        }
                    }
                }).catch(async (e) => {
                    await message(e, {
                        title: 'Unable to update cell contents.',
                        kind: 'error'
                    });
                });
            }

            // Remove the input, restore the readonly text
            elem.removeChild(input);
            elem.appendChild(readonly);
        };

        return elem;
    }



    /**
     * Construct a cell that contains a checkbox that toggles the boolean value of a cell.
     * @param cwd 
     * @param content The content of a checkbox entry cell.
     * @returns 
     */
    #constructCheckboxEntryCell(cwd: Document, content: CheckboxEntryCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        if (content.isChecked == null)
            elem.setAttribute('label', '');
        else if (content.isChecked) 
            elem.setAttribute('label', 'True');
        else 
            elem.setAttribute('label', 'False');
        
        // Add a checkbox to the cell
        let inputLabel: HTMLLabelElement = document.createElement('label');
        let input: HTMLInputElement = document.createElement('input');
        input.type = 'checkbox';
        input.checked = content.isChecked ?? false;
        input.addEventListener('click', (e) => {
            // The checkbox can only get triggered by startEditing()
            e.stopPropagation();
        });
        inputLabel.appendChild(input);
        elem.appendChild(inputLabel);

        // When you start editing, swap whether the checkbox is checked, then immediately stop editing
        this.#startEditingAsync = async () => {
            input.checked = !input.checked;
            await this.stopEditingAsync();
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            const isChecked: boolean = input.checked;
            if (isChecked !== content.isChecked) {
                // Update the cell contents in the database
                await executeAsync({
                    editCellContents: {
                        tableOid: content.dataTableOid,
                        columnOid: content.dataColumnOid,
                        rowOid: content.dataRowOid,
                        value: {
                            boolean: {
                                value: isChecked
                            }
                        }
                    }
                }).catch(async (e) => {
                    await message(e, {
                        title: 'Unable to update cell contents.',
                        kind: 'error'
                    });
                });
            }
        };

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }



    /**
     * Construct a DIV that allows the user to tab between uploading a reference to a file and uploading the file to the database as a BLOB.
     * @param cwd 
     * @param content The content of a file/image entry cell.
     * @returns 
     */
    #constructFileInput(cwd: Document, content: FileEntryCellContent): { div: HTMLDivElement, getFileOidAsync: () => Promise<number | null> } {
        let fileOid: number | null = content.fileOid;

        // Set up the tabs
        const input: HTMLDivElement = cwd.createElement('div');
        input.style.display = 'grid';
        input.style.gridTemplateColumns = '1fr 1fr';
        

        // Set up tab for relative/absolute path
        const pathTab: HTMLLabelElement = cwd.createElement('label');
        pathTab.innerText = "Path";
        pathTab.classList.add('tab');
        input.appendChild(pathTab);
        const pathRadio: HTMLInputElement = cwd.createElement('input');
        pathRadio.type = 'radio';
        pathRadio.name = `${content.cellIdentifier}`;
        pathRadio.style.visibility = 'hidden';
        pathTab.appendChild(pathRadio);
        const pathTabContent: HTMLDivElement = cwd.createElement('div');
        pathTabContent.style.display = 'grid';
        pathTabContent.style.gridTemplateColumns = '1fr auto';
        input.appendChild(pathTabContent);

        // Text input for a path
        const pathInput: HTMLInputElement = cwd.createElement('input');
        pathInput.innerText = (content.label && !content.label.endsWith(')')) ? content.label : '';
        pathTabContent.appendChild(pathInput);
        
        // Button for selecting a path
        const pathUploadButton: HTMLImageElement = document.createElement('img');
        pathUploadButton.alt = 'Upload';
        pathUploadButton.src = './src-tauri/icons/upload.png';
        pathUploadButton.addEventListener('click', async () => {
            const filepath = await open({
                title: 'Reference File by Path'
            });
            if (filepath) {
                pathInput.value = filepath;
            }
        });
        pathTabContent.appendChild(pathUploadButton);


        // Set up tab for blob upload/download
        const fileTab: HTMLLabelElement = cwd.createElement('label');
        fileTab.innerText = "File";
        fileTab.classList.add('tab');
        input.appendChild(fileTab);
        const fileRadio: HTMLInputElement = cwd.createElement('input');
        fileRadio.type = 'radio';
        fileRadio.name = `${content.cellIdentifier}`;
        fileRadio.style.visibility = 'hidden';
        fileTab.appendChild(fileRadio);
        const fileTabContent: HTMLDivElement = cwd.createElement('div');
        fileTabContent.style.display = 'grid';
        fileTabContent.style.gridTemplateColumns = '1fr auto auto';
        input.appendChild(fileTabContent);

        // Label for filename
        const fileLabel: HTMLDivElement = document.createElement('div');
        fileLabel.innerText = content.label?.endsWith(')') ? content.label : '';
        fileTabContent.appendChild(fileLabel);

        // Button for directly uploading a file to the database
        const fileUploadButton: HTMLImageElement = document.createElement('img');
        fileUploadButton.alt = 'Upload';
        fileUploadButton.src = './src-tauri/icons/upload.png';
        fileUploadButton.addEventListener('click', async () => {
            const filepath = await open({
                title: 'Upload File to DungeonDB'
            });
            if (filepath) {
                let worker: Worker = new Worker('./workers/uploadFile.ts', { type: 'module' });
                worker.onmessage = async function (event) {
                    fileOid = event.data;
                    fileLabel.innerText = filepath;
                };
                worker.onerror = async function (event) {
                    await message(event.error, {
                        title: 'An error occurred while uploading file.',
                        kind: 'error'
                    });
                };
                worker.postMessage({
                    file: {
                        blob: {
                            oid: 0
                        }
                    },
                    uploadFromPath: filepath
                });
            }
        });
        input.appendChild(fileUploadButton);

        // Button for downloading the file that is stored or referenced
        const fileDownloadButton: HTMLImageElement = document.createElement('img');
        fileDownloadButton.classList.add('clickable-text');
        fileDownloadButton.alt = 'Download';
        fileDownloadButton.src = './src-tauri/icons/download.png';
        fileDownloadButton.addEventListener('click', async () => {
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
                    fileOid: fileOid,
                    downloadToPath: filepath
                });
            }
        });
        input.appendChild(fileDownloadButton);


        // Choose which tab is selected by default, based on whether the label includes file size or not
        if (content.label?.endsWith(')')) {
            fileRadio.checked = true;
        } else {
            pathRadio.checked = true;
        }

        return {
            div: input,
            getFileOidAsync: async () => {
                if (pathRadio.checked && pathInput.value !== content.label) {
                    // If the file is a path and has been changed, upload the path to the database and get the resulting File OID
                    return await uploadFileAsync({
                        file: {
                            path: {
                                oid: 0,
                                path: pathInput.value
                            }
                        },
                        filepath: pathInput.value
                    });
                } else {
                    return fileOid;
                }
            }
        };
    }

    /**
     * Construct a cell that allows the user to select a file.
     * @param cwd 
     * @param content The content of a file entry cell.
     * @returns 
     */
    #constructFileEntryCell(cwd: Document, content: FileEntryCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        let { div: input, getFileOidAsync } = this.#constructFileInput(cwd, content);
        input.addEventListener('blur', this.stopEditingAsync);
        input.addEventListener('keydown', async (e) => {
            if (e.key == 'Enter' && !e.ctrlKey && !e.shiftKey && !e.metaKey) {
                e.preventDefault();
                await this.stopEditingAsync();
            }
        });

        this.#startEditingAsync = async () => {
            // Remove the image, insert the input
            elem.classList.add('editing');
            elem.removeChild(readonly);
            elem.appendChild(input);
        };
        this.#stopEditingAsync = async () => {
            // Get the File OID
            const fileOid: number | null = await getFileOidAsync();

            // Update the cell contents in the database
            await executeAsync({
                editCellContents: {
                    tableOid: content.dataTableOid,
                    columnOid: content.dataColumnOid,
                    rowOid: content.dataRowOid,
                    value: {
                        file: {
                            fileOid: fileOid
                        }
                    }
                }
            }).catch(async (e) => {
                await message(e, {
                    title: 'Unable to update cell contents.',
                    kind: 'error'
                });
            });

            // Remove the input, restore the image
            elem.classList.remove('editing');
            elem.removeChild(input);
            elem.appendChild(readonly);

            // Refresh the input area
            ({ div: input, getFileOidAsync } = this.#constructFileInput(cwd, content));
        };

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }

    /**
     * Construct a cell that allows the user to select a file, and attempts to display the file as an image.
     * @param cwd 
     * @param content The content of an image entry cell.
     * @returns 
     */
    #constructImageEntryCell(cwd: Document, content: ImageEntryCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const img: HTMLImageElement = cwd.createElement('img');
        if (content.fileSrc)
            img.src = content.fileSrc;
        img.alt = content.label || '';
        elem.appendChild(img);
        let { div: input, getFileOidAsync } = this.#constructFileInput(cwd, content);

        this.#startEditingAsync = async () => {
            // Remove the image, insert the input
            elem.classList.add('editing');
            elem.removeChild(img);
            elem.appendChild(input);
        };
        this.#stopEditingAsync = async () => {
            // Get the File OID
            const fileOid: number | null = await getFileOidAsync();

            // Update the cell contents in the database
            await executeAsync({
                editCellContents: {
                    tableOid: content.dataTableOid,
                    columnOid: content.dataColumnOid,
                    rowOid: content.dataRowOid,
                    value: {
                        file: {
                            fileOid: fileOid
                        }
                    }
                }
            }).catch(async (e) => {
                await message(e, {
                    title: 'Unable to update cell contents.',
                    kind: 'error'
                });
            });

            // Remove the input, restore the image
            elem.classList.remove('editing');
            elem.removeChild(input);
            elem.appendChild(img);

            // Refresh the input area
            ({ div: input, getFileOidAsync } = this.#constructFileInput(cwd, content));
        };

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }



    #constructSingleSelectDropdownCell(cwd: Document, content: SingleSelectDropdownCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');

        // When you start editing, swap the readonly DIV for an editable INPUT
        this.#startEditingAsync = async () => {
            // Load the dropdown values
            

            // Remove the readonly text
            elem.removeChild(readonly);
            elem.appendChild(input);
        };

        // When you stop editing, update the database and swap the editable INPUT for the readonly DIV
        this.#stopEditingAsync = async () => {
            const rawLabel: string | null = input.value || null;
            let label: string | null;
            if (content.format == 'jSON' && rawLabel) {
                // Automatically attempt to beautify the JSON
                try {
                    label = JSON.stringify(JSON.parse(rawLabel), null, '\t');
                } catch (e) {
                    label = rawLabel;
                }
            } else {
                label = rawLabel;
            }

            if (label !== content.label) {
                // Update the cell contents in the database
                await executeAsync({
                    editCellContents: {
                        tableOid: content.dataTableOid,
                        columnOid: content.dataColumnOid,
                        rowOid: content.dataRowOid,
                        value: {
                            text: {
                                label: label
                            }
                        }
                    }
                }).catch(async (e) => {
                    await message(e, {
                        title: 'Unable to update cell contents.',
                        kind: 'error'
                    });
                });
            }

            // Remove the input, restore the readonly text
            elem.removeChild(input);
            elem.appendChild(readonly);
        };

        return elem;
    }

    #constructMultiSelectDropdownCell(cwd: Document, content: MultiSelectDropdownCellContent): HTMLTableCellElement {
        
    }



    /**
     * Construct a cell that contains a readonly link to another schema.
     * @param cwd 
     * @param content The content of the link to another schema.
     * @returns 
     */
    #constructSchemaLinkCell(cwd: Document, content: SchemaLinkCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');
        
        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        readonly.classList.add('clickable');
        readonly.addEventListener('click', async () => {
            // Open schema using the provided query string
            await openDialogAsync({
                schema: {
                    title: content.label || '',
                    queryString: `schema_oid=${content.linkSchemaOid}&${content.linkQueryFilter}`
                }
            });
        });

        // Cell cannot be edited, so immediately stop editing
        this.#startEditingAsync = async () => {
            await this.stopEditingAsync();
        }

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }

    /**
     * Construct a cell that contains a readonly link to an object.
     * @param cwd 
     * @param content The content of the link to an object.
     * @returns 
     */
    #constructObjectLinkCell(cwd: Document, content: ObjectLinkCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.setAttribute('label', content.label || '');
        
        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', 'plain');
        readonly.classList.add('clickable');
        readonly.addEventListener('click', async () => {
            // Open schema using the provided query string
            await openDialogAsync({
                object: {
                    title: content.label || '',
                    queryString: `schema_oid=${content.linkSchemaOid}&${content.linkQueryFilter}`
                }
            });
        });

        // Cell cannot be edited, so immediately stop editing
        this.#startEditingAsync = async () => {
            await this.stopEditingAsync();
        }

        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }

    /**
     * Construct a cell with a readonly label.
     * @param cwd 
     * @param content The content of the readonly cell.
     * @returns 
     */
    #constructReadonlyCell(cwd: Document, content: ReadonlyCellContent): HTMLTableCellElement {
        const elem: HTMLTableCellElement = cwd.createElement('td');
        elem.classList.add('readonly');
        elem.setAttribute('label', content.label || '');

        const readonly: HTMLDivElement = this.#constructLabel(cwd, elem, content.label || '', content.format);
        this.#addValidationFailureTooltips(elem, content.validationFailures);
        return elem;
    }
}



let dropdownValueCallbacks: {
    [key: number]: ((dropdownValue: DropdownValue) => Promise<void>)[]
} = {};

function addDropdownValueCallback(schemaOid: number, callbackFn: (dropdownValue: DropdownValue) => Promise<void>) {
    if (schemaOid in dropdownValueCallbacks) {
        dropdownValueCallbacks[schemaOid].push(callbackFn);
    } else {
        dropdownValueCallbacks[schemaOid] = [callbackFn];
    }
}

export function runDropdownValueQueries() {
    let promises: Promise<void>[] = [];
    for (let schemaOidStr in dropdownValueCallbacks) {
        const schemaOid: number = parseInt(schemaOidStr);
        promises.push(queryAsync({
            columnValues: {
                schemaOid: schemaOid,
                channel: new Channel<DropdownValue>(async (dropdownValue) => {
                    await Promise.all(dropdownValueCallbacks[schemaOid].map(callbackFn => callbackFn(dropdownValue)));
                })
            }
        }));
    }
    Promise.all(promises).then(() => {
        dropdownValueCallbacks = {};
    })
}





function addValidationFailureTooltips(elem: HTMLElement, validationFailures: ValidationFailures) {
    
}



function updateRowIndexCell(cell: SchemaRow, elem: HTMLTableCellElement) {
    elem.innerText = `${cell.index}`;

    // Attach context menu
    elem.addEventListener('contextmenu', async (e) => {
        e.preventDefault();

        let contextMenuItems: Promise<MenuItem>[] = [];
        if (cell.rowIdentifier) {
            let [tableOid, rowOid] = cell.rowIdentifier;

            // Item to insert row
            contextMenuItems.push(MenuItem.new({
                text: 'Insert Row',
                action: () => {
                    executeAsync({
                        createRow: {
                            tableOid: tableOid,
                            rowOid: rowOid,
                            fixedParentDatasource: cell.fixedParentDatasource
                        }
                    })
                    .catch(async (e) => {
                        await message(e, {
                            title: 'An error occurred while inserting the row.',
                            kind: 'error'
                        });
                    });
                }
            }));

            // Item to delete row
            contextMenuItems.push(MenuItem.new({
                text: 'Delete Row',
                action: () => {
                    executeAsync({
                        trashRow: {
                            tableOid: tableOid,
                            rowOid: rowOid
                        }
                    })
                    .catch(async (e) => {
                        await message(e, {
                            title: 'An error occurred while deleting the row.',
                            kind: 'error'
                        });
                    });
                }
            }));
        }

        // Only display context menu if there are context menu items to display
        if (contextMenuItems.length > 0) {
            const contextMenu: Menu = await Menu.new({
                items: await Promise.all(contextMenuItems)
            });

            await contextMenu.popup()
            .catch(async (e) => {
                await message(e, {
                    title: 'An error occurred while displaying context menu.',
                    kind: 'error'
                });
            });
        }
    });
}

function updateReadonlyCell(cell: ReadonlyCellContent, elem: HTMLTableCellElement) {
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
                            label: newPrimitiveValue ? newPrimitiveValue : null,
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
                            label: newPrimitiveValue ? newPrimitiveValue : null,
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

function updateFileEntryCell(cell: FileEntryCellContent, elem: HTMLTableCellElement) {
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

function updateObjectCell(cell: ObjectLinkCellContent, elem: HTMLTableCellElement) {
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

function updateSelectEntryCell(cell: SingleSelectDropdownCellContent, elem: HTMLTableCellElement) {
    if (!cell.selectRowOid)
        elem.classList.add('cell-null');

    const selectElem: HTMLSelectElement = document.createElement('select');
    selectElem.classList.add('cell-dropdown');
    selectElem.innerHTML = '<option value="">— NULL —</option>'
    elem.appendChild(selectElem);

    // Add callback function to populate the options for the SELECT element
    addDropdownValueCallback(cell.selectSchemaOid, async (dropdownValue) => {
        const optionElem: HTMLOptionElement = document.createElement('option');
        optionElem.value = dropdownValue.value.toString();
        optionElem.innerText = dropdownValue.label;
        if (dropdownValue.value == cell.selectRowOid) {
            optionElem.selected = true;
        }
        selectElem.appendChild(optionElem);
    });

    // Add event listener to update the cell when select is changed
    selectElem.addEventListener('input', async () => {
        await executeAsync({
            editCellContents: {
                selectEntry: {
                    cellOid: cell.cellOid,
                    valueOid: cell.valueOid,
                    selectSchemaOid: cell.selectSchemaOid,
                    selectRowOid: selectElem.value ? parseInt(selectElem.value) : null,
                    validationFailures: []
                }
            }
        })
        .catch(async (e) => {
            await message(e, {
                title: 'An error occurred while updating cell value.',
                kind: 'error'
            });
        })
    });
    
    // Add validation failure tooltips
    addValidationFailureTooltips(elem, cell.validationFailures);
}

function updateMultiselectEntryCell(cell: MultiSelectDropdownCellContent, elem: HTMLTableCellElement) {
    // Set the cell's label when unselected
    if (cell.label)
        elem.innerText = cell.label;
    else 
        elem.classList.add('cell-null');

    // Create DIV to hold multiselect options
    let multiselectElem: HTMLDivElement = document.createElement('div');
    multiselectElem.classList.add('cell-multiselect');
    elem.appendChild(multiselectElem);

    // Add callback function to populate the multiselect options
    addDropdownValueCallback(cell.multiselectSchemaOid, async (dropdownValue) => {
        // Create HTML element to represent the option
        let labelNode: HTMLLabelElement = document.createElement('label');
        labelNode.innerText = dropdownValue.label;
        labelNode.insertAdjacentHTML('afterbegin', `<input type="checkbox" value="${dropdownValue.value}" ${(cell.multiselectRowOid.includes(dropdownValue.value) ? 'checked' : '')}>`);
        multiselectElem.appendChild(labelNode);

        
    });

    // Add event listener to update from the multiselect DIV when unfocused
    multiselectElem.addEventListener('focusout', async (e) => {
        console.debug('focusout event triggered.');
        if (!e.relatedTarget || !elem.contains(e.relatedTarget as HTMLElement)) {
            console.debug(`Multiselect focused out.\n  e.relatedTarget: ${e.relatedTarget}\n  elem.contains(e.relatedTarget): ${elem.contains(e.relatedTarget as HTMLElement)}`);
            let newSelectedOidList: number[] = [];
            multiselectElem.querySelectorAll('input:checked').forEach((checkboxNode) => {
                newSelectedOidList.push(parseInt((checkboxNode as HTMLInputElement).value));
            });
            await executeAsync({
                editCellContents: {
                    multiselectEntry: {
                        cellOid: cell.cellOid,
                        valueOid: cell.valueOid,
                        label: cell.label,
                        multiselectSchemaOid: cell.multiselectSchemaOid,
                        multiselectRowOid: newSelectedOidList,
                        validationFailures: []
                    }
                }
            })
            .catch(async (e) => {
                await message(e, {
                    title: 'An error occurred while updating cell value.',
                    kind: 'error'
                });
            });
        }
    });

    // Add validation failure tooltips
    addValidationFailureTooltips(elem, cell.validationFailures);
}

export function createCellAsync(cell: CellContent, isSchema: boolean): HTMLTableCellElement | HTMLTableRowElement | null {
    function createCellElement(cellOid: CellIdentifier, callbackFn: (e: HTMLTableCellElement) => void) {
        const elem: HTMLTableCellElement = document.createElement('td');
        elem.dataset.cellOid = JSON.stringify(cellOid);
        elem.classList.add(`column${cellOid.columnOid}`);
        if ('filters' in cellOid) {
            cellOid.filters.forEach(([datasourceAlias, datasourceRowOid]) => {
                elem.classList.add(`${datasourceAlias}__${datasourceRowOid}`);
            });
        } else {
            elem.id = `column${cellOid.columnOid}-row${cellOid.rowOid}`;
        }
        elem.tabIndex = 0;
        callbackFn(elem);
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
            updatePrimitiveEntryCell(cell.primitiveEntry, elem, isSchema);
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
        if (isSchema) {
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
                        rowOid: null,
                        fixedParentDatasource: cell.addNewRowButton.fixedParentDatasource
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
            // Do not add an Add New Row button if is an Object form
            return null;
        }
    } else {
        if (isSchema) {
            // Create a new row, if the page is showing a schema
            const row: HTMLTableRowElement = document.createElement('tr');
            const elem: HTMLTableCellElement = document.createElement('td');
            if (cell.row.rowIdentifier) {
                row.dataset.rowIdentifier = JSON.stringify(cell.row.rowIdentifier);
                row.classList.add('reorderable-row');
                elem.classList.add('reorderable-row-dragger');
            }
            updateRowIndexCell(cell.row, elem);
            row.appendChild(elem);
            return row;
        } else if (cell.row.rowIdentifier) {
            // Show dropdown that allows user to select the Object subtype
            const row: HTMLTableRowElement = document.createElement('tr');
            row.innerHTML = '<td>Object Type:</td>'
            const elem: HTMLTableCellElement = document.createElement('td');
            const selectElem: HTMLSelectElement = document.createElement('select');
            queryAsync({
                inheritorTables: {
                    tableOid: cell.row.rowIdentifier[0],
                    rowOid: cell.row.rowIdentifier[1],
                    channel: new Channel<SelectedHierarchicalListItemMetadata>((objectType) => {
                        const optionElem: HTMLOptionElement = document.createElement('option');
                        optionElem.value = `${objectType.oid}:${objectType.masterOid}`;
                        optionElem.innerText = `${' '.repeat(objectType.level)}${objectType.name}`;
                        selectElem.appendChild(optionElem);
                    })
                }
            })
            .catch(async (e) => {
                await message(e, {
                    title: 'An error occurred while querying for Object type.',
                    kind: 'error'
                });
            });
            elem.appendChild(selectElem);
            row.appendChild(elem);
            return row;
        }
    }
    return null;
}

export function updateCell(cellOid: CellIdentifier, isSchema: boolean) {
    const query: string = 'filters' in cellOid ? 
        `.column${cellOid.columnOid}${cellOid.filters.map(([datasourceAlias, datasourceRowOid]) => `.${datasourceAlias}__${datasourceRowOid}`).join('')}` :
        `#column${cellOid.columnOid}-row${cellOid.rowOid}`
    ;
    console.debug(`  Query string: "${query}"`);
    document.querySelectorAll(query).forEach(async (prevElem) => {
        console.debug(prevElem);

        // Construct replacement element
        const elem: HTMLTableCellElement = document.createElement('td');
        elem.id = prevElem.id;
        elem.classList.add(`column${cellOid.columnOid}`);
        prevElem.classList.forEach((prevElemClass) => {
            if (prevElemClass.toUpperCase().startsWith('ROOT')) {
                elem.classList.add(prevElemClass);
            }
        });
        elem.tabIndex = 0;
        prevElem.replaceWith(elem);

        // Query for the cell
        const cell: CellContent = await getCellAsync(cellOid);
        console.debug(`cell: ${JSON.stringify(cell)}`);
        if ('readonly' in cell) {
            updateReadonlyCell(cell.readonly, elem);
        } else if ('subreport' in cell) {
            updateSubreportCell(cell.subreport, elem);
        } else if ('primitiveEntry' in cell) {
            updatePrimitiveEntryCell(cell.primitiveEntry, elem, isSchema);
        } else if ('fileEntry' in cell) {
            updateFileEntryCell(cell.fileEntry, elem);
        } else if ('object' in cell) {
            updateObjectCell(cell.object, elem);
        } else if ('selectEntry' in cell) {
            updateSelectEntryCell(cell.selectEntry, elem);
        } else if ('multiselectEntry' in cell) {
            updateMultiselectEntryCell(cell.multiselectEntry, elem);
        } // Ignore everything else

        // Query for dropdowns
        runDropdownValueQueries();
    });
}
