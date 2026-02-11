import { message } from "@tauri-apps/plugin-dialog";
import { BasicHierarchicalMetadata, BasicMetadata, closeDialogAsync, ColumnType, DropdownValue, executeAsync, queryAsync, TableColumnMetadata } from "./backendutils";
import { Channel } from "@tauri-apps/api/core";


let showAdvancedParameters: boolean = false;

/**
 * Show the parameters specific to the currently-specified type.
 */
function showParameters() {
    // Turn off all parameters
    const selectedType = (document.getElementById('column-type') as HTMLInputElement)?.value;
    document.querySelectorAll('.parameter').forEach((varParamNode) => { (varParamNode as HTMLTableRowElement).style.display = 'none'; });

    // Turn on only the parameters for the specified type
    document.querySelectorAll(`.parameter-${selectedType}`).forEach((varParamNode) => {
        let varParamRowNode = varParamNode as HTMLTableRowElement;
        if (!varParamRowNode.classList.contains('parameter-advanced') || (varParamRowNode.classList.contains('parameter-advanced') && showAdvancedParameters))
            varParamRowNode.style.display = 'table-row'; 
    });
}

/**
 * Toggle whether advanced parameters are displayed or not.
 */
function toggleAdvancedParameters() {
    showAdvancedParameters = !showAdvancedParameters;
    showParameters();

    let advancedParametersButton: HTMLElement | null = document.getElementById('advanced-parameter-toggle-button');
    if (advancedParametersButton) {
        advancedParametersButton.innerText = `${showAdvancedParameters ? '⊖' : '⊕'} Advanced`;
    }
}

/**
 * Retrieves the inputted metadata from the fields of the dialog.
 * @returns 
 */
async function loadMetadataFromFields(): Promise<TableColumnMetadata> {
    const columnName = (document.getElementById('column-name') as HTMLInputElement)?.value;
    if (!columnName) {
        throw new Error("A column cannot have no name.");
    }

    const columnTypeStr = (document.getElementById('column-type') as HTMLInputElement)?.value;
    const isNullable: boolean = (document.getElementById('column-is-nullable') as HTMLInputElement)?.checked ?? true;
    let isUnique: boolean = (document.getElementById('column-is-unique') as HTMLInputElement)?.checked ?? false;
    let isPrimaryKey: boolean = (document.getElementById('column-is-primary-key') as HTMLInputElement)?.checked ?? false;
    const columnStyle: string = (document.getElementById('column-style') as HTMLTextAreaElement)?.value ?? '';

    let columnType: ColumnType;
    switch (columnTypeStr) {
        case 'Boolean':
            columnType = { primitive: columnTypeStr };
            isUnique = false;
            break;
        case 'Integer':
        case 'Number':
        case 'Date':
        case 'Timestamp':
        case 'Text':
        case 'JSON':
            columnType = { primitive: columnTypeStr };
            break;
        case 'File':
        case 'Image':
            columnType = { primitive: columnTypeStr };
            isUnique = false;
            isPrimaryKey = false;
            break;
        case 'SingleSelectDropdown':
            columnType = { singleSelectDropdown: 0 };
            isUnique = false;
            break;
        case 'MultiSelectDropdown':
            columnType = { multiSelectDropdown: 0 };
            break;
        case 'Reference':
            const referencedTableOid = (document.getElementById('column-type-oid-reference') as HTMLInputElement)?.value;
            if (!referencedTableOid) {
                throw new Error("You must select a referenced table for a column of type Reference.");
            }
            columnType = { reference: parseInt(referencedTableOid) };
            break;
        case 'Object':
            const objTableOid = (document.getElementById('column-type-oid-reference') as HTMLInputElement)?.value;
            if (!objTableOid) {
                throw new Error("You must select a global data type for a column of type Global Data Type.");
            }
            columnType = { childObject: parseInt(objTableOid) };
            isUnique = false;
            isPrimaryKey = false;
            break;
        case 'ChildTable':
            columnType = { childTable: 0 };
            isUnique = false;
            isPrimaryKey = false;
            break;
        default:
            throw new Error("Unknown column type.");
    }

    return {
        oid: 0,
        name: columnName,
        columnOrdering: 0,
        columnStyle: columnStyle,
        columnType: columnType,
        isNullable: isNullable,
        isUnique: isUnique,
        isPrimaryKey: isPrimaryKey
    };
}

// Add initial listeners
window.addEventListener("DOMContentLoaded", async () => {
    const urlParams = new URLSearchParams(window.location.search);

    document.getElementById('advanced-parameter-toggle-button')?.addEventListener('click', toggleAdvancedParameters);

    document.getElementById('add-new-dropdown-value-button')?.addEventListener('click', (_) => {
        let columnDropdownValuesTbl = document.querySelector('#column-dropdown-values > tbody');
        columnDropdownValuesTbl?.insertAdjacentHTML('beforeend', 
            `<tr>
                <td class="dropdown-value-oid"></td>
                <td>${columnDropdownValuesTbl.childElementCount + 1}</td>
                <td><div editablecontent class="dropdown-value-editor" /></td>
            </tr>`)
    });

    // Fill in the dropdown of possible Reference types
    let columnReferenceInput: HTMLSelectElement | null = document.getElementById('column-type-oid-reference') as HTMLSelectElement;
    if (columnReferenceInput) {
        const referenceTypeChannel: Channel<BasicMetadata> = new Channel<BasicMetadata>();
        referenceTypeChannel.onmessage = (referenceType) => {
            console.debug(`Received ${referenceType}`);
            let columnReferenceOption: HTMLOptionElement = document.createElement('option');
            columnReferenceOption.innerText = referenceType.name;
            columnReferenceOption.value = referenceType.oid.toString();
            columnReferenceInput.insertAdjacentElement('beforeend', columnReferenceOption);
        };
        await queryAsync({
            invokeAction: 'get_table_column_reference_values',
            invokeParams: {
                referenceTypeChannel: referenceTypeChannel
            }
        });
    }

    // Fill in the dropdown of possible Global Data Type types
    let columnObjDataTypeInput: HTMLSelectElement | null = document.getElementById('column-type-oid-object') as HTMLSelectElement;
    if (columnObjDataTypeInput) {
        const objDataTypeChannel: Channel<BasicMetadata> = new Channel<BasicMetadata>();
        objDataTypeChannel.onmessage = (objDataType) => {
            let columnObjDataTypeOption: HTMLOptionElement = document.createElement('option');
            columnObjDataTypeOption.innerText = objDataType.name;
            columnObjDataTypeOption.value = objDataType.oid.toString();
            columnObjDataTypeInput.insertAdjacentElement('beforeend', columnObjDataTypeOption);
        };
        await queryAsync({
            invokeAction: 'get_table_column_object_values',
            invokeParams: {
                objectTypeChannel: objDataTypeChannel
            }
        });
    }

    if (urlParams.has('column_oid')) {
        // This indicates that the column exists already and is being edited

        const tableOid = urlParams.get('table_oid');
        const columnOid = urlParams.get('column_oid');
        if (!tableOid || !columnOid) {
            await message("Dialog window does not have expected GET parameters.", { 
                title: "An error occurred while editing column.", 
                kind: 'error' 
            });
            return;
        }

        // Populate in the metadata for the column
        await queryAsync({
            invokeAction: 'get_table_column',
            invokeParams: { 
                columnOid: parseInt(columnOid) 
            }
        })
        .then((columnMetadata: TableColumnMetadata) => {
            console.debug(columnMetadata);
            let columnNameInput: HTMLInputElement | null = document.getElementById('column-name') as HTMLInputElement;
            if (columnNameInput)
                columnNameInput.value = columnMetadata.name;

            let columnTypeInput: HTMLSelectElement | null = document.getElementById('column-type') as HTMLSelectElement;
            if (columnTypeInput) {
                if ('primitive' in columnMetadata.columnType) {
                    columnTypeInput.value = columnMetadata.columnType.primitive;
                } else if ('singleSelectDropdown' in columnMetadata.columnType) {
                    columnTypeInput.value = 'SingleSelectDropdown';
                } else if ('multiSelectDropdown' in columnMetadata.columnType) {
                    columnTypeInput.value = 'MultiSelectDropdown';
                } else if ('reference' in columnMetadata.columnType) {
                    columnTypeInput.value = 'Reference';
                    if (columnReferenceInput)
                        columnReferenceInput.value = columnMetadata.columnType.reference.toString();
                } else if ('childObject' in columnMetadata.columnType) {
                    columnTypeInput.value = 'Object';
                    if (columnObjDataTypeInput)
                        columnObjDataTypeInput.value = columnMetadata.columnType.childObject.toString();
                } else if ('childTable' in columnMetadata.columnType) {
                    columnTypeInput.value = 'ChildTable';
                }
            }

            let columnStyleInput: HTMLInputElement | null = document.getElementById('column-style') as HTMLInputElement;
            if (columnStyleInput)
                columnStyleInput.value = columnMetadata.columnStyle;

            let isNullableInput: HTMLInputElement | null = document.getElementById('column-is-nullable') as HTMLInputElement;
            if (isNullableInput)
                isNullableInput.checked = columnMetadata.isNullable;

            let isUniqueInput: HTMLInputElement | null = document.getElementById('column-is-unique') as HTMLInputElement;
            if (isUniqueInput)
                isUniqueInput.checked = columnMetadata.isUnique;
            
            let isPrimaryKeyInput: HTMLInputElement | null = document.getElementById('column-is-primary-key') as HTMLInputElement;
            if (isPrimaryKeyInput)
                isPrimaryKeyInput.checked = columnMetadata.isPrimaryKey;

            // Edit the column when OK is clicked
            document.querySelector('#create-table-column-button')?.addEventListener("click", async (e) => {
                e.preventDefault();
                e.returnValue = false;

                // Edit the column
                await loadMetadataFromFields()
                .then(async (changedMetadata) => {
                    // Correct the metadata for the type to use the same type OID, if the type hasn't changed
                    if (('singleSelectDropdown' in columnMetadata.columnType && 'singleSelectDropdown' in changedMetadata.columnType) 
                        || ('multiSelectDropdown' in columnMetadata.columnType && 'multiSelectDropdown' in changedMetadata.columnType) 
                        || ('childTable' in columnMetadata.columnType && 'childTable' in columnMetadata.columnType)) {
                        changedMetadata.columnType = columnMetadata.columnType;
                    }

                    // Edit the column
                    await executeAsync({
                        editTableColumnMetadata: {
                            tableOid: parseInt(tableOid),
                            columnOid: parseInt(columnOid),
                            columnName: changedMetadata.name,
                            columnType: changedMetadata.columnType,
                            columnStyle: changedMetadata.columnStyle,
                            isNullable: changedMetadata.isNullable,
                            isUnique: changedMetadata.isUnique,
                            isPrimaryKey: changedMetadata.isPrimaryKey
                        }
                    });

                    // Update dropdown values
                    if ('singleSelectDropdown' in changedMetadata.columnType || 'multiSelectDropdown' in changedMetadata.columnType) {
                        // Pull new list of dropdown values from form
                        let dropdownValues: DropdownValue[] = [];
                        document.querySelectorAll('#column-dropdown-values > tbody > tr').forEach((dropdownValueRow) => {
                            dropdownValues.push({
                                trueValue: (dropdownValueRow.querySelector('.dropdown-value-oid') as HTMLTableCellElement)?.innerText,
                                displayValue: (dropdownValueRow.querySelector('.dropdown-value-editor') as HTMLDivElement)?.innerText
                            });
                        });

                        // Send request to update set of dropdown values
                        await executeAsync({
                            editTableColumnDropdownValues: {
                                tableOid: parseInt(tableOid),
                                columnOid: parseInt(columnOid),
                                dropdownValues: dropdownValues
                            }
                        });
                    }
                })
                .then(async (_) => await closeDialogAsync())
                .catch(async (e) => {
                    await message(e, {
                        title: "An error occurred while applying changes to table.",
                        kind: 'error'
                    });
                });
            });
        })
        .catch(async e => {
            await message(e, { title: "An error occurred while retrieving column metadata.", kind: 'error' });
        });
    } else {
        // This indicates that the column is being created for the first time, so leave the fields populated with the defaults

        // Create the column when OK is clicked
        document.querySelector('#create-table-column-button')?.addEventListener("click", async (e) => {
            e.preventDefault();
            e.returnValue = false;

            const tableOid = urlParams.get('table_oid');
            const columnOrdering = urlParams.get('column_ordering');
            if (!tableOid) {
                await message("Dialog window does not have expected GET parameters.", { title: "An error occurred while creating column.", kind: 'error' });
                return;
            }

            // Create the column
            await loadMetadataFromFields()
            .then(async (metadata) => await executeAsync({
                createTableColumn: {
                    tableOid: parseInt(tableOid),
                    columnName: metadata.name,
                    columnType: metadata.columnType,
                    columnStyle: metadata.columnStyle,
                    columnOrdering: columnOrdering ? parseInt(columnOrdering) : null,
                    isNullable: metadata.isNullable,
                    isUnique: metadata.isUnique,
                    isPrimaryKey: metadata.isPrimaryKey
                }
            }))
            .then(async (_) => await closeDialogAsync())
            .catch(async (e) => {
                await message(e, {
                    title: "An error occurred while creating column in table.",
                    kind: 'error'
                });
            });
        });
    }


    // Close the dialog when Cancel is clicked
    document.querySelector('#cancel-create-table-column-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;

        await closeDialogAsync();
    });

    // Turn on or off various parameters to match the necessary parameters for the chosen column type
    showParameters();
    document.getElementById('column-type')?.addEventListener('change', showParameters);
});