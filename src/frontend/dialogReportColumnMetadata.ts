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
async function loadMetadataFromFields(): Promise<{ name: string, columnStyle: string } & ({ columnType: 'Formula', formula: string } | { columnType: 'Subreport', baseParameterOid: number }) > {
    const columnName = (document.getElementById('column-name') as HTMLInputElement)?.value;
    if (!columnName) {
        throw new Error("A column cannot have no name.");
    }

    const columnType: string = (document.getElementById('column-type') as HTMLInputElement)?.value;
    const columnStyle: string = (document.getElementById('column-style') as HTMLTextAreaElement)?.value ?? '';

    if (columnType == 'Subreport') {
        // Get the base parameter OID
        return {
            name: columnName,
            columnStyle: columnStyle,
            columnType: 'Subreport',
            baseParameterOid: 0
        }
    } else {
        return {
            name: columnName,
            columnStyle: columnStyle,
            columnType: 'Formula',
            formula: ''
        }
    }
}

// Add initial listeners
window.addEventListener("DOMContentLoaded", async () => {
    const urlParams = new URLSearchParams(window.location.search);

    document.getElementById('advanced-parameter-toggle-button')?.addEventListener('click', toggleAdvancedParameters);


    if (urlParams.has('column_oid')) {
        // This indicates that the column exists already and is being edited

        const urlParamReportOid = urlParams.get('report_oid');
        const urlParamColumnOid = urlParams.get('column_oid');
        if (!urlParamReportOid || !urlParamColumnOid) {
            await message("Dialog window does not have expected GET parameters.", { 
                title: "An error occurred while editing column.", 
                kind: 'error' 
            });
            return;
        }
        const reportOid: number = parseInt(urlParamReportOid);
        const columnOid: number = parseInt(urlParamColumnOid);

        // Populate in the metadata for the column
        await queryAsync({
            invokeAction: 'get_table_column',
            invokeParams: { 
                columnOid: columnOid
            }
        })
        .then(async (columnMetadata: TableColumnMetadata) => {
            

            // Edit the column when OK is clicked
            document.querySelector('#confirm-button')?.addEventListener("click", async (e) => {
                e.preventDefault();
                e.returnValue = false;

                // Edit the column
                await loadMetadataFromFields()
                .then(async (changedMetadata) => {
                    if (changedMetadata.columnType == 'Formula') {
                        // Update formula
                    } else {
                        // Update subreport
                    }
                })
                .then(closeDialogAsync)
                .catch(async (e) => {
                    await message(e, {
                        title: "An error occurred while applying changes to report column.",
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
        document.querySelector('#confirm-button')?.addEventListener("click", async (e) => {
            e.preventDefault();
            e.returnValue = false;

            const reportOid = urlParams.get('report_oid');
            const columnOrdering = urlParams.get('column_ordering');
            if (!reportOid) {
                await message("Dialog window does not have expected GET parameters.", { 
                    title: "An error occurred while creating column.", 
                    kind: 'error' 
                });
                return;
            }

            // Load metadata from fields
            await loadMetadataFromFields()
            .then(async (metadata) => {
                // Create the column
                if (metadata.columnType == 'Formula') {
                    // Create a formula column
                    await executeAsync({
                        createReportFormulaColumn: {
                            reportOid: parseInt(reportOid),
                            columnName: metadata.name,
                            columnOrdering: columnOrdering ? parseInt(columnOrdering) : null,
                            columnStyle: metadata.columnStyle,
                            formula: metadata.formula
                        }
                    });
                } else {
                    // Create a subreport column
                    await executeAsync({
                        createReportSubreportColumn: {
                            reportOid: parseInt(reportOid),
                            columnName: metadata.name,
                            columnOrdering: columnOrdering ? parseInt(columnOrdering) : null,
                            columnStyle: metadata.columnStyle,
                            baseParameterOid: metadata.baseParameterOid
                        }
                    });
                }
            })
            .then(closeDialogAsync)
            .catch(async (e) => {
                await message(e, {
                    title: "An error occurred while creating column in report.",
                    kind: 'error'
                });
            });
        });
    }


    // Close the dialog when Cancel is clicked
    document.querySelector('#cancel-button')?.addEventListener("click", async (e) => {
        e.preventDefault();
        e.returnValue = false;

        await closeDialogAsync();
    });

    // Turn on or off various parameters to match the necessary parameters for the chosen column type
    showParameters();
    document.getElementById('column-type')?.addEventListener('change', showParameters);
});