import { invoke } from "@tauri-apps/api/core";
import { message } from "@tauri-apps/plugin-dialog";

export type Dialog = { 
    createTable: null 
} | { 
    editTable: { 
        tableOid: number 
    } 
} | { 
    createReport: null 
} | { 
    editReport: { 
        reportOid: number 
    } 
} | { 
    createColumn: {
        schemaOid: number,
        columnOrdering: number | null
    }
} | {
    editColumn: {
        columnOid: number
    }
} | {
    addParameter: {
        id: number,
        schemaOid: number
    }
} | {
    schema: {
        title: string,
        queryString: string 
    }
} | {
    object: {
        title: string,
        queryString: string
    }
};

/**
 * Opens a dialog window.
 * @param dialog The dialog window to open.
 */
export async function openDialogAsync(dialog: Dialog): Promise<void> {
    await invoke('dialog_open', { dialog: dialog })
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while opening dialog box.",
            kind: 'error'
        });
    });
}

/**
 * Closes the current dialog window.
 */
export async function closeDialogAsync(): Promise<void> {
    await invoke('dialog_close', {})
    .catch(async (e) => {
        await message(e, {
            title: "An error occurred while closing dialog box.",
            kind: 'error'
        });
    });
}