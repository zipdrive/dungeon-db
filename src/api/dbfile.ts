import { invoke } from "@tauri-apps/api/core";

/**
 * Initializes a new DungeonDB file.
 */
export async function newAsync(): Promise<void> {
    await invoke('init_new', {});
}

/**
 * Opens a prompt to load a DungeonDB file.
 */
export async function loadAsync(): Promise<void> {
    await invoke('load', {});
}

/**
 * Saves the current DungeonDB file, or opens a prompt to save to a location if the file has not been saved yet.
 */
export async function saveAsync(): Promise<void> {
    await invoke('save', {});
}

/**
 * Opens a prompt to save to a particular location.
 */
export async function saveAsAsync(): Promise<void> {
    await invoke('save_as', {});
}

/**
 * Undoes the last action.
 */
export async function undoAsync(): Promise<void> {
    await invoke('undo', {});
}

/**
 * Redoes the last undone action.
 */
export async function redoAsync(): Promise<void> {
    await invoke('redo', {});
}