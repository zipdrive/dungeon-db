import { loadAsync, newAsync, redoAsync, saveAsAsync, saveAsync, undoAsync } from "./dbfile";

window.addEventListener("keydown", async (e: KeyboardEvent) => {
    // Check for any shortcuts global to the entire application
    if (e.ctrlKey) {
        switch (e.key.toUpperCase()) {
            case 'N':
                e.preventDefault();
                await newAsync();
                break;
            case 'O':
                e.preventDefault();
                await loadAsync();
                break;
            case 'S':
                e.preventDefault();
                await (e.shiftKey ? saveAsAsync : saveAsync)();
                break;
            case 'Z':
                e.preventDefault();
                await undoAsync();
                break;
            case 'Y':
                e.preventDefault();
                await redoAsync();
                break;
        }
    }
});