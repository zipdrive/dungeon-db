import { invoke } from "@tauri-apps/api/core";
import { open, save, message } from "@tauri-apps/plugin-dialog";

/**
 * Start up DungeonDB.
 * @param filePath The filepath of the database file used by DungeonDB.
 */
function initialize(filePath: string) {
  // Initialize the database connection
  invoke("init", {
    path: filePath
  }).catch(async e => {
    await message(e, {
      title: 'Error while connecting to DungeonDB file.',
      kind: 'error'
    });
  });

  // GOTO main page
  window.location.replace('/src/frontend/tables.html');
}


window.addEventListener("DOMContentLoaded", () => {
  // Set up the main menu listeners
  let newDbButton: HTMLInputElement | null = document.querySelector('#new-db-button');
  newDbButton?.addEventListener("click", async (e) => {
    const filePath = await save({
      filters: [{
        name: "DungeonDB (*.db)",
        extensions: ['db']
      }]
    });
    if (filePath != null) {
        initialize(filePath);
    }
  });

  let loadDbButton: HTMLInputElement | null = document.querySelector('#load-db-button');
  loadDbButton?.addEventListener("click", async (e) => {
    const filePath = await open({
      filters: [{
        name: "DungeonDB (*.db)",
        extensions: ['db']
      }]
    });
    if (filePath != null) {
      initialize(filePath);
    }
  });
});