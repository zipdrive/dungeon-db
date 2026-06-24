import { invoke } from "@tauri-apps/api/core";
import { open, save, message } from "@tauri-apps/plugin-dialog";
import "./util/shortcut"; // Install shortcuts

// Open new DungeonDB file
invoke('init_new', {}).catch(async (e) => {
  await message(e, {
    title: 'Error while creating new DungeonDB file.',
    kind: 'error'
  });
});
// Go to main page
window.location.href = '/src/main.html';