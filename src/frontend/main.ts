import { invoke } from "@tauri-apps/api/core";
import { open, save, message } from "@tauri-apps/plugin-dialog";
import { initListeners as initTableListeners } from "./tables";

window.addEventListener("DOMContentLoaded", () => {
  // Set up listeners for the Tables tab
  initTableListeners();
});
