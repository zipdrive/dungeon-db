import { invoke } from "@tauri-apps/api/core";

export function initListeners() {
  // Set up the tab listeners
  let addTableButton: HTMLInputElement | null = document.querySelector('#add-new-table-button');
  addTableButton?.addEventListener("click", (e) => {
    invoke("dialog__create_table", {});
  });
};
