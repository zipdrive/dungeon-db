import { Action, executeAsync } from "../action";
import { message } from "@tauri-apps/plugin-dialog";

onmessage = async function (event) {
    const action: Action = event.data;
    await executeAsync(action);
}