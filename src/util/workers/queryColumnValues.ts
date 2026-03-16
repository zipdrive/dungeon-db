import { Channel } from "@tauri-apps/api/core";
import { DropdownValue, queryAsync } from "../query";

onmessage = function(event) {
    let schemaOid: number = event.data;
    let channel: Channel<DropdownValue> = new Channel<DropdownValue>();
    channel.onmessage = function (payload) {
        postMessage(payload);
    }
    queryAsync({
        columnValues: {
            schemaOid: schemaOid,
            channel: channel
        }
    });
}