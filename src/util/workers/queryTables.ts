import { Channel } from "@tauri-apps/api/core";
import { HierarchicalListItemMetadata, queryAsync } from "../query";

onmessage = function(event) {
    let channel: Channel<HierarchicalListItemMetadata> = new Channel<HierarchicalListItemMetadata>();
    channel.onmessage = function (payload) {
        postMessage(payload);
    };
    queryAsync({
        tables: {
            channel: channel
        }
    });
}