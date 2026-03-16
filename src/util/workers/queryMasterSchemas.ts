import { Channel } from "@tauri-apps/api/core";
import { ToggledHierarchicalListItemMetadata, queryAsync } from "../query";

onmessage = function(event) {
    let data: { schemaOid: number | null, isTable: boolean } = event.data;
    let channel: Channel<ToggledHierarchicalListItemMetadata> = new Channel<ToggledHierarchicalListItemMetadata>();
    channel.onmessage = function (payload) {
        postMessage(payload);
    };
    queryAsync({
        masterSchemas: {
            ...data,
            channel: channel
        }
    });
}