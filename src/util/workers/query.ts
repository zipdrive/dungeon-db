import { Channel } from "@tauri-apps/api/core";
import { queryAsync } from "../query";

onmessage = function (event) {
    queryAsync({
        channel: new Channel<any>((data: any) => {
            this.postMessage(data);
        }),
        ...event.data
    });
}