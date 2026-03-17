import { Blob } from "../cell";
import { getFileBase64Async } from "../query";

onmessage = function (event) {
    const blob: Blob = event.data;
    getFileBase64Async(blob)
        .then((base64) => this.postMessage(base64));
}