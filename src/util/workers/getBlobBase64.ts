import { Blob } from "../cell";
import { getBlobBase64Async } from "../query";

onmessage = function (event) {
    const blob: Blob = event.data;
    getBlobBase64Async(blob)
        .then((base64) => this.postMessage(base64));
}