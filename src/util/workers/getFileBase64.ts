import { getFileBase64Async } from "../query";

onmessage = function (event) {
    getFileBase64Async(event.data)
        .then((base64) => this.postMessage(base64));
}