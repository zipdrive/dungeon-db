import { uploadFileAsync } from "../query";

onmessage = function (event) {
    uploadFileAsync(event.data)
        .then((fileOid) => this.postMessage(fileOid));
}