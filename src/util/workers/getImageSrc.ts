import { getImageSrcAsync } from "../query";

onmessage = function (event) {
    getImageSrcAsync(event.data)
        .then((src) => this.postMessage(src));
}