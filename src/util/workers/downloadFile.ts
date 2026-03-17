import { downloadFileAsync } from "../query";

onmessage = function (event) {
    downloadFileAsync(event.data);
}