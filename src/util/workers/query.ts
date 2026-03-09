import { Query, queryAsync } from "../query";

onmessage = function(event) {
    let query: Query = event.data;
    queryAsync(query);
}