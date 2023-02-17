import * as hrana from "@libsql/hrana-client";

const jwt = (
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSJ9.eyJleHAiOjE2NzY5MDkwOTR9._8Dt3MSN7b5-ykbxM2dCh8CzIPpkqDmPagRXfSO3s1es-6vRN_qMrNGsEUdCFP6tAmCNYd9RJZ9zaUT_wCQ3Bg"
);
const client = hrana.open("ws://localhost:2023", jwt);
const stream = client.openStream();
console.log(await stream.queryValue("SELECT 1"));
client.close();
