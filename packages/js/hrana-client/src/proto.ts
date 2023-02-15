// TypeScript types for the messages in the Hrana protocol
//
// The structure of this file follows the specification in HRANA_SPEC.md

export type int32 = number

// ## Messages

export type ClientMsg =
    | HelloMsg
    | RequestMsg

export type ServerMsg =
    | HelloOkMsg
    | HelloErrorMsg
    | ResponseOkMsg
    | ResponseErrorMsg

// ### Hello

export type HelloMsg = {
    "type": "hello",
    "jwt": string | null,
}

export type HelloOkMsg = {
    "type": "hello_ok",
}

export type HelloErrorMsg = {
    "type": "hello_error",
    "error": Error,
}

// ### Request/response

export type RequestMsg = {
    "type": "request",
    "request_id": int32,
    "request": Request,
}

export type ResponseOkMsg = {
    "type": "response_ok",
    "request_id": int32,
    "response": Response,
}

export type ResponseErrorMsg = {
    "type": "response_error",
    "request_id": int32,
    "error": Error,
}

// ### Errors

export type Error = {
    "message": string,
}

// ## Requests

export type Request =
    | OpenStreamReq
    | CloseStreamReq
    | ExecuteReq

export type Response =
    | OpenStreamResp
    | CloseStreamResp
    | ExecuteResp

// ### Open stream

export type OpenStreamReq = {
    "type": "open_stream",
    "stream_id": int32,
}

export type OpenStreamResp = {
    "type": "open_stream",
}

// ### Close stream

export type CloseStreamReq = {
    "type": "close_stream",
    "stream_id": int32,
}

export type CloseStreamResp = {
    "type": "close_stream",
}

// ### Execute a statement

export type ExecuteReq = {
    "type": "execute",
    "stream_id": int32,
    "stmt": Stmt,
}

export type ExecuteResp = {
    "type": "execute",
    "result": StmtResult,
}

export type Stmt = {
    "sql": string,
    "args": Array<Value>,
    "want_rows": boolean,
}

export type StmtResult = {
    "cols": Array<Col>,
    "rows": Array<Array<Value>>,
    "affected_row_count": number,
}

export type Col = {
    "name": string | null,
}

export type Value =
    | { "type": "null" }
    | { "type": "integer", "value": string }
    | { "type": "float", "value": number }
    | { "type": "text", "value": string }
    | { "type": "blob", "base64": string }
