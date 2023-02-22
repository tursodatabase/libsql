import WebSocket from "isomorphic-ws";

import type { Stmt, Value, StmtResult, RowArray, Row } from "./convert.js";
import {
    stmtToProto, rowArrayFromProto, rowFromProto,
    stmtResultFromProto, valueFromProto, errorFromProto,
} from "./convert.js";
import IdAlloc from "./id_alloc.js";
import type * as proto from "./proto.js";

export type { Stmt, Value, StmtResult, RowArray, Row } from "./convert";
export type { proto };

/** Open a Hrana client connected to the given `url`. */
export function open(url: string, jwt?: string): Client {
    const socket = new WebSocket(url, ["hrana1"]);
    return new Client(socket, jwt ?? null);
}

/** A client that talks to a SQL server using the Hrana protocol over a WebSocket. */
export class Client {
    #socket: WebSocket;
    // List of messages that we queue until the socket transitions from the CONNECTING to the OPEN state.
    #msgsWaitingToOpen: proto.ClientMsg[];
    // Stores the error that caused us to close the client (and the socket). If we are not closed, this is
    // `undefined`.
    #closed: Error | undefined;

    // Have we received a response to our "hello" from the server?
    #recvdHello: boolean;
    // A map from request id to the responses that we expect to receive from the server.
    #responseMap: Map<number, ResponseState>;
    // An allocator of request ids.
    #requestIdAlloc: IdAlloc;
    // An allocator of stream ids.
    #streamIdAlloc: IdAlloc;

    /** @private */
    constructor(socket: WebSocket, jwt: string | null) {
        this.#socket = socket;
        this.#socket.binaryType = "arraybuffer";
        this.#msgsWaitingToOpen = [];
        this.#closed = undefined;

        this.#recvdHello = false;
        this.#responseMap = new Map();
        this.#requestIdAlloc = new IdAlloc();
        this.#streamIdAlloc = new IdAlloc();

        this.#socket.onopen = () => this.#onSocketOpen();
        this.#socket.onclose = (event) => this.#onSocketClose(event);
        this.#socket.onerror = (event) => this.#onSocketError(event);
        this.#socket.onmessage = (event) => this.#onSocketMessage(event);

        this.#send({"type": "hello", "jwt": jwt});
    }

    // Send (or enqueue to send) a message to the server.
    #send(msg: proto.ClientMsg): void {
        if (this.#closed !== undefined) {
            throw new Error("Internal error: trying to send a message on a closed client");
        }

        if (this.#socket.readyState >= WebSocket.OPEN) {
            this.#sendToSocket(msg);
        } else {
            this.#msgsWaitingToOpen.push(msg);
        }
    }

    // The socket transitioned from CONNECTING to OPEN
    #onSocketOpen(): void {
        for (const msg of this.#msgsWaitingToOpen) {
            this.#sendToSocket(msg);
        }
        this.#msgsWaitingToOpen.length = 0;
    }

    #sendToSocket(msg: proto.ClientMsg): void {
        this.#socket.send(JSON.stringify(msg));
    }

    // Send a request to the server and invoke a callback when we get the response.
    #sendRequest(request: proto.Request, callbacks: ResponseCallbacks) {
        const requestId = this.#requestIdAlloc.alloc();
        this.#responseMap.set(requestId, {...callbacks, type: request.type});
        this.#send({"type": "request", "request_id": requestId, request});
    }

    // The socket encountered an error.
    #onSocketError(event: Event | WebSocket.ErrorEvent): void {
        const eventMessage = (event as {message?: string}).message;
        const message = eventMessage ?? "Connection was closed due to an error";
        this.#setClosed(new Error(message));
    }

    // The socket was closed.
    #onSocketClose(event: WebSocket.CloseEvent): void {
        this.#setClosed(new Error(`WebSocket was closed with code ${event.code}: ${event.reason}`));
    }

    // Close the client with the given error.
    #setClosed(error: Error): void {
        if (this.#closed !== undefined) {
            return;
        }
        this.#closed = error;

        for (const [requestId, responseState] of this.#responseMap.entries()) {
            responseState.errorCallback(error);
            this.#requestIdAlloc.free(requestId);
        }
        this.#responseMap.clear();

        this.#socket.close();
    }

    // We received a message from the socket.
    #onSocketMessage(event: WebSocket.MessageEvent): void {
        if (typeof event.data !== "string") {
            this.#socket.close(3003, "Only string messages are accepted");
            this.#setClosed(new Error("Received non-string message from server"))
            return;
        }

        try {
            this.#handleMsg(event.data);
        } catch (e) {
            this.#socket.close(3007, "Could not handle message");
            this.#setClosed(e as Error);
        }
    }

    // Handle a message from the server.
    #handleMsg(msgText: string): void {
        const msg = JSON.parse(msgText) as proto.ServerMsg;

        if (msg["type"] === "hello_ok" || msg["type"] === "hello_error") {
            if (this.#recvdHello) {
                throw new Error("Received a duplicated hello response");
            }
            this.#recvdHello = true;

            if (msg["type"] === "hello_error") {
                throw errorFromProto(msg["error"]);
            }
            return;
        } else if (!this.#recvdHello) {
            throw new Error("Received a non-hello message before a hello response");
        }

        if (msg["type"] === "response_ok") {
            const requestId = msg["request_id"];
            const responseState = this.#responseMap.get(requestId);
            this.#responseMap.delete(requestId);

            if (responseState === undefined) {
                throw new Error("Received unexpected OK response");
            } else if (responseState.type !== msg["response"]["type"]) {
                throw new Error("Received unexpected type of response");
            }

            try {
                responseState.responseCallback(msg["response"]);
            } catch (e) {
                responseState.errorCallback(e as Error);
                throw e;
            }
        } else if (msg["type"] === "response_error") {
            const requestId = msg["request_id"];
            const responseState = this.#responseMap.get(requestId);
            this.#responseMap.delete(requestId);

            if (responseState === undefined) {
                throw new Error("Received unexpected error response");
            }
            responseState.errorCallback(errorFromProto(msg["error"]));
        } else {
            throw new Error("Received unexpected message type");
        }
    }

    /** Open a {@link Stream}, a stream for executing SQL statements. */
    openStream(): Stream {
        if (this.#closed !== undefined) {
            throw new Error("Client is closed", {cause: this.#closed});
        }

        const streamId = this.#streamIdAlloc.alloc();
        const streamState = {
            streamId,
            closed: undefined,
        };

        const responseCallback = () => undefined;
        const errorCallback = (e: Error) => this._closeStream(streamState, e);

        const request: proto.OpenStreamReq = {
            "type": "open_stream",
            "stream_id": streamId,
        };
        this.#sendRequest(request, {responseCallback, errorCallback});

        return new Stream(this, streamState);
    }

    // Make sure that the stream is closed.
    /** @private */
    _closeStream(streamState: StreamState, error: Error): void {
        if (streamState.closed !== undefined || this.#closed !== undefined) {
            return;
        }
        streamState.closed = error;

        const callback = () => {
            this.#streamIdAlloc.free(streamState.streamId);
        };
        const request: proto.CloseStreamReq = {
            "type": "close_stream",
            "stream_id": streamState.streamId,
        };
        this.#sendRequest(request, {responseCallback: callback, errorCallback: callback});
    }

    // Execute a statement on a stream and invoke callbacks in `stmtState` when we get the results (or an
    // error).
    /** @private */
    _execute(streamState: StreamState, stmtState: StmtState): void {
        const responseCallback = (response: proto.Response) => {
            stmtState.resultCallback((response as proto.ExecuteResp)["result"]);
        };
        const errorCallback = (error: Error) => {
            stmtState.errorCallback(error);
        }

        if (streamState.closed !== undefined) {
            errorCallback(new Error("Stream was closed", {cause: streamState.closed}));
            return;
        } else if (this.#closed !== undefined) {
            errorCallback(new Error("Client was closed", {cause: this.#closed}));
            return;
        }

        const request: proto.ExecuteReq = {
            "type": "execute",
            "stream_id": streamState.streamId,
            "stmt": stmtState.stmt,
        };
        this.#sendRequest(request, {responseCallback, errorCallback});
    }

    /** Close the client and the WebSocket. */
    close() {
        this.#setClosed(new Error("Client was manually closed"));
    }
}

interface ResponseCallbacks {
    responseCallback: (_: proto.Response) => void;
    errorCallback: (_: Error) => void;
}

interface ResponseState extends ResponseCallbacks {
    type: string;
}

interface StmtState {
    stmt: proto.Stmt;
    resultCallback: (_: proto.StmtResult) => void;
    errorCallback: (_: Error) => void;
}

interface StreamState {
    streamId: number;
    closed: Error | undefined;
}

/** A stream for executing SQL statements (a "database connection"). */
export class Stream {
    #client: Client;
    #state: StreamState;

    /** @private */
    constructor(client: Client, state: StreamState) {
        this.#client = client;
        this.#state = state;
    }

    /** Execute a raw Hrana statement. */
    executeRaw(stmt: proto.Stmt): Promise<proto.StmtResult> {
        return new Promise((resultCallback, errorCallback) => {
            this.#client._execute(this.#state, {stmt, resultCallback, errorCallback});
        });
    }

    /** Execute a statement that returns rows. */
    query(stmt: Stmt): Promise<RowArray> {
        return new Promise((rowsCallback, errorCallback) => {
            this.#client._execute(this.#state, {
                stmt: stmtToProto(stmt, true),
                resultCallback(result) {
                    rowsCallback(rowArrayFromProto(result))
                },
                errorCallback,
            });
        });
    }

    /** Execute a statement that returns at most a single row. */
    queryRow(stmt: Stmt): Promise<Row | undefined> {
        return new Promise((rowCallback, errorCallback) => {
            this.#client._execute(this.#state, {
                stmt: stmtToProto(stmt, true),
                resultCallback(result) {
                    if (result.rows.length >= 1) {
                        rowCallback(rowFromProto(result, result.rows[0]));
                    } else {
                        rowCallback(undefined);
                    }
                },
                errorCallback,
            });
        });
    }

    /** Execute a statement that returns at most a single value. */
    queryValue(stmt: Stmt): Promise<Value | undefined> {
        return new Promise((valueCallback, errorCallback) => {
            this.#client._execute(this.#state, {
                stmt: stmtToProto(stmt, true),
                resultCallback(result) {
                    if (result.rows.length >= 1 && result.rows[0].length >= 1) {
                        valueCallback(valueFromProto(result.rows[0][0]));
                    } else {
                        valueCallback(undefined);
                    }
                },
                errorCallback,
            });
        });
    }

    /** Execute a statement that does not return rows. */
    execute(stmt: Stmt): Promise<StmtResult> {
        return new Promise((doneCallback, errorCallback) => {
            this.#client._execute(this.#state, {
                stmt: stmtToProto(stmt, false),
                resultCallback(result) { doneCallback(stmtResultFromProto(result)); },
                errorCallback,
            });
        });
    }

    /** Close the stream. */
    close(): void {
        this.#client._closeStream(this.#state, new Error("Stream was manually closed"));
    }
}

