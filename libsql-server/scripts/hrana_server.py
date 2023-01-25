import asyncio
import base64
import collections
import json
import sqlite3
import sys
import websockets

db_file = sys.argv[1]

async def main():
    server = await websockets.serve(handle_socket, "localhost", 2023, subprotocols=["hrana1"])
    for sock in server.sockets:
        print(f"Listening on {sock.getsockname()!r}")
    await server.wait_closed()

async def handle_socket(websocket):
    print(f"Accepted connection from {websocket.remote_address}")
    async def recv_msg():
        try:
            msg_str = await websocket.recv()
        except websockets.exceptions.ConnectionClosed:
            return None
        assert isinstance(msg_str, str)
        msg = json.loads(msg_str)
        return msg

    async def send_msg(msg):
        msg_str = json.dumps(msg)
        await websocket.send(msg_str)

    Stream = collections.namedtuple("Stream", ["conn", "lock"])
    streams = {}

    async def handle_request(req):
        if req["type"] == "open_stream":
            conn = await asyncio.to_thread(lambda: sqlite3.connect(db_file))
            streams[int(req["stream_id"])] = Stream(conn, asyncio.Lock())
            return {"type": "open_stream"}
        elif req["type"] == "close_stream":
            stream = streams.pop(int(req["stream_id"]), None)
            if stream is not None:
                async with stream.lock:
                    await asyncio.to_thread(lambda: stream.conn.close())
            return {"type": "close_stream"}
        elif req["type"] == "execute":
            stream = streams[int(req["stream_id"])]
            async with stream.lock:
                result = await asyncio.to_thread(lambda: execute_stmt(stream.conn, req["stmt"]))
            return {"type": "execute", "result": result}
        else:
            raise RuntimeError(f"Unknown req: {req!r}")

    def execute_stmt(conn, stmt):
        params = [value_to_sqlite(arg) for arg in stmt["args"]]
        cursor = conn.execute(stmt["sql"], params)
        cols = [{"name": name} for name, *_ in cursor.description]

        rows = []
        for row in cursor:
            if stmt["want_rows"]:
                rows.append([value_from_sqlite(val) for val in row])

        return {"cols": cols, "rows": rows, "affected_row_count": cursor.rowcount}

    def value_to_sqlite(value):
        if value["type"] == "null":
            return None
        elif value["type"] == "integer":
            return int(value["value"])
        elif value["type"] == "float":
            return float(value["value"])
        elif value["type"] == "text":
            return str(value["value"])
        elif value["type"] == "blob":
            return base64.b64decode(value["base64"])
        else:
            raise RuntimeError(f"Unknown value: {value!r}")

    def value_from_sqlite(value):
        if value is None:
            return {"type": "null"}
        elif isinstance(value, int):
            return {"type": "integer", "value": str(value)}
        elif isinstance(value, float):
            return {"type": "float", "value": value}
        elif isinstance(value, str):
            return {"type": "text", "value": value}
        elif isinstance(value, bytes):
            return {"type": "blob", "value": base64.b64encode(value)}
        else:
            raise RuntimeError(f"Unknown SQLite value: {value!r}")

    hello_msg = await recv_msg()
    assert hello_msg.get("type") == "hello"
    await send_msg({"type": "hello_ok"})

    request_tasks = set()
    while True:
        msg = await recv_msg()
        if msg is None:
            break
        elif msg["type"] == "request":
            response = await handle_request(msg["request"])
            await send_msg({
                "type": "response_ok",
                "request_id": msg["request_id"],
                "response": response,
            })
        else:
            raise RuntimeError(f"Unknown msg: {msg!r}")

asyncio.run(main())
