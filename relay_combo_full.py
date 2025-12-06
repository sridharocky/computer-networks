# relay_server_streamlit.py
import asyncio
import json
import logging
import websockets
import streamlit as st
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
st.set_page_config(page_title="WebSocket Relay Server", layout="wide")

# -------------------------
# Globals
# -------------------------
clients = {}
log_messages = []

def log(message):
    log_messages.append(message)
    st.session_state.logs_area = "\n".join(log_messages)
    st.experimental_rerun()

# -------------------------
# Relay Handler
# -------------------------
async def handler(ws, path):
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        obj = json.loads(raw)
        if obj.get("type") != "hello" or "id" not in obj:
            await ws.send(json.dumps({"type":"error","message":"expected hello with id"}))
            await ws.close()
            return
        client_id = obj["id"]
        clients[client_id] = ws
        log(f"[CONNECTED] {client_id}")

        # inform about peers
        peers = [k for k in clients.keys() if k != client_id]
        await ws.send(json.dumps({"type":"peers","peers":peers}))

        async for message in ws:
            try:
                msg = json.loads(message)
            except Exception:
                log(f"[WARN] Non-json message from {client_id}: {message[:80]}")
                continue

            to = msg.get("to")
            if not to:
                for cid, peer_ws in list(clients.items()):
                    if cid == client_id:
                        continue
                    try:
                        await peer_ws.send(json.dumps({"from": client_id, "payload": msg.get("payload"), "msg_id": msg.get("msg_id")}))
                        log(f"[BROADCAST] {client_id} → {cid}: {msg.get('payload')}")
                    except Exception as e:
                        log(f"[ERROR] Failed sending to {cid}: {e}")
                continue

            peer = clients.get(to)
            if peer:
                try:
                    await peer.send(json.dumps({"from": client_id, "payload": msg.get("payload"), "msg_id": msg.get("msg_id")}))
                    log(f"[SEND] {client_id} → {to}: {msg.get('payload')}")
                except (ConnectionClosedOK, ConnectionClosedError):
                    log(f"[INFO] Peer {to} disconnected while sending")
                except Exception as e:
                    log(f"[ERROR] {e}")
            else:
                await ws.send(json.dumps({"type":"error","message":f"peer {to} not connected"}))
                log(f"[ERROR] {client_id} tried to send to disconnected {to}")

    except asyncio.TimeoutError:
        log("[TIMEOUT] Handshake timeout")
        await ws.close()
    except Exception as e:
        log(f"[EXCEPTION] {e}")
    finally:
        for cid, peer_ws in list(clients.items()):
            if peer_ws is ws:
                del clients[cid]
                log(f"[DISCONNECTED] {cid}")
                break

# -------------------------
# Server Starter
# -------------------------
async def start_server():
    server = await websockets.serve(handler, "0.0.0.0", 8765)
    log("[SYSTEM] Relay server started at ws://0.0.0.0:8765")
    await server.wait_closed()

# -------------------------
# Client Simulator
# -------------------------
async def run_client(client_id, target, payload, duration):
    uri = "ws://127.0.0.1:8765"
    try:
        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps({"type":"hello","id":client_id}))
            log(f"[CLIENT] {client_id} connected, targeting {target}")

            import time
            start = time.time()
            count = 1
            while time.time() - start < duration:
                msg = {"to": target, "payload": payload, "msg_id": count}
                await ws.send(json.dumps(msg))
                log(f"[CLIENT] {client_id} sent: {msg}")
                await asyncio.sleep(1)  # 1 msg/sec
                count += 1
    except Exception as e:
        log(f"[CLIENT ERROR] {client_id}: {e}")

# -------------------------
# Streamlit UI
# -------------------------
st.title("WebSocket Relay Server")

if "logs_area" not in st.session_state:
    st.session_state.logs_area = ""

col1, col2 = st.columns([1,2])

with col1:
    st.header("Server Control")
    if st.button("Start Server"):
        asyncio.create_task(start_server())
        st.success("Server starting in background...")

    st.header("Clients Simulator")
    client_name = st.text_input("Client Name", "clientA")
    target_client = st.text_input("Target Client", "clientB")
    duration = st.number_input("Duration (seconds)", 10, 600, 60)
    payload = st.text_area("Payload", "Hello World!")

    if st.button("Run Client"):
        asyncio.create_task(run_client(client_name, target_client, payload, duration))
        st.success(f"Client {client_name} started targeting {target_client}")

with col2:
    st.header("Logs")
    st.text_area("Server & Client Logs", value=st.session_state.logs_area, height=400)
