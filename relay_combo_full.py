import asyncio
import json
import logging
import time
import random
import websockets
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Streamlit page setup
st.set_page_config(page_title="WebSocket Relay Server & Clients", layout="wide")

# -------------------------
# Globals
# -------------------------
clients = {}
log_messages = []
latency_records = []
throughput_records = []

def log(msg):
    log_messages.append(msg)
    st.session_state.logs_area = "\n".join(log_messages)
    st.experimental_rerun()

# -------------------------
# WebSocket Relay Handler
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

        peers = [k for k in clients.keys() if k != client_id]
        await ws.send(json.dumps({"type":"peers","peers":peers}))

        async for message in ws:
            try:
                msg = json.loads(message)
            except:
                log(f"[WARN] Non-JSON message from {client_id}")
                continue

            to = msg.get("to")
            start_time = time.time()
            if not to:
                for cid, peer_ws in clients.items():
                    if cid == client_id:
                        continue
                    try:
                        await peer_ws.send(json.dumps({"from": client_id, "payload": msg.get("payload"), "msg_id": msg.get("msg_id")}))
                        latency_records.append(time.time() - start_time)
                        throughput_records.append(len(msg.get("payload")))
                        log(f"[BROADCAST] {client_id} → {cid}: {msg.get('payload')}")
                    except:
                        log(f"[ERROR] Failed sending to {cid}")
                continue

            peer = clients.get(to)
            if peer:
                try:
                    await peer.send(json.dumps({"from": client_id, "payload": msg.get("payload"), "msg_id": msg.get("msg_id")}))
                    latency_records.append(time.time() - start_time)
                    throughput_records.append(len(msg.get("payload")))
                    log(f"[SEND] {client_id} → {to}: {msg.get('payload')}")
                except:
                    log(f"[ERROR] Failed sending to {to}")
            else:
                await ws.send(json.dumps({"type":"error","message":f"peer {to} not connected"}))
                log(f"[ERROR] {client_id} → disconnected peer {to}")

    except asyncio.TimeoutError:
        log(f"[TIMEOUT] Handshake timeout for client")
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
# Server starter
# -------------------------
async def start_server():
    server = await websockets.serve(handler, "0.0.0.0", 8765)
    log("[SYSTEM] Relay server started at ws://0.0.0.0:8765")
    await server.wait_closed()

# -------------------------
# Client simulator
# -------------------------
async def run_client(client_id, target, payload, duration):
    uri = "ws://127.0.0.1:8765"
    try:
        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps({"type":"hello","id":client_id}))
            log(f"[CLIENT] {client_id} connected → targeting {target}")
            count = 1
            start_time = time.time()
            while time.time() - start_time < duration:
                msg = {"to": target, "payload": payload, "msg_id": count}
                send_start = time.time()
                await ws.send(json.dumps(msg))
                latency_records.append(time.time() - send_start)
                throughput_records.append(len(payload))
                log(f"[CLIENT] {client_id} sent: {payload}")
                await asyncio.sleep(1)
                count += 1
    except Exception as e:
        log(f"[CLIENT ERROR] {client_id}: {e}")

# -------------------------
# Streamlit UI
# -------------------------
if "logs_area" not in st.session_state:
    st.session_state.logs_area = ""

st.title("WebSocket Relay Server with Clients GUI")

col1, col2 = st.columns([1,2])

with col1:
    st.header("Server Control")
    if st.button("Start Server"):
        asyncio.create_task(start_server())
        st.success("Server starting in background...")

    st.header("Run Clients")
    client_name = st.text_input("Client Name", "clientA")
    target_client = st.text_input("Target Client", "clientB")
    payload = st.text_area("Payload", "Hello World!")
    duration = st.number_input("Duration (seconds)", 10, 600, 30)

    if st.button("Run Client"):
        asyncio.create_task(run_client(client_name, target_client, payload, duration))
        st.success(f"Client {client_name} started → targeting {target_client}")

with col2:
    st.header("Logs")
    st.text_area("Server & Client Logs", value=st.session_state.logs_area, height=400)

    if st.button("Show Latency Histogram"):
        if latency_records:
            df = pd.DataFrame({"latency": latency_records})
            fig = px.histogram(df, x="latency", nbins=30, title="Latency Histogram (seconds)")
            st.plotly_chart(fig)
        else:
            st.warning("No latency records yet")

    if st.button("Show Throughput"):
        if throughput_records:
            df = pd.DataFrame({"throughput": throughput_records})
            fig = px.line(df, y="throughput", title="Throughput over time (bytes per message)")
            st.plotly_chart(fig)
        else:
            st.warning("No throughput records yet")