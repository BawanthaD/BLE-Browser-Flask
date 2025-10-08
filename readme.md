# BLE Browser (Flask + Bleak)

A modern, Windows-friendly web app to scan, connect, and interact with BLE devices using Python’s **Bleak**—with a sleek **Flask** UI, live notifications over WebSocket, a byte-by-byte write editor, and per-characteristic **computed values** saved to JSON.

> **Why this repo?** Keep your proven Bleak backend, but get a clean web UI you can run locally on any Windows machine with Bluetooth.

---

## Features

- **Scan / Connect / Disconnect** to BLE devices (Bleak).
- **List characteristics** with properties.
- **Read / Write** characteristics.
- **Subscribe / Unsubscribe** to notifications (live stream to browser via WebSocket).
- **Computed Values panel**
  - Define formulas per **characteristic UUID** (e.g., `temperature = (b1*256 + b0)/10`).
  - Evaluated on every notification.
  - Persisted in `configs/ble_<UUID>.json`.
- **Byte-by-byte Write Editor**
  - Paste hex → auto-creates editable 2-digit boxes (e.g., `04 00 00 64`).
  - Edit individual bytes and send.
- **Live Log & Raw Values** (hex) with autoscroll.
- **No external CSS framework** required; runs offline.

---

## Screenshots

> Replace with your own assets later.

- UI: `assets/screenshot.png`

---

## Quick Start

### Requirements

- **Windows 10/11** with Bluetooth.
- **Python 3.10+**
- A BLE device to test.

### Install

```bash
py -m pip install --upgrade pip
py -m pip install flask flask-sock simple-websocket bleak
