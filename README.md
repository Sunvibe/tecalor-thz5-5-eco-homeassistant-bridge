# Tecalor THZ 5.5 Eco → Home Assistant (Rewrite)

## Status

This repository is currently being **reworked**.

The previous implementation has been moved to:
`ser2net-legacy/`

---

## 🔴 Legacy

The `ser2net-legacy/` folder contains the old architecture:

```
THZ → Raspberry Pi (ser2net) → AppDaemon → MQTT → Home Assistant
```

While functional, this setup has several downsides:

* multiple components and dependencies
* difficult to debug
* hard to maintain and extend

---

## 🟢 New Approach

The goal is a **simpler and cleaner architecture**:

```
THZ → ESP (ESPHome usb_uart + serial_proxy) → Python (serialx) → Home Assistant
```

Core idea:

* the ESP acts only as a serial bridge (no protocol logic)
* protocol handling stays in Python
* direct integration into Home Assistant (no MQTT/AppDaemon)

---

## 🎯 Goals

* fewer moving parts
* easier setup for users
* improved maintainability
* solid foundation for a proper Home Assistant integration

---

## 🚧 Current State

Work in progress.

Current focus:

* ESP + `serialx` communication
* rebuilding the protocol layer

---

## ℹ️ Note

The legacy implementation remains fully available in
`ser2net-legacy/` and can be used as a reference.

---
