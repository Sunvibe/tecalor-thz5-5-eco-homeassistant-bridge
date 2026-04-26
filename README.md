# tecalor-thz5-5-eco-homeassistant-bridge
Integration of the Tecalor THZ 5.5 Eco heat pump with Home Assistant using MQTT, AppDaemon, and ser2net on a Raspberry Pi.

# Tecalor THZ 5.5 Eco MQTT Bridge

Integration of the Tecalor THZ 5.5 Eco heat pump with Home Assistant using MQTT, AppDaemon, and ser2net on a Raspberry Pi.

---

## 🧩 Architecture Overview

The system connects the THZ heat pump via USB to a Raspberry Pi and exposes its data to Home Assistant through MQTT.

### Data Flow
```
THZ 5.5 Eco (USB-B)
↓
Raspberry Pi (ser2net service)
↓
AppDaemon (data decoding)
↓
MQTT Broker
↓
Home Assistant (MQTT Device)
```

---

## 🔧 Components

### 1. THZ 5.5 Eco → Raspberry Pi
- The heat pump is connected via **USB-B interface**
- Appears as a serial device on the Raspberry Pi

---

### 2. Raspberry Pi running ser2net
- ser2net runs **as a system service (not in Docker)**
- Exposes the serial interface over TCP/IP
- Acts as a bridge between hardware and network clients
- Only the connection `connection: &conthz` (Port **3334**) in `ser2net.yaml` is relevant for this project

---

### 3. AppDaemon Interface Layer
- AppDaemon connects to the ser2net service
- Reads raw serial data from the THZ system
- Decodes proprietary protocol data
- Transforms data into structured values

> ⚠️ Note:  
> The script `thz505eco_ser2net2mqtt.py` is **not well structured / messy**.  
> It was written while learning and under time constraints – improvements and refactoring are welcome.

---

### 4. MQTT Publishing
- Decoded values are published via MQTT
- Enables loose coupling between data source and consumers

---

### 5. Home Assistant Integration
- Home Assistant detects the THZ as an **MQTT device**
- Sensors and values are automatically available
- No direct hardware integration required

---

## ⚙️ AppDaemon Configuration

- The file `apps.yaml` contains the configuration to run the script
- It defines how `thz505eco_ser2net2mqtt.py` is started within AppDaemon

---

## 🔁 Replacement for ISG Gateway

This setup can replace the official **ISG (Internet Service Gateway)** for **basic read access** to the heat pump.

- Provides local access to operational data
- Removes dependency on vendor cloud services
- Enables a fully **local-only setup (no cloud required)**

> ⚠️ Note:  
> This project focuses on **reading data**. Write/control capabilities are not the primary goal.

---

## 🚀 Key Advantages

- ✅ No direct Home Assistant hardware dependency  
- ✅ Clean separation of concerns (hardware / decoding / automation)  
- ✅ Network-based access to THZ data  
- ✅ Easily extendable and debuggable  
- ✅ Works with standard IoT tools (MQTT)  
- ✅ Can replace the ISG gateway for local monitoring  
- ✅ No cloud dependency

---

## ⚠️ Notes

- This is **not a native Home Assistant integration**
- Communication is handled via a **serial-to-network bridge**
- Requires basic setup of MQTT and AppDaemon

---

## 📖 Background

This project was created due to **major issues with the heat pump during the early installation phase**.

Even though technicians repeatedly stated *"the heat pumps are actually great"*, the reality required deeper insight and monitoring — which ultimately led to building this solution.

---

## 📌 Summary

This project provides a modular and network-based approach to integrate the Tecalor THZ 5.5 Eco into Home Assistant by using a Raspberry Pi as a bridge and MQTT as the communication layer — fully local and independent from cloud services.