'''
# Heatpumpmonitor via MQTT (For Tecalor THZ 5.5 Eco [Stiebel Eltron])
* Setup
    * Raspberry Pi interfacing Diagnostic port via USB
    * Raspberry Pi is running set2net
    

#
# Thz Tecalor Monitor app
# -> https://github.com/rhuitl/openhab-addons/blob/cd3c9cd223e9d4922cf7732f10210ef8e7d208c7/bundles/org.openhab.binding.stiebelheatpump/src/main/resources/HeatpumpConfig/Tecalor_THZ55_7_62.xml
# Args:
#

# Changelog
# 2025-12-28: Complete rewrite as mqtt discovery program for ha
# 2025-10-26: Struggle with Percentage-Values... disabled most of them in code
# 2025-11-24: Added AppDaemon Workaround Patch

'''

import appdaemon.plugins.hass.hassapi as hass
import random
import json
import traceback
import time
import datetime
 
class Thz505ecoSer2Net2HaMqtt(hass.Hass):
    """
    MQTT Discovery Demo Template (AppDaemon) - with LEGACY unique_id mapping

    Goal:
    - Keep existing Home Assistant entities (entity_id, history, automations) by reusing the SAME unique_id.
    - New/extra entities can follow a new unique_id scheme, while legacy ones stay stable.

    Notes:
    - Home Assistant matches entities via `unique_id` (NOT entity_id).
    - `entity_id` cannot be set via MQTT discovery.
    """

    # =========================
    # CONFIG (edit here)
    # =========================
    DISCOVERY_PREFIX = "homeassistant"
    BASE_TOPIC = "demo"

    # This is the "device id" used in discovery topics.
    # Changing this is OK if unique_id stays the same.
    DEVICE_ID = "thz505eco"

    # Prefix for NEW (non-legacy) unique_ids
    UNIQUE_ID_PREFIX = "thz505eco_yolo_2025"

    # If you already have entities in HA, put their old unique_id values here.
    # These MUST match exactly, otherwise HA will create NEW entities.
    LEGACY_UNIQUE_IDS = {
        #"outside_temperature": "demo_outside_temperature",
        #"party_mode": "demo_party_mode",
        #"party_mode_last_cmd": "demo_party_mode_last_cmd",
    }

    DEVICE_INFO = {
        "name": "Heatpump",
        "manufacturer": "Stiebel Eltron (via AppDaemon)",
        "model": "Tecalor THZ 5.5 Eco (via Ser2Net-Diag-USB)",
    }

    PUBLISH_INTERVAL_SEC = 30

    # Switch topics (you can change these; unique_id is what keeps entity stable)
    PARTY_MODE_COMMAND_TOPIC_SUFFIX = "cmd/party_mode/set"
    PARTY_MODE_STATE_TOPIC_SUFFIX = "state/party_mode"

    # =========================
    # AppDaemon lifecycle
    # =========================
    def initialize(self):
        self.log("THZ5.5 Ser2Net2MqttHA init...")

        self.mqtt = self.get_plugin_api("mqtt")
        if not self.mqtt:
            self.log("MQTT plugin api is None - check mqtt plugin configuration")
            return

        if False:
            self.log("Script execution blocked in code!")
            return


        self.log("Delaying startup (will prompt a warning, only one time on startup)")
        time.sleep(3.0) # Wait a moment to prevent execution to early 


        # 1) Discovery publish (retained)
        self.publish_discovery()

        # 2) Availability online (retained)
        self.publish_availability(online=True)

        # 3) Listen for switch commands
        self.listen_event(
            self.on_party_mode_cmd,
            "MQTT_MESSAGE",
            topic=self.topic(self.PARTY_MODE_COMMAND_TOPIC_SUFFIX),
            namespace="mqtt",
        )

        # 4) Publish sensor periodically
        #self.run_every(self.publish_temperature, self.datetime(), self.PUBLISH_INTERVAL_SEC)

        # Optional: publish an initial state so HA has something immediately
        # (Uncomment if you want a default after restart)
        # self.publish_party_mode_state("OFF")

        # Register the task calls
        self.run_minutely(self.run_every_min, datetime.time(0, 0, 0))

        # Debug run -> Could produce peaks -> Remove when in production!
        self.run_in(self.cyclic_task_status, 0) 




    # =========================
    # Cyclic Tasks
    # =========================
    def run_every_min(self, kwargs):
        """ Register all runs for this minute """
        # Seperate the tasks into 5 minute segments
        current_minute = datetime.datetime.now().minute
        #self.log(current_minute)
        if (current_minute%2==0):
            self.run_in(self.cyclic_task_status, 0)
            self.run_in(self.cyclic_task_consumption, 15)
            self.run_in(self.cyclic_task_status, 30)
            self.run_in(self.cyclic_task_consumption, 45)  
        if (current_minute%2==1):
            self.run_in(self.cyclic_task_status, 0)
            self.run_in(self.cyclic_task_consumption, 15)
            self.run_in(self.cyclic_task_status, 30)
            self.run_in(self.cyclic_task_consumption, 45)  
        return


    def cyclic_task_status(self, kwargs):
        try:
            thz_device = TecalorThzSocket('192.168.64.101', 3334)
            thz_delay_between_commands = 0.3

            thz_device.connect()
            #self.log("Connected to device")
            time.sleep(thz_delay_between_commands)

            # Read global data
            for retry in range(0,3):
                data_sGlobal_raw = thz_device.get_raw_data(b'\xFB') # Global data
                data_sGlobal = thz_device.decode_raw_data(data_sGlobal_raw)

                # Show debug informationen
                self.show_sGlobal_data_debug(data_sGlobal, False)
                
                
                # When Length nok, retry
                time.sleep(thz_delay_between_commands)
                if (len(data_sGlobal) != 83):
                    continue
                
                # When length ok, parse the data
                data_sGlobal_parsed = thz_device.decode_global_data(data_sGlobal)
                break
            else:
                self.log(f"Too many failed receives for global data {len(data_sGlobal)}")
                return           

            # Read heat circuit 1 information
            for retry in range(0,3):
                data_sHC1_raw = thz_device.get_raw_data(b'\xF4') # Heating Circuit 1
                data_sHC1 = thz_device.decode_raw_data(data_sHC1_raw)       

                # Show debug informationen
                self.show_sHC1_data_debug(data_sGlobal, False)

                # When Length nok, retry
                time.sleep(thz_delay_between_commands)
                if (len(data_sHC1) != 46):
                    continue
                
                # When length ok, parse the data   
                data_sHC1_parsed = thz_device.decode_hc1_data(data_sHC1)
                break
            else:
                print(f"Too many failed receives for HC1 data {len(data_sHC1)}")
                return

            # Read domestic heat water information
            for retry in range(0,3):
                data_sDHW_raw = thz_device.get_raw_data(b'\xF3') # Domestic Heat Water
                data_sDHW = thz_device.decode_raw_data(data_sDHW_raw)  
                
                # Show debug informationen
                self.show_sDHW_data_debug(data_sDHW, False)

                # When Length nok, retry
                time.sleep(thz_delay_between_commands)
                if (len(data_sDHW) != 25):
                    continue
                
                # When length ok, parse the data   
                data_sDHW_parsed = thz_device.decode_dhw_data(data_sDHW)
                break
            else:
                print(f"Too many failed receives for DHW data {len(data_sDHW)}") 
                return

            thz_device.disconnect()

            # Publish sGlobal Data to mqtt
            self.publish_sGlobal_data(data_sGlobal_parsed=data_sGlobal_parsed)  
            
            # Publish sHC1 Data to mqtt
            self.publish_sHC1_data(data_sHC1_parsed=data_sHC1_parsed)  
            
            # Publish sHC1 Data to mqtt
            self.publish_sDHW_data(data_sDHW_parsed=data_sDHW_parsed)  
            

            #self.log(value)

        except Exception as e:
            self.log(traceback.format_exc())

    def cyclic_task_consumption(self, kwargs):
        #self.log("Consumption recording")
        try:
            
            thz_device = TecalorThzSocket('192.168.64.101', 3334)
            thz_delay_between_commands = 0.3

            thz_device.connect()
            time.sleep(thz_delay_between_commands)

            # Read total electric dhw usage
            for retry in range(0,3):
                data_sElectrDHWTotal1_raw = thz_device.get_raw_data(b'\x0A\x09\x1C')
                time.sleep(thz_delay_between_commands)
                data_sElectrDHWTotal2_raw = thz_device.get_raw_data(b'\x0A\x09\x1D')
                data_sElectrDHWTotal1 = thz_device.decode_raw_data(data_sElectrDHWTotal1_raw)
                data_sElectrDHWTotal2 = thz_device.decode_raw_data(data_sElectrDHWTotal2_raw)
            
                # Show debug informationen

                if False:
                    data = data_sElectrDHWTotal1
                    #for i in range(0+24, len(data)-1-56, 1):
                    for i in range(0, len(data)-1, 1):
                        print(f'FlowRate Data: {i}:{i+2} = ', end="")
                        print(f'{(int.from_bytes(data[i:(i+2)], "big")/10.0)} / ', end="")
                        print("Flowrate?!", end="") if i == 6 else ""
                        print()
                        
                # When Length nok, retry
                time.sleep(thz_delay_between_commands)
                if (len(data_sElectrDHWTotal1) != 10):
                    continue
                if (len(data_sElectrDHWTotal2) != 10):
                    continue
                    
                # When length ok, parse the data   
                data_sElectrDHWTotal_parsed = {"electrDHWTotal": thz_device.decode_single_int_data(data_sElectrDHWTotal1)['value'] + thz_device.decode_single_int_data(data_sElectrDHWTotal2)['value'] * 1000.0}
                break
            else:
                self.log("Too many failed receives for ElectrDHWTotal1 or ElectrDHWTotal2 data") 
                return

            # Read total electric heat circuit usage
            for retry in range(0,3):
                data_sElectrHCTotal1_raw = thz_device.get_raw_data(b'\x0A\x09\x20')
                time.sleep(thz_delay_between_commands)
                data_sElectrHCTotal2_raw = thz_device.get_raw_data(b'\x0A\x09\x21')
                data_sElectrHCTotal1 = thz_device.decode_raw_data(data_sElectrHCTotal1_raw)
                data_sElectrHCTotal2 = thz_device.decode_raw_data(data_sElectrHCTotal2_raw)
            
                # Show debug informationen
                if False:
                    data = data_sElectrHCTotal1
                    #for i in range(0+24, len(data)-1-56, 1):
                    for i in range(0, len(data)-1, 1):
                        print(f'FlowRate Data: {i}:{i+2} = ', end="")
                        print(f'{(int.from_bytes(data[i:(i+2)], "big")/10.0)} / ', end="")
                        print("Flowrate?!", end="") if i == 6 else ""
                        print()
                        
                # When Length nok, retry
                time.sleep(thz_delay_between_commands)
                if (len(data_sElectrHCTotal1) != 10):
                    continue
                if (len(data_sElectrHCTotal2) != 10):
                    continue
                    
                # When length ok, parse the data   
                data_sElectrHCTotal_parsed = {"electrHCTotal": thz_device.decode_single_int_data(data_sElectrHCTotal1)['value'] + thz_device.decode_single_int_data(data_sElectrHCTotal2)['value'] * 1000.0}
                break
            else:
                self.log("Too many failed receives for ElectrHCTotal1 or ElectrHCTotal2 data") 
                return
            
            # Read day electric dhw usage
            for retry in range(0,3):
                data_sElectrDHWDay1_raw = thz_device.get_raw_data(b'\x0A\x09\x1A')
                time.sleep(thz_delay_between_commands)
                data_sElectrDHWDay2_raw = thz_device.get_raw_data(b'\x0A\x09\x1B')
                data_sElectrDHWDay1 = thz_device.decode_raw_data(data_sElectrDHWDay1_raw)
                data_sElectrDHWDay2 = thz_device.decode_raw_data(data_sElectrDHWDay2_raw)
            
                # Show debug informationen

                if False:
                    data = data_sElectrDHWDay1
                    #for i in range(0+24, len(data)-1-56, 1):
                    for i in range(0, len(data)-1, 1):
                        print(f'FlowRate Data: {i}:{i+2} = ', end="")
                        print(f'{(int.from_bytes(data[i:(i+2)], "big")/10.0)} / ', end="")
                        print("Flowrate?!", end="") if i == 6 else ""
                        print()
                        
                # When Length nok, retry
                time.sleep(thz_delay_between_commands)
                if (len(data_sElectrDHWDay1) != 10):
                    continue
                if (len(data_sElectrDHWDay2) != 10):
                    continue
                    
                # When length ok, parse the data   
                data_sElectrDHWDay_parsed = {"electrDHWDay": thz_device.decode_single_int_data(data_sElectrDHWDay1)['value'] + thz_device.decode_single_int_data(data_sElectrDHWDay2)['value'] * 1000.0}
                break
            else:
                self.log("Too many failed receives for ElectrDHWDay1 or ElectrDHWDay2 data") 
                return
            
        
            # Read day electric heat circuit usage
            for retry in range(0,3):
                data_sElectrHCDay1_raw = thz_device.get_raw_data(b'\x0A\x09\x1E')
                time.sleep(thz_delay_between_commands)
                data_sElectrHCDay2_raw = thz_device.get_raw_data(b'\x0A\x09\x1F')
                data_sElectrHCDay1 = thz_device.decode_raw_data(data_sElectrHCDay1_raw)
                data_sElectrHCDay2 = thz_device.decode_raw_data(data_sElectrHCDay2_raw)
            
                # Show debug informationen
                if False:
                    data = data_sElectrHCDay1
                    #for i in range(0+24, len(data)-1-56, 1):
                    for i in range(0, len(data)-1, 1):
                        print(f'FlowRate Data: {i}:{i+2} = ', end="")
                        print(f'{(int.from_bytes(data[i:(i+2)], "big")/10.0)} / ', end="")
                        print("Flowrate?!", end="") if i == 6 else ""
                        print()
                        
                # When Length nok, retry
                time.sleep(thz_delay_between_commands)
                if (len(data_sElectrHCDay1) != 10):
                    continue
                if (len(data_sElectrHCDay2) != 10):
                    continue
                    
                # When length ok, parse the data   
                data_sElectrHCDay_parsed = {"electrHCDay": thz_device.decode_single_int_data(data_sElectrHCDay1)['value'] + thz_device.decode_single_int_data(data_sElectrHCDay2)['value'] * 1000.0}
                break
            else:
                self.log("Too many failed receives for ElectrHCDay1 or ElectrHCDay2 data") 
                return
        

            thz_device.disconnect()

            # Publish Consumption Data to mqtt
            self.publish_consumption_data(
                dhw_total=data_sElectrDHWTotal_parsed,
                hc_total=data_sElectrHCTotal_parsed,
                dhw_day=data_sElectrDHWDay_parsed,
                hc_day=data_sElectrHCDay_parsed,
            )

        except Exception as e:
            self.log(traceback.format_exc())

    # ========================= 
    # DECODING HELPERS
    # =========================
    def show_sGlobal_data_debug(self, data_sGlobal, show):
        if show is not True:
            return 

        data = data_sGlobal

        for i in range(0, len(data) - 1):
            parts = []

            parts.append(f"Global Data: {i}:{i+2} = ")
            parts.append(f"{(int.from_bytes(data[i:(i+2)], 'big') / 10.0)} / ")

            # Labels
            if i == 6:  parts.append("Außentemp!")
            if i == 8:  parts.append("Vorlauftemp!")
            if i == 10: parts.append("Ruecklauftemp!")
            if i == 12: parts.append("hotGas")
            if i == 14: parts.append("WW-Wasser!")
            if i == 16: parts.append("flowTempHC2")
            if i == 20: parts.append("Verdampfertemperatur!")
            if i == 22: parts.append("Verfluessigertemperatur!")

            # Bit/Byte Dumps
            if i == 24:
                parts.append(f"BitData {data[24:25].hex()} ")
                parts.append(f"BytePattern {data[24:27]} ")
            if i == 25:
                parts.append(f"BitData {data[25:26].hex()} ")
            if i == 26:
                parts.append(f"BitData {data[26:27].hex()} ")

            if i == 40:
                parts.append(f"BitData {data[40:41].hex()} {data[41:42].hex()}")
            if i == 39:
                parts.append(f"BitData {data[39:40].hex()} {data[40:41].hex()}")
                parts.append("10.9 Static?!")

            if i == 45: parts.append("Niederdruck!")
            if i == 47: parts.append("Hochdruck!")
            if i == 57: parts.append("Flowrate 0.01!")

            # eine Log-Zeile pro i
            self.log("".join(parts))

    def show_sHC1_data_debug(self, data_sHC1, show):
        if show is not True:
            return 

        data = data_sHC1
        for i in range(0, len(data)-1, 1):
            parts = []
            parts.append(f'HC1 Data: {i}:{i+2} = ')
            parts.append(f'{(int.from_bytes(data[i:(i+2)], "big")/10.0)} / ')
            if i == 8: parts.append("Return Temp  HK1?!")
            if i == 10: parts.append("Integral Heat HK1?!")
            if i == 12: parts.append("FlowTemp HK1?!")

            if i == 14: parts.append("Sollwert HK1?!")
            if i == 16: parts.append("Istwert HK1?!") 
            if i == 20: parts.append("Booster Stage HK1?!") 
            if i == 26: parts.append("opMode HK1?!") 

            if i == 30: parts.append("Room Setpoint HK1?!")
            if i == 36: parts.append("Inside Temp RC  HK1?!") 
            self.log("".join(parts))

    def show_sDHW_data_debug(self, data_sDHW, show):
        if show is not True:
            return 

        data = data_sDHW
        for i in range(0, len(data)-1, 1):
            parts = []
            parts.append(f'DHW Data: {i}:{i+2} = ')
            parts.append(f'{(int.from_bytes(data[i:(i+2)], "big")/10.0)} / ')
            if i == 4: parts.append("WW Istwert?!") 
            if i == 6: parts.append("Außentemp?!") 
            if i == 8: parts.append("WW Sollwert?!") 
            if i == 10: parts.append("compBlockTime?!") 
            if i == 12: parts.append("out?!")
            if i == 14: parts.append("heatBlockTime?!")
            self.log("".join(parts))

# ========================= 
    # Helpers
    # =========================
    def topic(self, suffix: str) -> str:
        """Build <BASE_TOPIC>/<suffix>"""
        suffix = suffix.lstrip("/")
        return f"{self.BASE_TOPIC}/{suffix}"

    def discovery_topic(self, domain: str, object_id: str) -> str:
        """Build Home Assistant discovery config topic"""
        return f"{self.DISCOVERY_PREFIX}/{domain}/{self.DEVICE_ID}/{object_id}/config"

    def uid(self, entity_key: str) -> str:
        """
        Unique ID resolver:
        - If entity_key is in LEGACY_UNIQUE_IDS -> return legacy ID (keeps existing entity)
        - Else -> return new stable scheme
        """
        legacy = self.LEGACY_UNIQUE_IDS.get(entity_key)
        if legacy:
            return legacy
        return f"{self.UNIQUE_ID_PREFIX}_{self.DEVICE_ID}_{entity_key}"

    def device_block(self) -> dict:
        """MQTT discovery device object"""
        return {
            "identifiers": [self.DEVICE_ID],
            "name": self.DEVICE_INFO["name"],
            "manufacturer": self.DEVICE_INFO.get("manufacturer", ""),
            "model": self.DEVICE_INFO.get("model", ""),
        }

    def publish_json(self, topic: str, payload_obj: dict, retain: bool = True, qos: int = 0):
        self.mqtt.mqtt_publish(topic, json.dumps(payload_obj), qos=qos, retain=retain)

    def publish_availability(self, online: bool):
        self.mqtt.mqtt_publish(
            self.topic("status"),
            "online" if online else "offline",
            qos=0,
            retain=True,
        )

    # =========================
    # MQTT Discovery
    # =========================
    def publish_discovery(self):
        device = self.device_block()
        availability = {
            "availability_topic": self.topic("status"),
            "payload_available": "online",
            "payload_not_available": "offline",
        }

        """
        # --- Temperature Sensor (LEGACY unique_id if present)
        self.publish_json(
            self.discovery_topic("sensor", "demo_outside_temperature"),
            {
                "name": "Demo - Outside Temperature",
                "unique_id": self.uid("demo_outside_temperature"),
                "state_topic": self.topic("sensor/outside_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )
        
        # --- Party Mode Switch (LEGACY unique_id if present)
        self.publish_json(
            self.discovery_topic("switch", "demo_party_mode"),
            {
                "name": "Demo - Party Mode",
                "unique_id": self.uid("demo_party_mode"),
                "command_topic": self.topic(self.PARTY_MODE_COMMAND_TOPIC_SUFFIX),
                "state_topic": self.topic(self.PARTY_MODE_STATE_TOPIC_SUFFIX),
                "payload_on": "ON",
                "payload_off": "OFF",
                "state_on": "ON",
                "state_off": "OFF",
                "retain": True,
                "device": device,
                **availability,
            },
            retain=True,
        )

        # --- Diagnostic Sensor: last command (LEGACY unique_id if present)
        self.publish_json(
            self.discovery_topic("sensor", "demo_party_mode_last_cmd"),
            {
                "name": "Party Mode – Last Command",
                "unique_id": self.uid("demo_party_mode_last_cmd"),
                "state_topic": self.topic("diagnostic/party_mode_last_cmd"),
                "icon": "mdi:gesture-tap",
                "entity_category": "diagnostic",
                "device": device,
                **availability,
            },
            retain=True,
        )
         

        """


        self.publish_json(
            self.discovery_topic("sensor", "thz_global_outside_temperature"),
            {
                "name": "THZ - Outside temperature",
                "unique_id": self.uid("global_outside_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_outside_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_flow_temperature"),
            {
                "name": "THZ - Flow temperature",
                "unique_id": self.uid("global_flow_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_flow_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_return_temperature"),
            {
                "name": "THZ - Return temperature",
                "unique_id": self.uid("global_return_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_return_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_hotgas_temperature"),
            {
                "name": "THZ - Hotgas temperature",
                "unique_id": self.uid("global_hotgas_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_hotgas_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )


        self.publish_json(
            self.discovery_topic("sensor", "thz_global_dhw_temperature"),
            {
                "name": "THZ - DHW temperature",
                "unique_id": self.uid("global_dhw_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_dhw_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_flow_hc2_temperature"),
            {
                "name": "THZ - Flow HC2 temperature",
                "unique_id": self.uid("global_flow_hc2_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_flow_hc2_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_evaporator_temperature"),
            {
                "name": "THZ - Evaporator temperature",
                "unique_id": self.uid("global_evaporator_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_evaporator_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_condenser_temperature"),
            {
                "name": "THZ - Condenser temperature",
                "unique_id": self.uid("global_condenser_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_condenser_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_output_ventilator_power"),
            {
                "name": "THZ - Output ventilator power",
                "unique_id": self.uid("global_output_ventilator_power"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_output_ventilator_power"),
                "unit_of_measurement": "%",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_input_ventilator_power"),
            {
                "name": "THZ - Input ventilator power",
                "unique_id": self.uid("global_input_ventilator_power"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_input_ventilator_power"),
                "unit_of_measurement": "%",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_main_ventilator_power"),
            {
                "name": "THZ - Main ventilator power",
                "unique_id": self.uid("global_main_ventilator_power"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_main_ventilator_power"),
                "unit_of_measurement": "%",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )
        
        self.publish_json(
            self.discovery_topic("sensor", "thz_global_output_ventilator_speed"),
            {
                "name": "THZ - Output Ventilator Frequency",
                "unique_id": self.uid("output_ventilator_speed"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_output_ventilator_speed"),
                "unit_of_measurement": "Hz",
                "device_class": "frequency",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_input_ventilator_speed"),
            {
                "name": "THZ - Input Ventilator Frequency",
                "unique_id": self.uid("input_ventilator_speed"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_input_ventilator_speed"),
                "unit_of_measurement": "Hz",
                "device_class": "frequency",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_main_ventilator_speed"),
            {
                "name": "THZ - Main Ventilator Frequency",
                "unique_id": self.uid("main_ventilator_speed"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_main_ventilator_speed"),
                "unit_of_measurement": "Hz",
                "device_class": "frequency",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_outside_temperature_filtered"),
            {
                "name": "THZ - Outside temperature (Filtered)",
                "unique_id": self.uid("global_outside_temperature_filtered"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_outside_temperature_filtered"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )



        self.publish_json(
            self.discovery_topic("sensor", "thz_global_high_pressure"),
            {
                "name": "THZ - High pressure sensor",
                "unique_id": self.uid("global_high_pressure"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_high_pressure"),
                "unit_of_measurement": "bar",
                "device_class": "pressure",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_low_pressure"),
            {
                "name": "THZ - Low pressure sensor",
                "unique_id": self.uid("global_low_pressure"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_low_pressure"),
                "unit_of_measurement": "bar",
                "device_class": "pressure",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_global_flowrate"),
            {
                "name": "THZ - Flowrate",
                "unique_id": self.uid("global_flowrate"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_global_flowrate"),
                "unit_of_measurement": "L/min",
                "device_class": "volume_flow_rate",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )



        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_mixer_opened"),
            {
                "name": "THZ - Mixer opened",
                "unique_id": self.uid("global_mixer_opened"),
                "state_topic": self.topic("sensor/thz_global_mixer_opened"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "opening",
                "device": device,
                **availability,
            },
            retain=True,
        )

        

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_mixer_closed"),
            {
                "name": "THZ - Mixer closed",
                "unique_id": self.uid("global_mixer_closed"),
                "state_topic": self.topic("sensor/thz_global_mixer_closed"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_heatpipe_valve"),
            {
                "name": "THZ - Heatpipe",
                "unique_id": self.uid("global_heatpipe_valve"),
                "state_topic": self.topic("sensor/thz_global_heatpipe_valve"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_diverter_valve"),
            {
                "name": "THZ - Diverter Valve",
                "unique_id": self.uid("global_diverter_valve"),
                "state_topic": self.topic("sensor/thz_global_diverter_valve"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )
        

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_dhw_pump"),
            {
                "name": "THZ - DHW Pump",
                "unique_id": self.uid("global_dhw_pump"),
                "state_topic": self.topic("sensor/thz_global_dhw_pump"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_heating_circuit_pump"),
            {
                "name": "THZ - Heating circuit Pump",
                "unique_id": self.uid("global_heating_circuit_pump"),
                "state_topic": self.topic("sensor/thz_global_heating_circuit_pump"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_mixer_pump"),
            {
                "name": "THZ - Mixer Pump",
                "unique_id": self.uid("global_mixer_pump"),
                "state_topic": self.topic("sensor/thz_global_mixer_pump"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_solar_pump"),
            {
                "name": "THZ - Solar Pump",
                "unique_id": self.uid("global_solar_pump"),
                "state_topic": self.topic("sensor/thz_global_solar_pump"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_oil_sump_heater"),
            {
                "name": "THZ - Oil Sump Heater",
                "unique_id": self.uid("global_oil_sump_heater"),
                "state_topic": self.topic("sensor/thz_global_oil_sump_heater"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_bit_25_1"),
            {
                "name": "THZ - Bit 25.1",
                "unique_id": self.uid("global_bit_25_1"),
                "state_topic": self.topic("sensor/thz_global_bit_25_1"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_bit_25_2"),
            {
                "name": "THZ - Bit 25.2",
                "unique_id": self.uid("global_bit_25_2"),
                "state_topic": self.topic("sensor/thz_global_bit_25_2"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_compressor"),
            {
                "name": "THZ - Compressor",
                "unique_id": self.uid("global_compressor"),
                "state_topic": self.topic("sensor/thz_global_compressor"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_bit_25_4"),
            {
                "name": "THZ - Bit 25.4",
                "unique_id": self.uid("global_bit_25_4"),
                "state_topic": self.topic("sensor/thz_global_bit_25_4"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_bit_25_5"),
            {
                "name": "THZ - Bit 25.5",
                "unique_id": self.uid("global_bit_25_5"),
                "state_topic": self.topic("sensor/thz_global_bit_25_5"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_bit_25_6"),
            {
                "name": "THZ - Bit 25.6",
                "unique_id": self.uid("global_bit_25_6"),
                "state_topic": self.topic("sensor/thz_global_bit_25_6"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_bit_25_7"),
            {
                "name": "THZ - Bit 25.7",
                "unique_id": self.uid("global_bit_25_7"),
                "state_topic": self.topic("sensor/thz_global_bit_25_7"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_hps_inverted"),
            {
                "name": "THZ - High Pressure Sensor Inverted",
                "unique_id": self.uid("global_hps_inverted"),
                "state_topic": self.topic("sensor/thz_global_hps_inverted"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_lps_inverted"),
            {
                "name": "THZ - Low Pressure Sensor Inverted",
                "unique_id": self.uid("global_lps_inverted"),
                "state_topic": self.topic("sensor/thz_global_lps_inverted"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_evaporator_ice_monitor"),
            {
                "name": "THZ - Evaporator Ice Monitor",
                "unique_id": self.uid("global_evaporator_ice_monitor"),
                "state_topic": self.topic("sensor/thz_global_evaporator_ice_monitor"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_signal_anode"),
            {
                "name": "THZ - Signal Anode",
                "unique_id": self.uid("global_signal_anode"),
                "state_topic": self.topic("sensor/thz_global_signal_anode"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_evu_release"),
            {
                "name": "THZ - Evu Release",
                "unique_id": self.uid("global_evu_release"),
                "state_topic": self.topic("sensor/thz_global_evu_release"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_oven_fireplace"),
            {
                "name": "THZ - Oven Fireplace",
                "unique_id": self.uid("global_oven_fireplace"),
                "state_topic": self.topic("sensor/thz_global_oven_fireplace"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_stb"),
            {
                "name": "THZ - STB",
                "unique_id": self.uid("global_stb"),
                "state_topic": self.topic("sensor/thz_global_stb"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("binary_sensor", "thz_global_bit_26_7"),
            {
                "name": "THZ - Bit 26.7",
                "unique_id": self.uid("global_bit_26_7"),
                "state_topic": self.topic("sensor/thz_global_bit_26_7"),
                "value_template": "{{ 'ON' if value == 'True' else 'OFF' }}",
                "payload_on": "ON",
                "payload_off": "OFF",
                #"device_class": "running",
                "device": device,
                **availability,
            },
            retain=True,
        )

        
        self.publish_json(
            self.discovery_topic("sensor", "thz_hc1_return_temperature"),
            {
                "name": "THZ - HC1 Return temperature",
                "unique_id": self.uid("hc1_return_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_hc1_return_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_hc1_flow_temperature"),
            {
                "name": "THZ - HC1 Flow temperature",
                "unique_id": self.uid("hc1_flow_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_hc1_flow_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_hc1_temperature_setpoint"),
            {
                "name": "THZ - HC1 temperature setpoint",
                "unique_id": self.uid("hc1_temperature_setpoint"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_hc1_temperature_setpoint"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_hc1_temperature"),
            {
                "name": "THZ - HC1 temperature",
                "unique_id": self.uid("hc1_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_hc1_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_hc1_room_temperature_setpoint"),
            {
                "name": "THZ - HC1 room temperature setpoint",
                "unique_id": self.uid("hc1_room_temperature_setpoint"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_hc1_room_temperature_setpoint"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_hc1_room_temperature_rc"),
            {
                "name": "THZ - HC1 Room temperature RC",
                "unique_id": self.uid("hc1_room_temperature_rc"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_hc1_room_temperature_rc"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_dhw_temperature_setpoint"),
            {
                "name": "THZ - DHW temperature setpoint",
                "unique_id": self.uid("dhw_temperature_setpoint"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_dhw_temperature_setpoint"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_dhw_temperature"),
            {
                "name": "THZ - DHW temperature",
                "unique_id": self.uid("dhw_temperature"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_dhw_temperature"),
                "unit_of_measurement": "°C",
                "device_class": "temperature",
                "state_class": "measurement",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_dhw_total_energy"),
            {
                "name": "THZ - DHW Total Energy Consumption",
                "unique_id": self.uid("dhw_total_energy"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_dhw_total_energy"),
                "unit_of_measurement": "kWh",
                "device_class": "energy",
                "state_class": "total_increasing",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_hc_total_energy"),
            {
                "name": "THZ - HC Total Energy Consumption",
                "unique_id": self.uid("hc_total_energy"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_hc_total_energy"),
                "unit_of_measurement": "kWh",
                "device_class": "energy",
                "state_class": "total_increasing",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_dhw_day_energy"),
            {
                "name": "THZ - DHW Day Energy Consumption",
                "unique_id": self.uid("dhw_day_energy"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_dhw_day_energy"),
                "unit_of_measurement": "kWh",
                "device_class": "energy",
                "state_class": "total",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )

        self.publish_json(
            self.discovery_topic("sensor", "thz_hc_day_energy"),
            {
                "name": "THZ - HC Day Energy Consumption",
                "unique_id": self.uid("hc_day_energy"),  # bleibt dein fixer key
                "state_topic": self.topic("sensor/thz_hc_day_energy"),
                "unit_of_measurement": "kWh",
                "device_class": "energy",
                "state_class": "total",
                "value_template": "{{ value | float }}",
                "device": device,
                **availability,
            },
            retain=True,
        )


        self.log("MQTT Discovery configs published (retained).")
        self.log(
            "Legacy unique_ids in use: "
            + ", ".join([f"{k}={v}" for k, v in self.LEGACY_UNIQUE_IDS.items()])
        )

    # =========================
    # Runtime publishing
    # =========================
    def publish_temperature(self, kwargs):
        value = round(10 + random.random() * 10, 2)
        self.mqtt.mqtt_publish(
            self.topic("sensor/outside_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

    def publish_sGlobal_data(self, **kwargs):
        #value = round(10 + random.random() * 10, 2)
        #self.log(f'sGlobal Publish Data Input {kwargs["data_sGlobal_parsed"]}')
        

        data_sGlobal_parsed = kwargs["data_sGlobal_parsed"]

         
        value = round(data_sGlobal_parsed['outsideTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_outside_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['flowTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_flow_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['returnTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_return_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['hotGasTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_hotgas_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['dhwTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_dhw_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['flowTempHC2'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_flow_hc2_temperature"),
            str(value),
            qos=0,
            retain=True,
        )


        value = round(data_sGlobal_parsed['evaporatorTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_evaporator_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['condenserTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_condenser_temperature"),
            str(value),
            qos=0,
            retain=True,
        )


        value = round(data_sGlobal_parsed['outputVentilatorPower'], 4) # in bar
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_output_ventilator_power"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['inputVentilatorPower'], 4) # in bar
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_input_ventilator_power"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['mainVentilatorPower'], 4) # in bar
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_main_ventilator_power"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['outputVentilatorSpeed'], 4) # in bar
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_output_ventilator_speed"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['inputVentilatorSpeed'], 4) # in bar
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_input_ventilator_speed"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['mainVentilatorSpeed'], 4) # in bar
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_main_ventilator_speed"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['outside_tempFiltered']/1.0, 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_outside_temperature_filtered"),
            str(value),
            qos=0,
            retain=True,
        )


        value = round(data_sGlobal_parsed['highPressureSensor']/100.0, 4) # in bar
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_high_pressure"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['lowPressureSensor']/100.0, 4) # in bar
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_low_pressure"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sGlobal_parsed['flowRate'], 4) # in l/min
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_flowrate"),
            str(value),
            qos=0,
            retain=True,
        )

        
        value = data_sGlobal_parsed['mixerOpen'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_mixer_opened"),
            str(value),
            qos=0,
            retain=True,
        )
        
        value = data_sGlobal_parsed['mixerClosed'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_mixer_closed"),
            str(value),
            qos=0,
            retain=True,
        )

        

        value = data_sGlobal_parsed['heatPipeValve'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_heatpipe_valve"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['diverterValve'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_diverter_valve"),
            str(value),
            qos=0,
            retain=True,
        )

        
        value = data_sGlobal_parsed['dhwPump'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_dhw_pump"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['heatingCircuitPump'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_heating_circuit_pump"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['mixerPumpOn'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_mixer_pump"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['solarPump'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_solar_pump"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['oilSumpHeaterOn'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_oil_sump_heater"),
            str(value),
            qos=0,
            retain=True,
        )

        
        value = data_sGlobal_parsed['Bit25_1'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_bit_25_1"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['Bit25_2'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_bit_25_2"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['compressorOn'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_compressor"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['Bit25_4'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_bit_25_4"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['Bit25_5'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_bit_25_5"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['Bit25_6'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_bit_25_6"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['Bit25_7'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_bit_25_7"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['highPressureSensorInverted'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_hps_inverted"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['lowPressureSensorInverted'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_lps_inverted"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['evaporatorIceMonitor'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_evaporator_ice_monitor"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['signalAnode'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_signal_anode"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['evuRelease'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_evu_release"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['ovenFireplace'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_oven_fireplace"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['STB'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_stb"),
            str(value),
            qos=0,
            retain=True,
        )

        value = data_sGlobal_parsed['Bit26_7'] # in True/False
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_global_bit_26_7"),
            str(value),
            qos=0,
            retain=True,
        )

    

    def publish_sHC1_data(self, **kwargs):
        #value = round(10 + random.random() * 10, 2)
        #self.log(f'sHC1 Publish Data Input {kwargs["data_sHC1_parsed"]}')
        data_sHC1_parsed = kwargs["data_sHC1_parsed"]

        value = round(data_sHC1_parsed['returnTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_hc1_return_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sHC1_parsed['flowTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_hc1_flow_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sHC1_parsed['heatSetTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_hc1_temperature_setpoint"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sHC1_parsed['heatTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_hc1_temperature"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sHC1_parsed['roomSetTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_hc1_room_temperature_setpoint"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sHC1_parsed['roomTempRC'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_hc1_room_temperature_rc"),
            str(value),
            qos=0,
            retain=True,
        )


    def publish_sDHW_data(self, **kwargs):
        #value = round(10 + random.random() * 10, 2)
        #self.log(f'sDHW Publish Data Input {kwargs["data_sDHW_parsed"]}')

        data_sDHW_parsed = kwargs["data_sDHW_parsed"]

        value = round(data_sDHW_parsed['dhwSetTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_dhw_temperature_setpoint"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sDHW_parsed['dhwTemp'], 4) # in °C
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_dhw_temperature"),
            str(value),
            qos=0,
            retain=True,
        )


    def publish_consumption_data(self, **kwargs):
        #value = round(10 + random.random() * 10, 2)
        #self.log(f'Consumption Publish Data Input {kwargs}')

        data_sElectrDHWTotal_parsed = kwargs["dhw_total"]
        data_sElectrHCTotal_parsed = kwargs["hc_total"]
        data_sElectrDHWDay_parsed = kwargs["dhw_day"]
        data_sElectrHCDay_parsed = kwargs["hc_day"]

        value = round(data_sElectrDHWTotal_parsed['electrDHWTotal'], 4)
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_dhw_total_energy"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sElectrHCTotal_parsed['electrHCTotal'], 4)
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_hc_total_energy"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sElectrDHWDay_parsed['electrDHWDay']/1000.0, 4)
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_dhw_day_energy"),
            str(value),
            qos=0,
            retain=True,
        )

        value = round(data_sElectrHCDay_parsed['electrHCDay']/1000.0, 4)
        self.mqtt.mqtt_publish(
            self.topic("sensor/thz_hc_day_energy"),
            str(value),
            qos=0,
            retain=True,
        ) 


    def publish_party_mode_state(self, state: str):
        state = (state or "").upper().strip()
        if state not in ("ON", "OFF"):
            self.log(f"⚠️ Invalid party mode state: {state}")
            return
        self.mqtt.mqtt_publish(
            self.topic(self.PARTY_MODE_STATE_TOPIC_SUFFIX),
            state,
            qos=0,
            retain=True,
        )

    def publish_last_cmd_diag(self, payload: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.mqtt.mqtt_publish(
            self.topic("diagnostic/party_mode_last_cmd"),
            f"{payload} @ {ts}",
            qos=0,
            retain=True,
        )

    # =========================
    # Command handler
    # =========================
    def on_party_mode_cmd(self, event_name, data, kwargs):
        payload = (data.get("payload") or "").upper().strip()
        self.log(f"Party mode command received: {payload}")

        # Diagnostic: last command
        self.publish_last_cmd_diag(payload)

        # Apply + publish state
        if payload == "ON":
            self.log("🎉 Party mode ENABLED")
            self.publish_party_mode_state("ON")

        elif payload == "OFF":
            self.log("😴 Party mode DISABLED")
            self.publish_party_mode_state("OFF")

        else:
            self.log(f"⚠️ Unknown payload: {payload}")



import socket

class TecalorThzSocket:
    """ Communicate to a tecalor thz over a TCP Serial Socket"""
    def __init__(self, tcp_ip, tcp_port):
        self.tcp_ip = tcp_ip
        self.tcp_port = tcp_port
        self.sleep_time = 0.25 # 0.25
        self.buffer_size = 200

        pass
    
    def connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(5.0) # in seconds as float
        self._socket.connect((self.tcp_ip, self.tcp_port))
        pass
    
    def disconnect(self):
        self._socket.close()

    def get_raw_data(self, command):
        """ command: bytestring for the thz to request the wanted data 
            return: raw byte string from the device (without escaping etc.) """
        # Flush existing socket data
        #if False:
        #    try:
        #        data_dummy = socket.recv(1) #Response from Relays
        #    except Exception as e:
        #        print(type(e))
        #        print(e)
        #    time.sleep(sleeptime)

        # Initiate a new request
        self._socket.send(b'\x02')
        time.sleep(self.sleep_time)
        data = self._socket.recv(self.buffer_size) #Response from Relays

        # Send request for data
        data_query = b'\xFB'
        send_data = b'\x01\x00' + self._add_checksum_to_command(command) + b'\x10\x03'
        #print(send_data)
        self._socket.send(send_data)
        time.sleep(self.sleep_time)
        data = self._socket.recv(self.buffer_size) #Response from Relays
        #print(data)

        # Acknowledge to receive data
        self._socket.send(b'\x10')
        time.sleep(self.sleep_time)
        data = self._socket.recv(self.buffer_size) #Response from Relays
        #print(data)

        # Finish the communication cycle
        #socket.send(b'\x10\x02')
        #try:
        #    data_dummy = socket.recv(1) #Response from Relays
        #except Exception as e:
        #    print(type(e))
        #    print(e)

        return data

    def decode_raw_data(self, bytestring):
        """ Decode the received data from the heatpump """
        bytestring = bytestring.replace(b'\x2b\x18', b'\x2b')
        bytestring = bytestring.replace( b'\x10\x10', b'\x10')
        return bytestring
    
    def encode_command(self, bytestring):
        """ Encode the command to send to the heatpump """
        bytestring = bytestring.replace(b'\x2b', b'\x2b\x18')
        bytestring = bytestring.replace(b'\x10', b'\x10\x10')
        return bytestring
    
    
    def decode_global_data(self, escaped_data):
        data = escaped_data

        thisdict = {
            "outsideTemp": (int.from_bytes(data[6:8], "big", signed=True)/10.0),
            "flowTemp": (int.from_bytes(data[8:10], "big", signed=True)/10.0),
            "returnTemp": (int.from_bytes(data[10:12], "big", signed=True)/10.0),
            "hotGasTemp": (int.from_bytes(data[12:14], "big", signed=True)/10.0),
            "dhwTemp": (int.from_bytes(data[14:16], "big", signed=True)/10.0),
            "flowTempHC2": (int.from_bytes(data[16:18], "big", signed=True)/10.0),
            "insideTemp": (int.from_bytes(data[18:20], "big", signed=True)/10.0),
            "evaporatorTemp": (int.from_bytes(data[20:22], "big", signed=True)/10.0),
            "condenserTemp": (int.from_bytes(data[22:24], "big", signed=True)/10.0),

            "outputVentilatorPower": (int.from_bytes(data[27:29], "big")/10.0),
            "inputVentilatorPower": (int.from_bytes(data[29:31], "big")/10.0),
            "mainVentilatorPower": (int.from_bytes(data[31:33], "big")/10.0),
            "outputVentilatorSpeed": (int.from_bytes(data[33:35], "big")/10.0),
            "inputVentilatorSpeed": (int.from_bytes(data[35:37], "big")/10.0),
            "mainVentilatorSpeed": (int.from_bytes(data[37:39], "big")/1.0),
            "outside_tempFiltered": (int.from_bytes(data[39:41], "big", signed=True)/10.0),
            "relHumidity": (int.from_bytes(data[41:43], "big", signed=True)/10.0),
            "dewPoint": (int.from_bytes(data[43:45], "big", signed=True)/10.0),
            
            "lowPressureSensor": (int.from_bytes(data[45:47], "big", signed=True)/1.0),
            "highPressureSensor": (int.from_bytes(data[47:49], "big", signed=True)/1.0),
            "flowRate": (int.from_bytes(data[57:59], "big", signed=True)/100.0),
            
            "mixerOpen": (int.from_bytes(data[24:25], "big")&1) != 0,
            "mixerClosed": (int.from_bytes(data[24:25], "big")&2) != 0,
            "heatPipeValve": (int.from_bytes(data[24:25], "big")&4) != 0,
            "diverterValve": (int.from_bytes(data[24:25], "big")&8) != 0,
            "dhwPump": (int.from_bytes(data[24:25], "big")&16) != 0,
            "heatingCircuitPump": (int.from_bytes(data[24:25], "big")&32) != 0,
            "mixerPumpOn": (int.from_bytes(data[24:25], "big")&64) != 0,
            "solarPump": (int.from_bytes(data[24:25], "big")&128) != 0,
            
            "oilSumpHeaterOn": (int.from_bytes(data[25:26], "big")&1) != 0,
            "Bit25_1": (int.from_bytes(data[25:26], "big")&2) != 0,
            "Bit25_2": (int.from_bytes(data[25:26], "big")&4) != 0,
            "compressorOn" : (int.from_bytes(data[25:26], "big")&8) != 0,
            "Bit25_4": (int.from_bytes(data[25:26], "big")&16) != 0,
            "Bit25_5": (int.from_bytes(data[25:26], "big")&32) != 0,
            "Bit25_6": (int.from_bytes(data[25:26], "big")&64) != 0,
            "Bit25_7": (int.from_bytes(data[25:26], "big")&128) != 0,

            "highPressureSensorInverted" : (int.from_bytes(data[26:27], "big")&1) != 0,
            "lowPressureSensorInverted" : (int.from_bytes(data[26:27], "big")&2) != 0,
            "evaporatorIceMonitor" : (int.from_bytes(data[26:27], "big")&4) != 0,
            "signalAnode" : (int.from_bytes(data[26:27], "big")&8) != 0,
            "evuRelease" : (int.from_bytes(data[26:27], "big")&16) != 0,
            "ovenFireplace" : (int.from_bytes(data[26:27], "big")&32) != 0,
            "STB" : (int.from_bytes(data[26:27], "big")&64) != 0,
            "Bit26_7" : (int.from_bytes(data[26:27], "big")&128) != 0,

        }
        return thisdict
    
    def decode_hc1_data(self, escaped_data):
        data = escaped_data

        thisdict = {
            "returnTemp": (int.from_bytes(data[8:10], "big", signed=True)/10.0),
            "integralHeat": (int.from_bytes(data[10:12], "big", signed=True)/1.0),
            "flowTemp": (int.from_bytes(data[12:14], "big", signed=True)/10.0),
            "heatSetTemp": (int.from_bytes(data[14:16], "big", signed=True)/10.0),
            "heatTemp": (int.from_bytes(data[16:18], "big", signed=True)/10.0),
            "roomSetTemp": (int.from_bytes(data[30:32], "big", signed=True)/10.0),
            "roomTempRC": (int.from_bytes(data[36:38], "big", signed=True)/10.0),
        }
        return thisdict

    def decode_dhw_data(self, escaped_data):
        data = escaped_data

        thisdict = {
            "dhwTemp": (int.from_bytes(data[4:6], "big", signed=True)/10.0),
            "dhwSetTemp": (int.from_bytes(data[8:10], "big", signed=True)/10.0)
        }
        return thisdict

    def decode_flowrate_data(self, escaped_data):
        data = escaped_data

        thisdict = {
            "flowRate": (int.from_bytes(data[6:8], "big", signed=True)/10.0)
        }
        return thisdict
    
    def decode_single_int_data(self, escaped_data):
        data = escaped_data

        thisdict = {
            "value": (int.from_bytes(data[6:8], "big", signed=True)/1.0)
        }
        return thisdict
    
    def _calc_checksum(self, s):
        """ Internal function that calcuates the checksum """
        checksum = 1
        for i in range(0, len(s)):
            checksum += s[i] #ord(s[i])
            checksum &= 0xFF
        return bytes([checksum]) # chr(checksum)

    def _add_checksum_to_command(self, s):
        """ inserts a the beginning a checksum """
        if len(s) < 1:
            raise ValueError("The provided string needs to be atleast 1 byte long")    
        return (self._calc_checksum(s) + s)