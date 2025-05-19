from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.enums import MQTTProtocolVersion
from typing_extensions import Self
from dataclasses import dataclass
import paho.mqtt.client as mqtt
from time import time
import threading

@dataclass
class UserData:    
    done_event  : threading.Event
    instances   : int
    
class AnalyserClient2(mqtt.Client):
    def __init__(
        s: Self,
        client_id: str,
        qos: int,
        userdata: UserData,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
        transport: str = 'tcp',
        manual_ack: bool = True,
    ):
        super().__init__(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=f"publisher_{id}",
            clean_session=True,
            protocol=MQTTProtocolVersion.MQTTv311,
            transport=transport,
            manual_ack=manual_ack
        )
        s.publisher_msgs = []
        s.sys_stats = []
        s.qos = qos
        s.client_id = client_id
        s.userdata = userdata
        s.current_subs = 0

        s.publishers_summary = {}
    
    def on_connect(
        s: Self,
        client: 'AnalyserClient2',
        userdata: None,
        flags: mqtt.ConnectFlags,
        rc: int,
        properties=None
    ):
        if rc == 0:
            s.subscribe('publishers_summary/#', qos=2)
        #     # print(f"{s.client_id}, Connected to {s._host, s._port}")
        #     pass
        # else:
        #     # print(f"{s.client_id} Connection to {s._host, s._port}, failed, rc={rc}")

    def on_message(s: Self, client: 'AnalyserClient2', userdata: UserData, msg):
        time_of_receive = int(time() * 1000)
        topic = msg.topic
        payload = msg.payload.decode()

        if topic.startswith('publishers_summary/'):
            print(topic, payload)
            s.publishers_summary[topic.split('/')[-1]] = payload
            if len(s.publishers_summary) == s.userdata.instances:
                s.userdata.done_event.set()

        s.ack(msg.mid, msg.qos)