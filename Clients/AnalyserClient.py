from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.enums import MQTTProtocolVersion
from typing_extensions import Self
from dataclasses import dataclass
import paho.mqtt.client as mqtt
from time import time
import threading

@dataclass
class UserData:
    subscribe_event : threading.Event
    num_subs        : int = 3
    
class AnalyserClient(mqtt.Client):
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
            clean_session=False,
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
    
    def on_connect(
        s: Self,
        client: 'AnalyserClient',
        userdata: None,
        flags: mqtt.ConnectFlags,
        rc: int,
        properties=None
    ):
        if rc == 0:
            print(f"{s.client_id}, Connected to {s._host, s._port}")
        else:
            print(f"{s.client_id} Connection to {s._host, s._port}, failed, rc={rc}")

        s.subscribe('counter/#', qos=s.qos)
        s.subscribe('$SYS/#', qos=2)
        s.subscribe('total_counts/#', qos=2)

    def on_subscribe(
        s: Self,
        client: 'AnalyserClient',
        userdata: UserData,
        mid: int,
        granted_qos: int,
        properties=None
    ):
        print(f"{s.client_id} SUBACK received: mid={mid}, granted_qos={granted_qos}")
        s.current_subs += 1

        if s.current_subs >= s.userdata.num_subs:
            s.userdata.subscribe_event.set()
        
    def on_message(s: Self, client: 'AnalyserClient', userdata: UserData, msg):
        time_of_receive = int(time() * 1000)
        topic = msg.topic
        payload = msg.payload.decode()

        if topic.startswith('counter/'):
            s.publisher_msgs(topic, []).append((topic, payload, time_of_receive))


    def publish_instructions(s: Self, pub_qos: int, delay: int, size: int, instances: int):
        s.publish('request/qos',           str(pub_qos))
        s.publish('request/delay',         str(delay))
        s.publish('request/messagesize',   str(size))
        s.publish('request/instancecount', str(instances))
        s.publish('request/go',            '1')