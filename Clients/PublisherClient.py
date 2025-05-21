from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.enums import MQTTProtocolVersion
from typing_extensions import Self
from dataclasses import dataclass
import paho.mqtt.client as mqtt
from time import perf_counter
from copy import deepcopy
from time import time
from time import sleep
import threading

@dataclass
class Instructions:
    instancecount   : int | None = None
    pub_qos         : int | None = None
    delay           : int | None = None
    messagesize     : int | None = None
    go              : str | None = None

    def update(s: Self, instruction: str, val: str):
        if not s.check_if_received_full_instruction():
            if instruction in ['instancecount', 'pub_qos', 'delay', 'messagesize']:
                object.__setattr__(s, instruction, int(val))
            if instruction == 'go':
                s.go = 'go'

    def check_if_received_full_instruction(s: Self):
        condition = all(
            [
                isinstance(s.instancecount, int),
                isinstance(s.pub_qos, int),
                isinstance(s.delay, int),
                isinstance(s.messagesize, int),
                isinstance(s.go, str)
            ]
        )
        return True if condition else False

    def check_if_can_start_publishing(s: Self, id: int):
        if id <= s.instancecount and s.check_if_received_full_instruction():
            return True
        else:
            return False

class PublisherClient(mqtt.Client):
    def __init__(
        s: Self,
        id: int,
        host: str,
        port: str,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
        transport: str = 'tcp',
        manual_ack: bool = True
    ):
        
        super().__init__(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=f"publisher_{id}",
            clean_session=False,
            protocol=MQTTProtocolVersion.MQTTv311,
            transport=transport,
            manual_ack=manual_ack
        )
        s.instructions = Instructions()
        s.id = id
        s.host = host
        s.port = port
        s.num_subscribed_topics = 0

        s.go_event: threading.Event = threading.Event()
        s.subscribe_event: threading.Event = threading.Event()

    def on_connect(
        s: Self,
        client: 'PublisherClient',
        userdata: None,
        flags: mqtt.ConnectFlags,
        rc: int,
        properties=None,
    ):

        if rc == 0:
            # print(f"{s._client_id}, Connected to {s._host, s._port}")
            s.subscribe('request/pub_qos',)
            s.subscribe('request/delay')
            s.subscribe('request/messagesize')
            s.subscribe('request/instancecount')
            s.subscribe('request/go')
        else:
            print(f"{s._client_id} Connection to {s._host, s._port}, failed, rc={rc}")

    
    def on_subscribe(
        s: Self,
        client: 'PublisherClient',
        userdata: None,
        mid: int,
        granted_qos: int,
        properties=None
    ):
        s.num_subscribed_topics += 1
        if s.num_subscribed_topics >= 5:
            s.subscribe_event.set()
    
    def on_message(s: Self, client: 'PublisherClient', userdata: None, msg):
     
        instruction = msg.topic.split('/')[-1]
        val = msg.payload.decode()
        s.instructions.update(instruction, val)
        if s.instructions.check_if_received_full_instruction():
            if s.instructions.check_if_can_start_publishing(s.id):
                s.go_event.set()
            else:
                s.instructions = Instructions()
        s.ack(msg.mid, msg.qos)

    def burst(s: Self, i: Instructions):
        deadline = perf_counter() + 30
        topic = '/'.join(
            [
                'counter',
                str(s.id),
                str(i.pub_qos),
                str(i.delay),
                str(i.messagesize)
            ]
        )
        count = 0
        payload = 'x' * i.messagesize
        while perf_counter() < deadline:
            timestamp = int(time() * 1000)
            msg = ':'.join([str(count), str(timestamp), payload])
            count += 1
            s.publish(topic, msg, qos=i.pub_qos)
            sleep(i.delay/1000)

    def run(s: Self):
        s.connect(s.host, s.port)
        s.loop_start()
        s.subscribe_event.wait()
        while True:
            s.go_event.wait()
            print('start burst', s.id)
            s.burst(deepcopy(s.instructions))
            s.go_event.clear()
            print('burst over', s.id)
            s.instructions = Instructions()
            
if __name__ == '__main__':
    VERSION = CallbackAPIVersion.VERSION2
    BROKER = 'localhost'
    PORT   = 1883
    publisher_client = PublisherClient(id=5, host=BROKER, port=PORT)
    publisher_client.run()
