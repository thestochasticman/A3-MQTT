import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.enums import MQTTProtocolVersion
from typing_extensions import Self
from dataclasses import dataclass
from copy import deepcopy
from time import time
from time import sleep
import threading

class PublisherClient(mqtt.Client):
    def __init__(
        s: Self,
        id: int,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
        transport: str = 'tcp',
        manual_ack: bool = True
    ):
        s.id = id
        super().__init__(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=f"publisher_{id}",
            clean_session=False,
            protocol=MQTTProtocolVersion.MQTTv311,
            transport=transport,
            manual_ack=manual_ack,
        )
        s.instructions = Instructions()

    def on_connect(
        s: Self,
        client: 'PublisherClient',
        userdata: None,
        flags: mqtt.ConnectFlags,
        rc: int,
        properties=None,
    ):
        if rc == 0:
            print(f"{s._client_id}, Connected to {s._host, s._port}")
        else:
            print(f"{s._client_id} Connection to {s._host, s._port}, failed, rc={rc}")


        s.subscribe('request/qos',)
        s.subscribe('request/delay')
        s.subscribe('request/messagesize')
        s.subscribe('request/instancecount')
        s.subscribe('request/go')
    
    def on_message(s: Self, client: 'PublisherClient', userdata: None, msg):
        instruction = msg.topic.split('/')[-1]
        val = msg.payload.decode()
        s.instructions.update(instruction, val)
        if s.instructions.check_if_received_full_instruction():
            params = deepcopy(s.instructions.__dict__)
            threading.Thread(
                target=s.run,
                kwargs=params,
                daemon=True,
                name=f"PublisherWorker-{s.id}"
            ).start()
            s.instructions = Instructions()


    def run(s: Self, instancecount: int, qos: int, delay: int, messagesize: int, go: str):
        count = 0
        topic = f"counter/{s.id}/{qos}/{delay}/{messagesize}"
        payload = 'x' * messagesize
        end_timestamp = time() + 30
        count = 0
        print('hello ji')
        if s.id <= instancecount:
            print(f"[Publisher Client Worker-{s.id}] starting burst on {topic}")
            while time() < end_timestamp:
                ts = int(time() * 1000)
                msg = f"{count}:{ts}:{payload}"
                s.publish(topic, msg, qos=qos)
                count += 1
                if delay: sleep(delay)
            print(f"[Publisher Client Worker-{s.id}] finished burst on {topic}")

@dataclass
class Instructions:
    instancecount   : int | None = None
    qos             : int | None = None
    delay           : int | None = None
    messagesize     : int | None = None
    go              : str | None = None

    def update(s: Self, instruction: str, val: str):
        print(instruction)
        if instruction in ['instancecount', 'qos', 'delay', 'messagesize']:
            object.__setattr__(s, instruction, int(val))
        if instruction == 'go':
            s.go = 'go'

    def check_if_received_full_instruction(s: Self):
        condition = all(
            [
                isinstance(s.instancecount, int),
                isinstance(s.qos, int),
                isinstance(s.delay, int),
                isinstance(s.messagesize, int),
                isinstance(s.go, str)
            ]
        )
        return True if condition else False
                
if __name__ == '__main__':
    VERSION = CallbackAPIVersion.VERSION2
    BROKER = 'localhost'
    PORT   = 1883
    publisher_client = PublisherClient(id=1)
    publisher_client.connect(BROKER, PORT)
    publisher_client.loop_forever()
    print('jhj')

