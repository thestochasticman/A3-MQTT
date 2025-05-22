from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.enums import MQTTProtocolVersion
from Clients.Instructions import Instructions
from typing_extensions import Self
import paho.mqtt.client as mqtt
from time import perf_counter
from pandas import DataFrame
from os.path import dirname
from os.path import exists
from copy import deepcopy
from os import makedirs
from time import sleep
from os import remove
from time import time
import threading

dir_logs = f"{dirname(dirname(__file__))}/logs/publisher"


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
            clean_session=True,
            protocol=MQTTProtocolVersion.MQTTv311,
            transport=transport,
            manual_ack=False
        )
        s.instructions = Instructions()
        s.id = id
        s.host = host
        s.port = port
        s.num_subscribed_topics = 0

        s.go_event: threading.Event = threading.Event()
        s.subscribe_event: threading.Event = threading.Event()
        s.path_logs = f"{dir_logs}/{s.id}.csv"
        
        makedirs(dir_logs, exist_ok=True)
        if exists(s.path_logs):
            remove(s.path_logs)

    def on_connect(
        s: Self,
        client: 'PublisherClient',
        userdata: None,
        flags: mqtt.ConnectFlags,
        rc: int,
        properties=None,
    ):

        if rc == 0:
            pass
        else:
            print(f"{s._client_id} Connection to {s._host, s._port}, failed, rc={rc}")

    def subscribe_to_topics(s: Self):
        s.subscribe('request/pub_qos')
        s.subscribe('request/delay')
        s.subscribe('request/messagesize')
        s.subscribe('request/instancecount')
        s.subscribe('request/go')
        s.subscribe_event.wait()
        s.subscribe_event.clear()

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
        # s.ack(msg.mid, msg.qos)

    def on_disconnect(
        s: Self,
        client: 'PublisherClient',
        userdata: None,
        disconnect_flags,
        reason_code: int,
        properties: None
    ):
        # print('disconnecting')
        s.num_subscribed_topics = 0

    def burst(s: Self, i: Instructions):
        deadline = time() + 30
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
        while time() < deadline:
            timestamp = int(time() * 1000)
            msg = ':'.join([str(count), str(timestamp), payload])
            count += 1
            s.publish(topic, msg, qos=i.pub_qos)
            sleep(i.delay/1000)
        
        df = DataFrame.from_records([{'id': s.id, 'count': count}])
        df.to_csv(s.path_logs)

    def run(s: Self):
        s.connect(s.host, s.port)
        s.loop_start()
        s.subscribe_to_topics()
        while True:
            s.go_event.wait()
            print('start burst', s.id)
            s.burst(deepcopy(s.instructions))
            s.instructions = Instructions()
            print('burst over', s.id)
            s.go_event.clear()
            s.loop_stop()
            s.disconnect()
            s.connect(s.host, s.port)
            s.loop_start()
            s.subscribe_to_topics()
            
if __name__ == '__main__':
    VERSION = CallbackAPIVersion.VERSION2
    BROKER = 'localhost'
    PORT   = 1883
    publisher_client = PublisherClient(id=5, host=BROKER, port=PORT)
    publisher_client.run()
