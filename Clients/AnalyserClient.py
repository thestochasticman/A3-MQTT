from paho.mqtt.client import CallbackAPIVersion
from paho.mqtt.enums import MQTTProtocolVersion
from Clients.Instructions import Instructions
from Clients.analysis import analyse
from typing_extensions import Self
from dataclasses import dataclass
import paho.mqtt.client as mqtt
from pandas import DataFrame
from pandas import read_csv
from os.path import dirname
from os.path import exists
from random import Random
from pprint import pprint
from os import makedirs
from time import sleep
from os import remove
from time import time
import threading

dir_publisher_logs = f"{dirname(dirname(__file__))}/logs/publisher"
dir_analyser_logs = f"{dirname(dirname(__file__))}/logs/analyser"
    
class AnalyserClient(mqtt.Client):
    def __init__(
        s: Self,
        client_id: str,
        host: str,
        port: str,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
        transport: str = 'tcp',
        manual_ack: bool = True,
    ):
        super().__init__(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=True,
            protocol=MQTTProtocolVersion.MQTTv311,
            transport=transport,
            manual_ack=False
        )

        s.host = host
        s.port = port
        s.sub_qos = 0
        s.connect_event = threading.Event()
        s.subscribe_event = threading.Event()
        s.unsubscribe_event = threading.Event()
        s.done_event = threading.Event()

        s.logs_path = f"{dir_analyser_logs}/report.csv"
        makedirs(dir_analyser_logs, exist_ok=True)
        if exists(s.logs_path):
            remove(s.logs_path)

        s.publisher_msgs = []
        s.sys_stats = []
        s.client_id = client_id
        s.current_subs = 0
        s.sys_messeges = []

    def on_connect(
        s: Self,
        client: 'AnalyserClient',
        userdata: None,
        flags: mqtt.ConnectFlags,
        rc: int,
        propeties=None
    ):
        if rc == 0:
            s.connect_event.set()
        else:
            print(f"{s.client_id} Connection to {s._host, s._port}, failed, rc={rc}")

    def subscribe_to_topics(s: Self, qos: int):
        s.subscribe('counter/#', qos=qos)
        s.subscribe('$SYS/#', qos=qos)
        s.subscribe('publisher_counts/#', qos=qos)
        s.subscribe('total_counts/#', qos=2)
        s.subscribe_event.wait()
        s.subscribe_event.clear()

    def on_subscribe(
        s: Self,
        client: 'AnalyserClient',
        userdata: None,
        mid: int,
        granted_qos: int,
        properties=None
    ):
        s.current_subs += 1
        if s.current_subs == 4:
            s.subscribe_event.set()

    def unsubcribe_from_topics(s: Self):
        s.unsubscribe('counter/#')
        s.unsubscribe('$SYS/#')
        s.unsubscribe('publisher_counts/#')
        s.unsubscribe('total_counts/#')
        s.unsubscribe_event.wait()
        s.unsubscribe_event.clear()

    def on_unsubscribe(s: Self, client: 'AnalyserClient', userdata: None, mid: int, removed_qos: int, properties=None):
        s.current_subs -= 1
        if s.current_subs == 0:
            s.unsubscribe_event.set()

    def publish_instructions(s: Self, pub_qos: int, sub_qos: int, delay: int, size: int, instances: int):
        s.publish('request/pub_qos',       str(pub_qos))
        s.publish('request/sub_qos',       str(sub_qos))
        s.publish('request/delay',         str(delay))
        s.publish('request/messagesize',   str(size))
        s.publish('request/instancecount', str(instances))
        s.publish('request/go',            '1')
        print('published', s.sub_qos, s.instances, delay, size)

    def on_message(s: Self, client: 'AnalyserClient', userdata: None, msg):
        time_of_receive = int(time() * 1000)
        topic = msg.topic
        payload = msg.payload.decode()

        if topic.startswith('counter/'):
            publisher_id, pub_qos, delay, size = topic.split('/')[1:]
            if int(pub_qos) == s.instructions.pub_qos:
                if int(delay) == s.instructions.delay:
                    if int(size) == s.instructions.messagesize:
                        counter, timestamp = payload.split(':')[:2]
                        if int(timestamp) <= time_of_receive:
                            record = {'ts_send': int(timestamp),'ts_rec': time_of_receive, 'counter': int(counter)}
                            s.count_logs.setdefault(int(publisher_id), []).append(record)

    @staticmethod
    def get_tests():
        return [
            (pq, sq, d, s, inst)
            for pq   in [0, 1, 2]
            for sq   in [0, 1, 2]
            for d    in [0, 100]
            for s    in [0, 1000, 4000]
            for inst in [1, 5, 10]
        ]

    def get_total_msgs_sent_per_publisher(s: Self):
        publishers_that_are_running = []
        for i in range(1, 11):
            if s.instructions.check_if_can_start_publishing(i):
                publishers_that_are_running += [i]
        total_counts = {}
        for publisher_id in publishers_that_are_running:
            logs_publisher = f"{dir_publisher_logs}/{publisher_id}.csv"
            if not exists(logs_publisher):
                return {}
            else:
                total_counts[publisher_id] = float(read_csv(logs_publisher)['count'][0])

        for publisher_id in publishers_that_are_running:
            remove(f"{dir_publisher_logs}/{publisher_id}.csv")

        if len(total_counts) == s.instances:
            return total_counts

    def run(s: Self):
        tests = s.get_tests()
        tests = Random(42).sample(tests, k=len(tests))
        for pub_qos, sub_qos, delay, size, instances in tests:
            s.count_logs = {}
            s.instructions = Instructions(
                instancecount=instances,
                pub_qos=pub_qos,
                delay=delay,
                messagesize=size
            )
            s.connect(s.host, s.port)
            s.loop_start()
            s.instances = instances
            if s.current_subs == 4 and s.sub_qos != sub_qos: s.unsubcribe_from_topics()
            if s.current_subs == 0 or s.sub_qos != sub_qos: s.subscribe_to_topics(sub_qos)
            s.publish_instructions(pub_qos, sub_qos, delay, size, instances)
            total_counts = {}
            while not total_counts:
                sleep(1)
                total_counts = s.get_total_msgs_sent_per_publisher()
            str_config = f"{pub_qos}-{sub_qos}-{delay}-{size}-{instances}"

            report = [
                analyse(
                    pub_qos,
                    sub_qos,
                    delay,
                    size,
                    instances,
                    total_counts,
                    s.count_logs
                )
            ]
            DataFrame.from_records(report).to_csv(s.logs_path ,mode='a', header=True if not exists(s.logs_path) else False)
            s.loop_stop()
            s.disconnect()  
            sleep(1.0)
        

if __name__ == '__main__':
    BROKER = 'localhost'
    PORT   = 1883
    client = AnalyserClient(client_id='analyser', host=BROKER, port=PORT)
    client.run()