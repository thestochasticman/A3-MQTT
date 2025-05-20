
import paho.mqtt.client as mqtt
from pandas import DataFrame
from os.path import exists
from os import makedirs
from os import mkdir
import threading
import time

BROKER = 'localhost'
PORT   = 1883
NUM_THREADS = 10

class PubWorker(threading.Thread):
    def __init__(self, instance_id):
        super().__init__(daemon=True)
        self.id = instance_id
        # per-thread config and event
        self.config = {
            'sub_qos': 0,
            'delay': 0,
            'messagesize': 0,
            'instancecount': 0,
            'pub_qos': 0
        }
        self.subscribe_event = threading.Event()
        self.total_subscriptions = 6
        self.current_subscriptions = 0
        self.go_event = threading.Event()
        self.client = mqtt.Client(client_id=f'pub-{instance_id:02d}', callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.connect(BROKER, PORT)
        self.client.loop_start()

    def on_subscribe(self, client, userdata, mid, rc, properties=None):
        if rc[0].is_failure:
            print(f"Broker rejected Subscription: {rc[0]}")
        else:
            self.current_subscriptions += 1
            if self.current_subscriptions >= self.total_subscriptions:
                self.subscribe_event.set()
                # print('subscribed', self.total_subscriptions)
            # print(f"Broker granted the following QOS", print(mid))

    def on_connect(self, client, userdata, flags, rc, properties=None):
        client.subscribe('request/pub_qos')
        client.subscribe('request/sub_qos',)
        client.subscribe('request/delay')
        client.subscribe('request/messagesize')
        client.subscribe('request/instancecount')
        client.subscribe('request/go')

    def on_message(self, client, userdata, msg):
        topic = msg.topic.split('/')[-1]
        val  = msg.payload.decode()
        if topic in self.config:
            self.config[topic] = int(val)
        elif topic == 'go':
            self.go_event.set()

    def run(self):
        self.subscribe_event.wait()
        self.subscribe_event.clear()
        print(f"[Worker-{self.id}] ready, waiting for GO")

        df = DataFrame()
        while True:
            self.go_event.wait()
            self.go_event.clear()
            if self.id <= self.config['instancecount']:
                pub_qos   = self.config['pub_qos']
                sub_qos = self.config['sub_qos']
                delay = self.config['delay']
                size  = self.config['messagesize']
                payload = 'x' * size
                topic   = f"counter/{self.id}/{pub_qos}/{delay}/{size}"
                end_t = time.time() + 30
                count = 0
                print(f"[Worker-{self.id}] starting burst on {topic}")
                while time.time() < end_t:
                    ts = int(time.time()*1000)
                    msg = f"{count}:{ts}:{payload}"
                    self.client.publish(topic, msg, qos=pub_qos)
                    count += 1
                    time.sleep(delay/1000)
                
                logs_dir = f"publisher_logs/{self.id}/{pub_qos}-{sub_qos}-{delay}-{size}-{self.config['instancecount']}"
                if not exists(logs_dir): makedirs(logs_dir)
                log_path = f"{logs_dir}/{self.id}.txt"
                
                with open(log_path, 'w') as f:
                    f.write(f"sent:{count}\n")
                
if __name__ == '__main__':
    if not exists('publisher_logs'): mkdir('publisher_logs')
    # spawn all 10 workers up front
    workers = [PubWorker(i) for i in range(1, NUM_THREADS+1)]
    for w in workers: w.start()
    # keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down…")
        for w in workers:
            w.client.loop_stop()
            w.client.disconnect()

