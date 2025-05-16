# # #!/usr/bin/env python3
# # """
# # analyser.py

# # Drives the MQTT test suite by publishing control messages to publisher.py
# # and logging everything that arrives on counter/# and $SYS/#.
# # """

# # import os
# # import csv
# # import time
# # import threading
# # import subprocess
# # import paho.mqtt.client as mqtt

# # BROKER_HOST     = 'localhost'
# # BROKER_PORT     = 1883
# # PUBLISHER_SCRIPT = os.path.join(os.path.dirname(__file__), 'publisher.py')

# # RESULTS_FILE    = 'mqtt_results.csv'
# # SYS_LOG_DIR     = 'sys_logs'

# # # In‐memory logs for each test
# # test_msgs = {}   # topic -> list of (topic, payload, recv_ts)
# # sys_stats = []   # list of (topic, payload, recv_ts)

# # def ensure_dir(path): os.makedirs(path, exist_ok=True)

# # def on_connect_sub(client, userdata, mid, rc, properties=None):
# #     if rc == 0:
# #         sub_qos = userdata.get('sub_qos', 0)
# #         client.subscribe('counter/#', qos=sub_qos)
# #         client.subscribe('$SYS/#', qos=0)
# #         client.subscribe('total_counts/#', qos=2)

# # def on_subscribe_sub(client, userdata, mid, rc, properties=None):
# #     if rc[0].is_failure:
# #         print(f"Broker rejected Subscription: {rc[0]}")
# #     else:
# #         userdata['subscribe_acks'] += 1
# #         if userdata['subscribe_acks'] == 3:
# #             userdata['subscribe_event'].set()
# #         print(f"Broker granted the following QOS", print(mid))

# # def on_message_sub(client, userdata, msg):
# #     now = int(time.time() * 1000)
# #     topic = msg.topic
# #     payload = msg.payload.decode(errors='ignore')
# #     if topic.startswith('counter/'):
# #         # print(f"RECV {topic} {payload[:50]}…")
# #         test_msgs.setdefault(topic, []).append((topic, payload, now))
    
# #     if topic.startswith('total_counts/'):
# #         userdata['total_counts_received'] += 1
# #         print('received', topic, payload)
# #         if userdata['total_counts_received'] == userdata['instances']:
# #             print('received', topic, payload)
# #             userdata['total_counts_event'].set()
# #     else:  # $SYS/#
# #         # print(f" SYS {topic} {payload}")
# #         sys_stats.append((topic, payload, now))

# # def connect_sub(qos: int, instances: int, subscribe_event, total_counts_event):
# #     return mqtt.Client(
# #         client_id='analyser_sub',
# #         userdata={
# #             'sub_qos': qos,
# #             'subscribe_acks': 0,
# #             'instances': instances,
# #             'total_counts_received': 0,
# #             'subscribe_event': subscribe_event,
# #             'total_counts_event': total_counts_event
# #         },
# #         callback_api_version=mqtt.CallbackAPIVersion.VERSION2
# #     )

# # def run_test(pub_qos, sub_qos, delay, size, instances):
    
# #     test_msgs.clear()
# #     sys_stats.clear()
# #     subscribe_event = threading.Event()
# #     total_counts_event      = threading.Event()
# #     sub_client = connect_sub(sub_qos, instances, subscribe_event, total_counts_event)
# #     sub_client.on_connect = on_connect_sub
# #     sub_client.on_message = on_message_sub
# #     sub_client.on_subscribe = on_subscribe_sub
# #     sub_client.connect(BROKER_HOST, BROKER_PORT)
# #     sub_client.loop_start()
# #     subscribe_event.wait()
# #     subscribe_event.clear()
# #     # 2) Control‐publisher to send parameters + GO
# #     pub_client = mqtt.Client(
# #         client_id='analyser_pub',
# #         callback_api_version=mqtt.CallbackAPIVersion.VERSION2
# #     )

# #     print('publishing')
# #     pub_client.connect(BROKER_HOST, BROKER_PORT)
# #     pub_client.loop_start()
    
# #     pub_client.publish('request/qos',           str(pub_qos))
# #     pub_client.publish('request/delay',         str(delay))
# #     pub_client.publish('request/messagesize',   str(size))
# #     pub_client.publish('request/instancecount', str(instances))
# #     pub_client.publish('request/go',            '1')
    
# #     # # 3) Let the 30 s burst run + 2 s buffer
# #     # time.sleep(32)

# #     # # 4) Clean up MQTT clients
# #     total_counts_event.wait()
# #     total_counts_event.clear()
# #     print('bahar aagaya')
# #     pub_client.loop_stop()
# #     sub_client.loop_stop()
# #     pub_client.disconnect()
# #     sub_client.disconnect()

# #     # # 5) Persist $SYS/# logs for this test
# #     ensure_dir(SYS_LOG_DIR)
# #     sys_path = os.path.join(
# #         SYS_LOG_DIR,
# #         f"sys_{pub_qos}_{sub_qos}_{delay}_{size}_{instances}.csv"
# #     )
# #     with open(sys_path, 'w', newline='') as f:
# #         writer = csv.writer(f)
# #         writer.writerow(['topic', 'payload', 'recv_ts'])
# #         writer.writerows(sys_stats)

# #     # 6) Summarize count of received messages
# #     total_received = sum(len(v) for v in test_msgs.values())

# #     return {
# #         'pub_qos': pub_qos,
# #         'sub_qos': sub_qos,
# #         'delay': delay,
# #         'size': size,
# #         'instances': instances,
# #         'received': total_received
# #     }

# # def main():

# #     # Build the full list of 162 tests
# #     tests = [
# #         (pq, sq, d, s, inst)
# #         for pq   in [0, 1, 2]
# #         for sq   in [0, 1, 2]
# #         for d    in [0, 100]
# #         for s    in [0, 1000, 4000]
# #         for inst in [1, 5, 10]
# #     ]

# #     # Prepare the results CSV
# #     with open(RESULTS_FILE, 'w', newline='') as f:
# #         writer = csv.DictWriter(f, fieldnames=[
# #             'pub_qos', 'sub_qos', 'delay', 'size', 'instances', 'received'
# #         ])
# #         writer.writeheader()

# #     # Run each test in sequence
# #     for params in tests:
# #         print(f"\nRunning test {params} …")
# #         result = run_test(*params)
# #         # Append result row
# #         with open(RESULTS_FILE, 'a', newline='') as f:
# #             writer = csv.DictWriter(f, fieldnames=result.keys())
# #             writer.writerow(result)
# #         # break
# #         print(f"Completed {params}: received={result['received']} msgs")

# #     # Tear down the publisher
# #     print("\nAll tests complete. Results in", RESULTS_FILE)

# # if __name__ == '__main__':
# #     main()

# #!/usr/bin/env python3
# """
# analyser.py

# Drives the MQTT test suite by publishing control messages to publisher.py
# and logging everything that arrives on counter/#, $SYS/#, and total_counts/+.
# """

# import os
# import csv
# import time
# import threading
# import subprocess
# import paho.mqtt.client as mqtt

# BROKER_HOST      = 'localhost'
# BROKER_PORT      = 1883
# PUBLISHER_SCRIPT = os.path.join(os.path.dirname(__file__), 'publisher.py')
# RESULTS_FILE     = 'mqtt_results.csv'
# SYS_LOG_DIR      = 'sys_logs'

# class MQTTAnalyser:
#     def __init__(self):
#         # shared state
#         self.test_msgs        = {}          # topic -> list of (topic, payload, recv_ts)
#         self.sys_stats        = []          # list of (topic, payload, recv_ts)
#         self.sent_counts      = {}          # instance_id -> count
#         self.lock             = threading.Lock()

#         # sync primitives
#         self.subscribe_event    = threading.Event()
#         self.total_counts_event = threading.Event()
#         self.subscribe_acks     = 0
#         self.expected_subs      = 3   # counter/#, $SYS/#, total_counts/+

#         # build single MQTT v5 client for both sub & pub
#         self.client = mqtt.Client(
#             client_id='analyser',
#             callback_api_version=mqtt.CallbackAPIVersion.VERSION2
#         )
#         # allow callbacks to access instance attributes
#         self.client.user_data_set(self)
#         # v5 callback signatures
#         self.client.on_connect    = MQTTAnalyser.on_connect
#         self.client.on_subscribe  = MQTTAnalyser.on_subscribe
#         self.client.on_message    = MQTTAnalyser.on_message

#     @staticmethod
#     def on_connect(client, userdata, flags, reasonCode, properties):
#         print(f"[Analyser] CONNECTED (reasonCode={reasonCode}), subscribing…")
#         # subscribe to data topics
#         client.subscribe('counter/#',      qos=userdata.sub_qos)
#         client.subscribe('$SYS/#',         qos=0)
#         client.subscribe('total_counts/+', qos=1)
#         print("[Analyser] Subscribed to counter/#, $SYS/#, total_counts/+")

#     @staticmethod
#     def on_subscribe(client, userdata, mid, reasonCodes, properties):
#         userdata.subscribe_acks += 1
#         print(f"[Analyser] SUBACK mid={mid}, granted={reasonCodes}")
#         if userdata.subscribe_acks >= userdata.expected_subs:
#             userdata.subscribe_event.set()

#     @staticmethod
#     def on_message(client, userdata, msg):
#         now     = int(time.time() * 1000)
#         topic   = msg.topic
#         payload = msg.payload.decode(errors='ignore')

#         if topic.startswith('counter/'):
#             userdata.test_msgs.setdefault(topic, []).append((topic, payload, now))

#         elif topic.startswith('total_counts/'):
#             inst = int(topic.split('/',1)[1])
#             with userdata.lock:
#                 if inst not in userdata.sent_counts:
#                     userdata.sent_counts[inst] = int(payload)
#                     print(f"[Analyser] got done from pub-{inst}: {payload}")
#                 if len(userdata.sent_counts) == userdata.instances:
#                     userdata.total_counts_event.set()

#         else:  # $SYS/#
#             userdata.sys_stats.append((topic, payload, now))

#     def run_test(self, pub_qos, sub_qos, delay, size, instances):
#         # prepare for this run
#         self.sub_qos    = sub_qos
#         self.instances  = instances
#         self.test_msgs.clear()
#         self.sys_stats.clear()
#         with self.lock:
#             self.sent_counts.clear()
#         self.subscribe_event.clear()
#         self.total_counts_event.clear()
#         self.subscribe_acks = 0

#         # start network loop & connect
        
#         self.client.loop_start()
#         self.client.connect(BROKER_HOST, BROKER_PORT)
        

#         # wait for counter/#, $SYS/#, total_counts/+ SUBACKs
#         self.subscribe_event.wait()
#         print("[Analyser] all subscriptions acknowledged")

#         # publish control parameters + GO
#         self.client.publish('request/qos',           str(pub_qos),        qos=1)
#         self.client.publish('request/delay',         str(delay),          qos=1)
#         self.client.publish('request/messagesize',   str(size),           qos=1)
#         self.client.publish('request/instancecount', str(instances),      qos=1)
#         self.client.publish('request/go',            '1',                 qos=1)

#         # wait until every publisher reports done
#         self.total_counts_event.wait()
#         # small grace period for any in-flight PUBACKs
#         time.sleep(0.1)

#         # tear down
#         self.client.loop_stop()
#         self.client.disconnect()

#         # persist $SYS/# logs
#         os.makedirs(SYS_LOG_DIR, exist_ok=True)
#         sys_path = os.path.join(
#             SYS_LOG_DIR,
#             f"sys_{pub_qos}_{sub_qos}_{delay}_{size}_{instances}.csv"
#         )
#         with open(sys_path, 'w', newline='') as f:
#             writer = csv.writer(f)
#             writer.writerow(['topic','payload','recv_ts'])
#             writer.writerows(self.sys_stats)

#         # summarize performance
#         total_received = sum(len(v) for v in self.test_msgs.values())
#         result = {
#             'pub_qos':   pub_qos,
#             'sub_qos':   sub_qos,
#             'delay':     delay,
#             'size':      size,
#             'instances': instances,
#             'received':  total_received
#         }
#         with self.lock:
#             for inst, cnt in self.sent_counts.items():
#                 result[f'sent_pub{inst}'] = cnt
#         return result

# def main():
#     # launch publisher.py in parallel
#     # pub_proc = subprocess.Popen(['python3', PUBLISHER_SCRIPT])

#     analyser = MQTTAnalyser()

#     # define the 162 tests
#     tests = [
#         (pq, sq, d, s, inst)
#         for pq   in [0,1,2]
#         for sq   in [0,1,2]
#         for d    in [0,100]
#         for s    in [0,1000,4000]
#         for inst in [1,5,10]
#     ]

#     # write CSV header
#     with open(RESULTS_FILE, 'w', newline='') as f:
#         fieldnames = ['pub_qos','sub_qos','delay','size','instances','received'] \
#                    + [f'sent_pub{i}' for i in range(1,11)]
#         writer = csv.DictWriter(f, fieldnames=fieldnames)
#         writer.writeheader()

#     # run all tests
#     for params in tests:
#         print(f"\nRunning test {params} …")
#         result = analyser.run_test(*params)
#         with open(RESULTS_FILE, 'a', newline='') as f:
#             writer = csv.DictWriter(f, fieldnames=result.keys())
#             writer.writerow(result)
#         print(f"Completed {params}: received={result['received']} msgs")

#     # shutdown publisher.py
#     # pub_proc.terminate()
#     print("\nAll tests complete. Results in", RESULTS_FILE)

# if __name__ == '__main__':
#     main()


#!/usr/bin/env python3
"""
analyser.py

Drives the MQTT test suite by publishing control messages to publisher.py
and logging everything that arrives on counter/#, $SYS/#, and total_counts/+.
"""

import os
import csv
import time
import threading
import subprocess
import paho.mqtt.client as mqtt

BROKER_HOST      = 'localhost'
BROKER_PORT      = 1883
PUBLISHER_SCRIPT = os.path.join(os.path.dirname(__file__), 'publisher.py')
RESULTS_FILE     = 'mqtt_results.csv'
SYS_LOG_DIR      = 'sys_logs'

class MQTTAnalyser:
    def __init__(self):
        # shared data across tests
        self.test_msgs   = {}
        self.sys_stats   = []
        self.sent_counts = {}
        self.lock        = threading.Lock()

    @staticmethod
    def _on_connect(client, userdata, flags, reasonCode, properties):
        print(f"[Analyser] CONNECTED (reason={reasonCode}), subscribing…")
        client.subscribe('counter/#',      qos=userdata['sub_qos'])
        client.subscribe('$SYS/#',         qos=0)
        client.subscribe('total_counts/+', qos=1)
        print("[Analyser] SUBSCRIBED to counter/#, $SYS/#, total_counts/+")

    @staticmethod
    def _on_subscribe(client, userdata, mid, reasonCodes, properties):
        userdata['acks'] += 1
        print(f"[Analyser] SUBACK mid={mid}, granted={reasonCodes}")
        if userdata['acks'] == userdata['expected']:
            userdata['sub_event'].set()

    @staticmethod
    def _on_message(client, userdata, msg):
        topic   = msg.topic
        payload = msg.payload.decode()
        now     = int(time.time() * 1000)

        if topic.startswith('counter/'):
            userdata['test_msgs'].setdefault(topic, []).append((topic, payload, now))

        elif topic.startswith('total_counts/'):
            inst = int(topic.split('/',1)[1])
            with userdata['lock']:
                if inst not in userdata['sent_counts']:
                    userdata['sent_counts'][inst] = int(payload)
                    print(f"[Analyser] DONE from pub-{inst}: {payload}")
                if len(userdata['sent_counts']) == userdata['instances']:
                    userdata['done_event'].set()

        else:  # $SYS/#
            userdata['sys_stats'].append((topic, payload, now))

    def run_test(self, pub_qos, sub_qos, delay, size, instances):
        # clear per-test state
        self.test_msgs.clear()
        self.sys_stats.clear()
        self.sent_counts.clear()

        sub_event  = threading.Event()
        done_event = threading.Event()

        userdata = {
            'sub_qos':       sub_qos,
            'test_msgs':     self.test_msgs,
            'sys_stats':     self.sys_stats,
            'sent_counts':   self.sent_counts,
            'lock':          self.lock,
            'instances':     instances,
            'sub_event':     sub_event,
            'done_event':    done_event,
            'acks':          0,
            'expected':      3
        }

        # 1) create a fresh client
        client = mqtt.Client(
            client_id='analyser',
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        client.user_data_set(userdata)
        client.on_connect   = MQTTAnalyser._on_connect
        client.on_subscribe = MQTTAnalyser._on_subscribe
        client.on_message   = MQTTAnalyser._on_message

        # 2) start loop & connect
        client.loop_start()
        client.connect(BROKER_HOST, BROKER_PORT)

        # 3) wait for 3 SUBACKs
        sub_event.wait()
        print("[Analyser] all subscriptions acked")

        # 4) send control + GO
        client.publish('request/qos',           str(pub_qos),        qos=1)
        client.publish('request/delay',         str(delay),          qos=1)
        client.publish('request/messagesize',   str(size),           qos=1)
        client.publish('request/instancecount', str(instances),      qos=1)
        client.publish('request/go',            '1',                 qos=1)

        # 5) wait until every publisher reports done
        done_event.wait()
        time.sleep(0.1)  # allow any late PUBACKs

        # 6) tear down
        client.loop_stop()
        client.disconnect()

        # 7) persist $SYS/# logs
        os.makedirs(SYS_LOG_DIR, exist_ok=True)
        path = os.path.join(
            SYS_LOG_DIR,
            f"sys_{pub_qos}_{sub_qos}_{delay}_{size}_{instances}.csv"
        )
        with open(path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['topic','payload','recv_ts'])
            writer.writerows(self.sys_stats)

        # 8) summarize results
        total_received = sum(len(v) for v in self.test_msgs.values())
        result = {
            'pub_qos':   pub_qos,
            'sub_qos':   sub_qos,
            'delay':     delay,
            'size':      size,
            'instances': instances,
            'received':  total_received
        }
        with self.lock:
            for inst, cnt in self.sent_counts.items():
                result[f'sent_pub{inst}'] = cnt

        return result

def main():
    # start publisher.py
    # pub_proc = subprocess.Popen(['python3', PUBLISHER_SCRIPT])

    analyser = MQTTAnalyser()

    tests = [
        (pq, sq, d, s, inst)
        for pq in [0,1,2]
        for sq in [0,1,2]
        for d  in [0,100]
        for s  in [0,1000,4000]
        for inst in [1,5,10]
    ]

    # write CSV header
    with open(RESULTS_FILE, 'w', newline='') as f:
        hdr = ['pub_qos','sub_qos','delay','size','instances','received']
        hdr += [f'sent_pub{i}' for i in range(1,11)]
        csv.DictWriter(f, fieldnames=hdr).writeheader()

    for params in tests:
        print(f"\nRunning test {params} …")
        row = analyser.run_test(*params)
        with open(RESULTS_FILE, 'a', newline='') as f:
            csv.DictWriter(f, fieldnames=row.keys()).writerow(row)
        print(f"Completed {params}: received={row['received']} msgs")

    # pub_proc.terminate()
    print("\nResults in", RESULTS_FILE)

if __name__ == '__main__':
    main()
