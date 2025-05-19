#!/usr/bin/env python3
"""
analyser.py

Drives the MQTT test suite by publishing control messages to publisher.py
and logging everything that arrives on counter/# and $SYS/#.
"""

import os
import csv
import time
import threading
import subprocess
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

BROKER_HOST     = 'localhost'
BROKER_PORT     = 1883
PUBLISHER_SCRIPT = os.path.join(os.path.dirname(__file__), 'publisher.py')

RESULTS_FILE    = 'mqtt_results.csv'
SYS_LOG_DIR     = 'sys_logs'

# In‐memory logs for each test
test_msgs = {}   # topic -> list of (topic, payload, recv_ts)
sys_stats = []   # list of (topic, payload, recv_ts)

def ensure_dir(path): os.makedirs(path, exist_ok=True)

def on_connect_sub(client, userdata, mid, rc, properties=None):
    sub_qos = userdata.get('sub_qos', 0)
    client.subscribe('counter/#', qos=sub_qos)
    client.subscribe('$SYS/#', qos=0)
    client.subscribe('total_counts/#', qos=2)

def on_subscribe_sub(client, userdata, mid, rc, properties=None):
    if rc[0].is_failure:
        print(f"Broker rejected Subscription: {rc[0]}")
    else:
        userdata['subscribe_acks'] += 1
        if userdata['subscribe_acks'] == 3:
            userdata['subscribe_event'].set()
        print(f"Broker granted the following QOS", print(mid))

def on_message_sub(client, userdata, msg):
    now = int(time.time() * 1000)
    topic = msg.topic
    payload = msg.payload.decode(errors='ignore')
    if topic.startswith('counter/'):
        # print(f"RECV {topic} {payload[:50]}…")
        test_msgs.setdefault(topic, []).append((topic, payload, now))
    
    if topic.startswith('total_counts/'):
        userdata['total_counts_received'] += 1
        print('received', topic, payload)
        if userdata['total_counts_received'] == userdata['instances']:
            print('received', topic, payload)
            userdata['total_counts_event'].set()
    else:  # $SYS/#
        # print(f" SYS {topic} {payload}")
        sys_stats.append((topic, payload, now))

def connect_sub(qos: int, instances: int, subscribe_event, total_counts_event):
    return mqtt.Client(
        client_id='analyser_sub',
        userdata={
            'sub_qos': qos,
            'subscribe_acks': 0,
            'instances': instances,
            'total_counts_received': 0,
            'subscribe_event': subscribe_event,
            'total_counts_event': total_counts_event
        },
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )

# def run_test(pub_qos, sub_qos, delay, size, instances):

#     test_msgs.clear()
#     sys_stats.clear()
#     subscribe_event = threading.Event()
#     total_counts_event      = threading.Event()
#     sub_client = connect_sub(sub_qos, instances, subscribe_event, total_counts_event)
#     sub_client.on_connect = on_connect_sub
#     sub_client.on_message = on_message_sub
#     sub_client.on_subscribe = on_subscribe_sub
#     sub_client.connect(BROKER_HOST, BROKER_PORT)
#     sub_client.loop_start()
#     subscribe_event.wait()
#     subscribe_event.clear()

#     # 2) Control‐publisher to send parameters + GO
#     pub_client = mqtt.Client(
#         client_id='analyser_pub',
#         callback_api_version=mqtt.CallbackAPIVersion.VERSION2
#     )
#     pub_client.connect(BROKER_HOST, BROKER_PORT, )
#     pub_client.loop_start()
#     pub_client.publish('request/qos',           str(pub_qos))
#     pub_client.publish('request/delay',         str(delay))
#     pub_client.publish('request/messagesize',   str(size))
#     pub_client.publish('request/instancecount', str(instances))
#     pub_client.publish('request/go',            '1')


#     # # 3) Let the 30 s burst run + 2 s buffer
#     time.sleep(32)

#     # # 4) Clean up MQTT clients
#     # total_counts_event.wait()
#     # total_counts_event.clear()
#     pub_client.loop_stop()
#     sub_client.loop_stop()
#     pub_client.disconnect()
#     sub_client.disconnect()

#     # # 5) Persist $SYS/# logs for this test
#     ensure_dir(SYS_LOG_DIR)
#     sys_path = os.path.join(
#         SYS_LOG_DIR,
#         f"sys_{pub_qos}_{sub_qos}_{delay}_{size}_{instances}.csv"
#     )
#     with open(sys_path, 'w', newline='') as f:
#         writer = csv.writer(f)
#         writer.writerow(['topic', 'payload', 'recv_ts'])
#         writer.writerows(sys_stats)

#     # 6) Summarize count of received messages
#     total_received = sum(len(v) for v in test_msgs.values())

#     return {
#         'pub_qos': pub_qos,
#         'sub_qos': sub_qos,
#         'delay': delay,
#         'size': size,
#         'instances': instances,
#         'received': total_received
#     }

def run_test(pub_qos, sub_qos, delay, size, instances):
    test_msgs.clear()
    sys_stats.clear()

    # 1) Prepare synchronization primitives
    subscribe_event      = threading.Event()
    total_counts_event   = threading.Event()

    # 2) Build our single client, with all the userdata it needs
    userdata = {
        'sub_qos':               sub_qos,
        'subscribe_acks':        0,
        'instances':             instances,
        'total_counts_received': 0,
        'subscribe_event':       subscribe_event,
        'total_counts_event':    total_counts_event
    }
    client = mqtt.Client(
        client_id="analyser",
        userdata=userdata,
        callback_api_version=CallbackAPIVersion.VERSION2,
        clean_session=False
    )

    # 3) Wire up callbacks
    client.on_connect   = on_connect_sub
    client.on_subscribe = on_subscribe_sub
    client.on_message   = on_message_sub

    # 4) Connect & subscribe
    client.connect(BROKER_HOST, BROKER_PORT)
    client.loop_start()
    subscribe_event.wait()       # wait until we've got all 3 SUBACKs
    subscribe_event.clear()

    # 5) Publish our “control” messages on the same client
    #    (you can specify qos here if you like)
    client.publish('request/qos',           str(pub_qos))
    client.publish('request/delay',         str(delay))
    client.publish('request/messagesize',   str(size))
    client.publish('request/instancecount', str(instances))
    client.publish('request/go',            '1')

    # 6) Let the 30 s burst happen + a small buffer
    time.sleep(40)

    # 7) Tear down
    client.loop_stop()
    client.disconnect()

    # 8) Persist $SYS/# logs for this test
    ensure_dir(SYS_LOG_DIR)
    sys_path = os.path.join(
        SYS_LOG_DIR,
        f"sys_{pub_qos}_{sub_qos}_{delay}_{size}_{instances}.csv"
    )
    with open(sys_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['topic', 'payload', 'recv_ts'])
        writer.writerows(sys_stats)

    # 9) Count what we saw on counter/#
    total_received = sum(len(v) for v in test_msgs.values())

    return {
        'pub_qos':   pub_qos,
        'sub_qos':   sub_qos,
        'delay':     delay,
        'size':      size,
        'instances': instances,
        'received':  total_received
    }


def main():
    # Launch the publisher subprocess


    # Build the full list of 162 tests
    tests = [
        (pq, sq, d, s, inst)
        for pq   in [0, 1, 2]
        for sq   in [0, 1, 2]
        for d    in [0, 100]
        for s    in [0, 1000, 4000]
        for inst in [1, 5, 10]
    ]

    # Prepare the results CSV
    with open(RESULTS_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'pub_qos', 'sub_qos', 'delay', 'size', 'instances', 'received'
        ])
        writer.writeheader()

    # Run each test in sequence
    for params in tests:
        print(f"\nRunning test {params} …")
        result = run_test(*params)
        # Append result row
        with open(RESULTS_FILE, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=result.keys())
            writer.writerow(result)
        # break
        print(f"Completed {params}: received={result['received']} msgs")

    # Tear down the publisher

    print("\nAll tests complete. Results in", RESULTS_FILE)

if __name__ == '__main__':
    main()