#!/usr/bin/env python3
"""
analyser.py

Drives the MQTT test suite by publishing control messages to publisher.py
and logging everything that arrives on counter/# and $SYS/#.
"""

import os
import csv
import time
import subprocess
import paho.mqtt.client as mqtt

# ─────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────

BROKER_HOST     = 'localhost'
BROKER_PORT     = 1883
PUBLISHER_SCRIPT = os.path.join(os.path.dirname(__file__), 'publisher.py')

RESULTS_FILE    = 'mqtt_results.csv'
SYS_LOG_DIR     = 'sys_logs'

# In‐memory logs for each test
test_msgs = {}   # topic -> list of (topic, payload, recv_ts)
sys_stats = []   # list of (topic, payload, recv_ts)


# ─────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


# ─────────────────────────────────────────────────────
# Subscriber callbacks
# ─────────────────────────────────────────────────────

def on_connect_sub(client, userdata, flags, rc):
    sub_qos = userdata.get('sub_qos', 0)
    client.subscribe('counter/#', qos=sub_qos)
    client.subscribe('$SYS/#', qos=0)

def on_message_sub(client, userdata, msg):
    now = int(time.time() * 1000)
    topic = msg.topic
    payload = msg.payload.decode(errors='ignore')
    if topic.startswith('counter/'):
        print(f"RECV {topic} {payload[:50]}…")
        test_msgs.setdefault(topic, []).append((topic, payload, now))
    else:  # $SYS/#
        # print(f" SYS {topic} {payload}")
        sys_stats.append((topic, payload, now))


# ─────────────────────────────────────────────────────
# Single‐test runner
# ─────────────────────────────────────────────────────

def run_test(pub_qos, sub_qos, delay, size, instances):
    # Clear logs
    test_msgs.clear()
    sys_stats.clear()

    # 1) Start subscriber client
    sub_client = mqtt.Client(
        client_id='analyser_sub',
        userdata={'sub_qos': sub_qos}
    )
    sub_client.on_connect = on_connect_sub
    sub_client.on_message = on_message_sub
    sub_client.connect(BROKER_HOST, BROKER_PORT)
    sub_client.loop_start()
    time.sleep(1)  # ensure subscriptions are active

    # 2) Control‐publisher to send parameters + GO
    pub_client = mqtt.Client(
        client_id='analyser_pub'
    )
    pub_client.connect(BROKER_HOST, BROKER_PORT)
    pub_client.loop_start()
    pub_client.publish('request/qos',           str(pub_qos))
    pub_client.publish('request/delay',         str(delay))
    pub_client.publish('request/messagesize',   str(size))
    pub_client.publish('request/instancecount', str(instances))
    pub_client.publish('request/go',            '1')

    # 3) Let the 30 s burst run + 2 s buffer
    time.sleep(32)

    # 4) Clean up MQTT clients
    pub_client.loop_stop()
    sub_client.loop_stop()
    pub_client.disconnect()
    sub_client.disconnect()

    # 5) Persist $SYS/# logs for this test
    ensure_dir(SYS_LOG_DIR)
    sys_path = os.path.join(
        SYS_LOG_DIR,
        f"sys_{pub_qos}_{sub_qos}_{delay}_{size}_{instances}.csv"
    )
    with open(sys_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['topic', 'payload', 'recv_ts'])
        writer.writerows(sys_stats)

    # 6) Summarize count of received messages
    total_received = sum(len(v) for v in test_msgs.values())

    return {
        'pub_qos': pub_qos,
        'sub_qos': sub_qos,
        'delay': delay,
        'size': size,
        'instances': instances,
        'received': total_received
    }


# ─────────────────────────────────────────────────────
# Main sweep
# ─────────────────────────────────────────────────────

def main():
    # Launch the publisher subprocess
    pub_proc = subprocess.Popen(['python3', PUBLISHER_SCRIPT])

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
        print(f"Completed {params}: received={result['received']} msgs")

    # Tear down the publisher
    pub_proc.terminate()
    print("\nAll tests complete. Results in", RESULTS_FILE)

if __name__ == '__main__':
    main()
