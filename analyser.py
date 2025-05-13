import paho.mqtt.client as mqtt
import statistics
import subprocess
import time
import csv
import os

# Configuration
BROKER_HOST = 'localhost'
BROKER_PORT = 1883
RESULTS_FILE = 'mqtt_results.csv'
SYS_LOG_DIR = 'sys_logs'
SCRIPT_DIR = os.path.dirname(__file__)
PUBLISHER_SCRIPT = os.path.join(SCRIPT_DIR, 'publisher.py')

# Ensure directory exists
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

# Data stores
test_msgs = {}
sys_stats = []

# Subscriber callbacks
def on_connect_sub(client, userdata, flags, rc):
    sub_qos = userdata.get('sub_qos', 0)
    client.subscribe('counter/#', qos=sub_qos)
    client.subscribe('$SYS/#', qos=0)

def on_message_sub(client, userdata, msg):
    now = int(time.time() * 1000)
    topic = msg.topic
    payload = msg.payload.decode()
    if topic.startswith('counter/'):
        instance = int(topic.split('/')[1])
        counter_str, sent_ts_str, _ = payload.split(':', 2)
        rec = (int(counter_str), int(sent_ts_str), now)
        test_msgs.setdefault(instance, []).append(rec)
    else:
        sys_stats.append((topic, payload, now))

# Run one test combination
def run_test(pub_qos, sub_qos, delay, size, instances):
    test_msgs.clear()
    sys_stats.clear()

    # Subscriber client
    sub = mqtt.Client(client_id='analyser_sub', userdata={'sub_qos': sub_qos})
    sub.on_connect = on_connect_sub
    sub.on_message = on_message_sub
    sub.connect(BROKER_HOST, BROKER_PORT)
    sub.loop_start()
    time.sleep(1)  # wait for subscription to register

    # Control publisher client
    pub = mqtt.Client(client_id='analyser_pub')
    pub.connect(BROKER_HOST, BROKER_PORT)
    pub.loop_start()
    pub.publish('request/qos', str(pub_qos))
    pub.publish('request/delay', str(delay))
    pub.publish('request/messagesize', str(size))
    pub.publish('request/instancecount', str(instances))
    pub.publish('request/go', '1')

    # Wait for 30s of publishing + buffer
    time.sleep(32)

    pub.loop_stop()
    pub.disconnect()
    sub.loop_stop()
    sub.disconnect()

    # Compute metrics
    duration = 30.0
    total_received = sum(len(v) for v in test_msgs.values())
    total_rate = total_received / duration if duration else 0.0

    per_metrics = []
    for recs in test_msgs.values():
        counters = [r[0] for r in recs]
        received = len(counters)
        expected = max(counters) + 1 if counters else 0
        loss_pct = ((expected - received) / expected * 100) if expected > 0 else 0
        dup = (received - len(set(counters))) / received * 100 if received else 0
        oo = sum(1 for i in range(len(counters)-1) if counters[i+1] < counters[i])
        oo_pct = oo / (received-1) * 100 if received > 1 else 0
        times = {c: t for c, _, t in recs}
        gaps = [times[c+1] - times[c] for c in range(max(counters)) if c in times and c+1 in times]
        mean_gap = statistics.mean(gaps) if gaps else 0
        sd_gap = statistics.stdev(gaps) if len(gaps) > 1 else 0
        per_metrics.append((loss_pct, dup, oo_pct, mean_gap, sd_gap))

    avg_loss = statistics.mean(m[0] for m in per_metrics) if per_metrics else 0
    avg_dup  = statistics.mean(m[1] for m in per_metrics) if per_metrics else 0
    avg_oo   = statistics.mean(m[2] for m in per_metrics) if per_metrics else 0
    avg_gap  = statistics.mean(m[3] for m in per_metrics) if per_metrics else 0
    avg_sd   = statistics.mean(m[4] for m in per_metrics) if per_metrics else 0

    # Save $SYS logs
    ensure_dir(SYS_LOG_DIR)
    sys_path = os.path.join(SYS_LOG_DIR, f"sys_{pub_qos}_{sub_qos}_{delay}_{size}_{instances}.csv")
    with open(sys_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['topic','payload','recv_ts'])
        writer.writerows(sys_stats)

    return {
        'pub_qos': pub_qos,
        'sub_qos': sub_qos,
        'delay': delay,
        'size': size,
        'instances': instances,
        'total_rate': total_rate,
        'avg_loss_pct': avg_loss,
        'avg_dup_pct': avg_dup,
        'avg_oo_pct': avg_oo,
        'avg_gap_ms': avg_gap,
        'avg_gap_sd_ms': avg_sd
    }

# Main sweep

def main():
    # Launch publisher subprocess
    pub_proc = subprocess.Popen(['python3', PUBLISHER_SCRIPT])

    # Prepare test parameter list
    tests = [
        (pq, sq, d, s, inst)
        for pq in [0,1,2]
        for sq in [0,1,2]
        for d in [0,100]
        for s in [0,1000,4000]
        for inst in [1,5,10]
    ]

    # Create results CSV
    with open(RESULTS_FILE, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'pub_qos','sub_qos','delay','size','instances',
            'total_rate','avg_loss_pct','avg_dup_pct','avg_oo_pct','avg_gap_ms','avg_gap_sd_ms'
        ])
        writer.writeheader()

    # Execute tests
    for params in tests:
        print(f"Running test {params}...")
        res = run_test(*params)
        with open(RESULTS_FILE, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=res.keys())
            writer.writerow(res)
        print(f"Completed {params}: rate={res['total_rate']:.1f} msg/s")

    # Shut down publisher
    pub_proc.terminate()
    print(f"All tests complete. Results in {RESULTS_FILE}, SYS logs in {SYS_LOG_DIR}")

if __name__ == '__main__':
    main()

