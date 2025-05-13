import time
import threading
import paho.mqtt.client as mqtt

BROKER_HOST = 'localhost'
BROKER_PORT = 1883

config = {'qos': 0, 'delay': 0, 'messagesize': 0, 'instancecount': 0}
go_event = threading.Event()

# MQTT control callbacks

def on_connect(client, userdata, flags, rc):
    print(f"[Publisher] Connected (rc={rc})")
    for topic in ('request/qos','request/delay','request/messagesize','request/instancecount','request/go'):
        client.subscribe(topic)

def on_message(client, userdata, msg):
    t = msg.topic.split('/')[-1]
    payload = msg.payload.decode()
    if t in config:
        config[t] = int(payload)
    elif t == 'go':
        go_event.set()

# Per-instance publisher thread

def publisher_thread(instance_id: int):
    qos = config['qos']
    delay_ms = config['delay']
    size = config['messagesize']
    payload = 'x' * size
    topic = f'counter/{instance_id}/{qos}/{delay_ms}/{size}'

    client = mqtt.Client(client_id=f'pub-{instance_id:02d}')
    client.connect(BROKER_HOST, BROKER_PORT)
    client.loop_start()

    end_time = time.time() + 30
    count = 0
    while time.time() < end_time:
        timestamp = int(time.time() * 1000)
        client.publish(topic, f'{count}:{timestamp}:{payload}', qos=qos)
        count += 1
        if delay_ms:
            time.sleep(delay_ms / 1000)

    client.loop_stop()
    client.disconnect()
    print(f"[Publisher-{instance_id}] sent {count} messages")

# Main control loop

def main():
    control = mqtt.Client(client_id='publisher_control')
    control.on_connect = on_connect
    control.on_message = on_message
    control.connect(BROKER_HOST, BROKER_PORT)
    control.loop_start()
    print("Publisher awaiting commands...")

    try:
        while True:
            go_event.wait()
            print("[Publisher] GO received—launching threads")
            threads = []
            for i in range(1, config['instancecount'] + 1):
                t = threading.Thread(target=publisher_thread, args=(i,))
                t.start()
                threads.append(t)
            for t in threads:
                t.join()
            go_event.clear()
    except KeyboardInterrupt:
        print("[Publisher] Interrupted—exiting")
    finally:
        control.loop_stop()
        control.disconnect()

if __name__ == '__main__':
    main()
