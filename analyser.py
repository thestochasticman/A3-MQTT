from Clients.AnalyserClient import UserData as UserData1
from Clients.AnalyserClient import AnalyserClient
import threading
from time import sleep


BROKER = 'localhost'
PORT   = 1883
def run_test(pub_qos, sub_qos, delay, size, instances):
    subscribe_event = threading.Event()
    done_event = threading.Event()
    userdata = UserData1(subscribe_event, 3)
    client = AnalyserClient('AnalyserClient', qos=sub_qos, userdata=userdata)
    client.connect(BROKER, PORT)
    client.loop_start()
    subscribe_event.wait()
    client.publish_instructions(pub_qos, sub_qos, delay, size, instances)
    sleep(40)
    client.loop_stop()
    client.disconnect()
    return {
        'counter_msgs': client.publisher_msgs,
    }

def main():
    from os import makedirs
    from pandas import DataFrame

    tests = [
        (pq, sq, d, s, inst)
        for pq   in [0, 1, 2]
        for sq   in [0, 1, 2]
        for d    in [0, 100]
        for s    in [0, 1000, 4000]
        for inst in [1, 5, 10]
    ]
    makedirs('analyser_logs', exist_ok=True)
    for i, params in enumerate(tests):
        print(f"\nRunning test {params}.")
        result = run_test(*params)
        pq, sq, d, s, inst = params
        logs_path = f"analyser_logs/{pq}-{sq}-{d}-{s}-{inst}.csv"
        DataFrame.from_records(result['counter_msgs']).to_csv(logs_path)
        # Append result row
        # with open(RESULTS_FILE, 'a', newline='') as f:
        #     writer = csv.DictWriter(f, fieldnames=result.keys())
        #     writer.writerow(result)
        # # break
        # print(f"Completed {params}: received={result['received']} msgs")

    # Tear down the publisher

    # print("\nAll tests complete. Results in", RESULTS_FILE)

if __name__ == '__main__':
   main()

