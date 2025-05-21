from Clients.PublisherClient import PublisherClient
import threading

BROKER_HOST = "localhost"     
BROKER_PORT = 1883

def start_publisher(pid: int):
    client = PublisherClient(id=pid, host=BROKER_HOST, port=BROKER_PORT)
    client.run()              

threads = []
for pid in range(1, 11):       
    t = threading.Thread(
        args=(pid,),
        target=start_publisher,
        name=f"pub-{pid}",
        daemon=True            
    )
    t.start()
    threads.append(t)

try:
    for t in threads:
        t.join()
except KeyboardInterrupt:
    print("Stopping all publishers…")

