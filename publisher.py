from Clients.PublisherClient import PublisherClient
from time import sleep

# if __name__ == '__main__':
#     BROKER = 'localhost'
#     PORT   = 1883
#     NUM_THREADS = 10
#     NUM_CLIENTS = 10

#     clients = [PublisherClient(id=i+1) for i in range(NUM_CLIENTS)]

#     for client in clients:
#         client.connect(BROKER, PORT)
#         client.loop_start()

#     # Now `clients` holds all your running PublisherClient instances.
#     # Keep the main thread alive so they don’t all exit immediately:
#     try:
#         while True:
#             sleep(1)
#     except KeyboardInterrupt:
#         # Clean shutdown
#         for client in clients:
#             client.loop_stop()
#             client.disconnect()

class PublisherManager:
    def __init__(self, n_clients):
        self.clients = [PublisherClient(id=i) for i in range(1, n_clients+1)]

    def start_all(self, broker, port):
        for c in self.clients:
            c.connect(broker, port)
            c.loop_start()

    def stop_all(self):
        for c in self.clients:
            c.loop_stop()
            c.disconnect()

if __name__ == "__main__":
    BROKER = 'localhost'
    PORT   = 1883
    mgr = PublisherManager(10)
    mgr.start_all(BROKER, PORT)
    try:
        while True: sleep(1)
    except KeyboardInterrupt:
        mgr.stop_all()
