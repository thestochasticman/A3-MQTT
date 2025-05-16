# import asyncio
# from hbmqtt.broker import Broker

# # Configuration for the custom MQTT broker using hbmqtt
# broker_config = {
#     'listeners': {
#         'default': {
#             'type': 'tcp',
#             'bind': '0.0.0.0:1883'  # Listen on all interfaces
#         }
#     },
#     'sys_interval': 10,        # Interval for broker system messages
#     'auth': {
#         'allow-anonymous': True   # Allow clients to connect without credentials
#     }
# }

# async def start_broker():
#     """
#     Start the MQTT broker with the specified configuration.
#     Supports CONNECT, PUBLISH, SUBSCRIBE, DISCONNECT.
#     """
#     broker = Broker(broker_config)
#     await broker.start()
#     print("HBMQTT broker started on port 1883")

#     try:
#         # Keep broker running until interrupted
#         await asyncio.Event().wait()
#     except (KeyboardInterrupt, asyncio.CancelledError):
#         print("Shutting down broker...")
#         await broker.shutdown()
#         print("Broker stopped")

# if __name__ == '__main__':
#     # Run the broker in the default asyncio loop
#     asyncio.run(start_broker())

import asyncio
from hbmqtt.broker import Broker

# Configuration for the custom MQTT broker using hbmqtt
broker_config = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '0.0.0.0:1883'  # Listen on all interfaces
        }
    },
    'sys_interval': 10,        # Interval for broker system messages
    'auth': {
        'allow-anonymous': True   # Allow clients to connect without credentials
    },
    # Explicitly disable topic-check to suppress warnings
    'topic-check': {
        'enabled': False,
        'plugins': []
    }
}

async def start_broker():
    """
    Start the MQTT broker with the specified configuration.
    Supports CONNECT, PUBLISH, SUBSCRIBE, DISCONNECT.
    """
    broker = Broker(broker_config)
    await broker.start()
    print("HBMQTT broker started on port 1883")

    try:
        # Keep broker running until interrupted
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("Shutting down broker...")
        await broker.shutdown()
        print("Broker stopped")

if __name__ == '__main__':
    # Run the broker in the default asyncio loop
    asyncio.run(start_broker())

