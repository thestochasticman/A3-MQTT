import os
import csv
import time
import threading
import subprocess
import paho.mqtt.client as mqtt

class UserData:
    test_messages: dict
    sys_stats: list


