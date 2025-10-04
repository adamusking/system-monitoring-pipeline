import time
import os
import json
import psutil as ps
from dotenv import load_dotenv
from paho.mqtt import client as mqtt_client

load_dotenv()

broker = os.getenv("BROKER")
port = int(os.getenv("PORT"))
topic = os.getenv("TOPIC")
client_id = os.getenv("CLIENT_ID_1")
username = os.getenv("MQTT_USER")       
password = os.getenv("MQTT_PASSWORD")   

def get_data():
    cpu_percent_total = ps.cpu_percent()
    cpu_per_core = ps.cpu_percent(percpu=True)
    cpu_freq = ps.cpu_freq()
    cpu_times = ps.cpu_times()

    vm = ps.virtual_memory()
    swap = ps.swap_memory()

    disk = ps.disk_usage("/")
    disk_io = ps.disk_io_counters()
    net_io = ps.net_io_counters()

    payload = {
        "host": client_id,  
        "cpu_usage": cpu_percent_total,
        "cpu_freq_current": cpu_freq.current,
        "cpu_freq_min": cpu_freq.min,
        "cpu_freq_max": cpu_freq.max,
        "cpu_user_time": cpu_times.user,
        "cpu_system_time": cpu_times.system,
        "cpu_idle_time": cpu_times.idle,
        "virtual_mem_total": vm.total,
        "virtual_mem_used": vm.used,
        "virtual_mem_free": vm.free,
        "virtual_mem_percent": vm.percent,
        "swap_total": swap.total,
        "swap_used": swap.used,
        "swap_free": swap.free,
        "swap_percent": swap.percent,
        "disk_total": disk.total,
        "disk_used": disk.used,
        "disk_free": disk.free,
        "disk_percent": disk.percent,
        "disk_read_count": disk_io.read_count,
        "disk_write_count": disk_io.write_count,
        "disk_read_bytes": disk_io.read_bytes,
        "disk_write_bytes": disk_io.write_bytes,
        "net_bytes_sent": net_io.bytes_sent,
        "net_bytes_recv": net_io.bytes_recv,
        "net_packets_sent": net_io.packets_sent,
        "net_packets_recv": net_io.packets_recv
    }

    for i, usage in enumerate(cpu_per_core):
        payload[f"cpu_core_{i}_usage"] = usage

    return payload

connected = False

def connect_mqtt():
    def on_connect(client, userdata, flags, reasonCode, properties=None):
        global connected
        if reasonCode == 0:
            print("Connected to MQTT Broker!")
            connected = True
        else:
            print(f"Failed to connect, reason code {reasonCode}")

    client = mqtt_client.Client(client_id=client_id)
    client.username_pw_set(username=username, password=password)  
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client):
    global connected
    msg_count = 1
    while True:
        if connected:
            data = get_data()
            msg = json.dumps(data)
            result = client.publish(topic, msg)
            status = result[0]
            if status == 0:
                print(f"Sent message #{msg_count} to topic `{topic}`")
            else:
                print(f"Failed to send message to topic {topic}")
            msg_count += 1
        time.sleep(1)

def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)
    client.loop_stop()

if __name__ == '__main__':
    run()
