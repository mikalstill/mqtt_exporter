import json
import sys
import time

import paho.mqtt.client as mqtt

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


key_map = {'Temperature': 'temp_c',
           'Humidity': 'relative_humidity'}


class MyClient(mqtt.Client):
    def __init__(self):
        super(MyClient, self).__init__()
        self.is_connected = False

    def on_connect(self, _self, userdata, flags, rc):
        print 'Connected'
        self.is_connected = True

    def on_message(self, _self, userdata, message):
        registry = CollectorRegistry()
        Gauge('job_last_success_unixtime',
              'Last time the MQTT job ran',
              registry=registry).set_to_current_time()
        push_to_gateway('localhost:9091', job='mqtt', registry=registry)

        print 'Received %s, %s' %(message.topic, message.payload)
        try:
            _, sender, _ = message.topic.split('/')
            data = json.loads(message.payload)
        except Exception as e:
            return

        print data
        registry = CollectorRegistry()
        Gauge('job_last_success_unixtime',
              'Last time the MQTT job ran',
              registry=registry).set_to_current_time()
        if 'DHT' in data:
            for key in data['DHT']:
                print '%s -> %s = %s' %(sender, key, data['DHT'][key])
                Gauge(key_map[key], '', registry=registry).set(data['DHT'][key])
        push_to_gateway('localhost:9091', job='mqtt',
                        grouping_key={'instance': sender},
                        registry=registry)


def main():
    client = MyClient()
    client.loop_start()
    client.connect('eeebox.home.stillhq.com')

    while not client.is_connected:
        print 'Not yet connected'
        time.sleep(1)

    client.subscribe('tele/+/+')
    while True:
        print '.'
        time.sleep(10)


if __name__ == '__main__':
    main()
