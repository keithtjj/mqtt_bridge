import inject
import paho.mqtt.client as mqtt
import rospy
from std_msgs.msg import String
from .bridge import create_bridge
from .mqtt_client import create_private_path_extractor
from .util import lookup_object

import socket
import sys
import time

import subprocess


available = {}

global connected
connected = 0

def create_config(mqtt_client, serializer, deserializer, mqtt_private_path):
    if isinstance(serializer, str):
        serializer = lookup_object(serializer)
    if isinstance(deserializer, str):
        deserializer = lookup_object(deserializer)
    private_path_extractor = create_private_path_extractor(mqtt_private_path)
    def config(binder):
        binder.bind('serializer', serializer)
        binder.bind('deserializer', deserializer)
        binder.bind(mqtt.Client, mqtt_client)
        binder.bind('mqtt_private_path_extractor', private_path_extractor)
    return config


def mqtt_bridge_node():
    # init node
    rospy.init_node('mqtt_bridge_node')

    # load parameters
    params = rospy.get_param("~", {})
    mqtt_params = params.pop("mqtt", {})
    conn_params = mqtt_params.pop("connection")
    mqtt_private_path = mqtt_params.pop("private_path", "")
    bridge_params = params.get("bridge", [])
    # print(bridge_params)

    ipparams = params.pop("ip",{})
    # print(ipparams)
    brokers = list(ipparams.values())
    # print(brokers)



    # create mqtt client
    mqtt_client_factory_name = rospy.get_param(
        "~mqtt_client_factory", ".mqtt_client:default_mqtt_client_factory")
    mqtt_client_factory = lookup_object(mqtt_client_factory_name)
    mqtt_client = mqtt_client_factory(mqtt_params)

    # initialise connection_flag
    mqtt_client.connected_flag=False

    # load serializer and deserializer
    serializer = params.get('serializer', 'msgpack:dumps')
    deserializer = params.get('deserializer', 'msgpack:loads')

    # dependency injection
    config = create_config(
        mqtt_client, serializer, deserializer, mqtt_private_path)
    inject.configure(config)

    # configure and connect to MQTT broker
    select = 0

    time.sleep(1)
    for broker in brokers:
        host = broker['host']
        port = broker['port']
        priority = broker['priority']
        try:
            res = subprocess.call(['ping', '-c', '3', host])
            if res == 0:
                available[host] = priority
                print(f"Ping to {host}:{port} succeeded")
            elif res == 2:
                print(f"Ping to {host}:{port} failed")
            else:
                print(f"Ping to {host}:{port} failed")
        
        except ConnectionRefusedError:
            print(f"Ping to {host}:{port} failed")
        


    
    print("Ping Test Complete")
    marklist = sorted(available.items(), key=lambda x:x[1])
    sortdict = dict(marklist) 
    # print(sortdict)
    key_list = list(sortdict.keys())
    val_list = list(sortdict.values())
    # print(min(val_list))
    key = list(filter(lambda x: sortdict[x] == min(val_list), sortdict))[0]

    print("highest priority: ", key)

    mqtt_client._host = key
    mqtt_client._port = port
    mqtt_client.on_connect = _on_connect
    mqtt_client.on_disconnect = _on_disconnect
    mqtt_client.connect(key, port, 60)


    # configure bridges
    bridges = []
    for bridge_args in bridge_params:
        bridges.append(create_bridge(**bridge_args))

    # start MQTT loop
    mqtt_client.loop_start()

    # register shutdown callback and spin
    rospy.on_shutdown(mqtt_client.disconnect)
    rospy.on_shutdown(mqtt_client.loop_stop)
    rospy.spin()

def _on_connect(client, userdata, flags, response_code):
    global connected
    rospy.loginfo('MQTT connected')
    if response_code == 0:
        client.connected_flag=True #set flag
        connected = 1
        print(f"Connected to the broker {client._host}:{client._port}")
    else:
        print(f"Connection to the broker {client._host}:{client._port} failed with return code", response_code)


def _on_disconnect(client, userdata, response_code):
    rospy.loginfo('MQTT disconnected')
    rospy.signal_shutdown('restarting...')

__all__ = ['mqtt_bridge_node']
