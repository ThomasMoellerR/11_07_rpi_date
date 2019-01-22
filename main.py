import json
import time
import queue
import threading
import argparse
import paho.mqtt.client as mqtt


def thread1():
    global client

    while True:

        client.on_connect = on_connect
        client.on_message = on_message

        try_to_connect = True

        while try_to_connect:
            try:
                client.connect(args.mqtt_server_ip, int(args.mqtt_server_port), 60)
                try_to_connect = False
                break
            except Exception as e:
                print(e)



        # Blocking call that processes network traffic, dispatches callbacks and
        # handles reconnecting.
        # Other loop*() functions are available that give a threaded interface and a
        # manual interface.
        client.loop_forever()

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):

    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    #client.subscribe(args.mqtt_topic_set_temperature)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):

    print(msg.topic + " "+ msg.payload.decode("utf-8"))


    # client.publish(args.mqtt_topic_ack_temperature, hours + ":" + minutes + ":" + seconds, qos=0, retain=False)





def search_and_send(date):
    for i in json.load(open(args.json_file)):

        #print(i["topic"])
        #print(i["payload"])
        #print(i["date"])

        if i["date"] == date:
            print(i["date"], i["topic"], i["payload"])
            client.publish(i["topic"], i["payload"], qos=0, retain=False)

        temp = "every" + " " + date[11:19]
        if i["date"] == temp:
            print(temp, i["topic"], i["payload"])
            client.publish(i["topic"], i["payload"], qos=0, retain=False)






def thread2():
    global q
    t_old = ""

    while True:
        time.sleep(0.1)
        t = time.strftime("%Y-%m-%d %H:%M:%S")

        if t != t_old:
            q.put(t)

        t_old = t




def thread3():
    while True:
        time.sleep(0.1)

        while not q.empty():
            date = q.get()

            search_and_send(date)



# Argparse
parser = argparse.ArgumentParser()
parser.add_argument("--mqtt_server_ip", help="")
parser.add_argument("--mqtt_server_port", help="")
parser.add_argument("--json_file", help="")
args = parser.parse_args()

q = queue.Queue()

client = mqtt.Client()

t1= threading.Thread(target=thread1)
t2= threading.Thread(target=thread2)
t3= threading.Thread(target=thread3)

t1.start()
time.sleep(1)
t2.start()
t3.start()
