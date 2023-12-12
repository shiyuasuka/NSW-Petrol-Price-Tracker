import requests
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import time
from data_gatherer import fetch_price_data

# MQTT settings
mqttBroker = "127.0.0.1"
cleaned_topic = "COMP5339/zzha9486_cleaned_prices"
data_storage = {}

def clean_data(data):

    # Extract prices from the response data if available
    prices = data.get('prices', [])

    for item in prices:
        fueltype = item.get("fueltype", "unknown")
        #print("Extracted fueltype:", fueltype)

        if fueltype not in data_storage:
            data_storage[fueltype] = []

        # Store the item under its fueltype in data_storage
        data_storage[fueltype].append(item)


def republish_cleaned_data(mqtt_topic):
    print("Republishing data:", data_storage)
    client = mqtt.Client()
    client.connect(mqttBroker, 1883, 60)
    client.on_connect = on_connect
    
    for fueltype, items in data_storage.items():
        if items:  # Ensure there's data to send
            payload = json.dumps({fueltype: items})
            print(f"Publishing to topic '{mqtt_topic}' the message: {payload}")  # This line prints the message content
            client.publish(mqtt_topic, payload)
    
    client.disconnect()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
    else:
        print("Failed to connect, return code %d\n", rc)

def start_republishing():
    while True:  # Continuous loop to keep fetching API data
        data = fetch_price_data()
        if data:
            print("Fetched price data successfully!")
            clean_data(data)
            republish_cleaned_data(cleaned_topic)
            data_storage.clear()  # Clear the data storage for the next round
        else:
            print("Failed to fetch price data.")
        time.sleep(60)  # Wait for 60 seconds before fetching again

if __name__ == "__main__":
    start_republishing()
