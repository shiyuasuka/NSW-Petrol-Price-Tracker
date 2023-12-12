import requests
from datetime import datetime
import time
import paho.mqtt.client as mqtt

# reploace to your API authorization details
api_key = "replace to your api key"
api_secret = "replace to your secret"
authorization_header = "replace to your authorization_header"

# MQTT setup
mqttBroker = "127.0.0.1"
mqttTopic = "custom your topic"
mqttClient = mqtt.Client("FuelDataPublisher")
mqttClient.connect(mqttBroker)

# Define the request URL and headers for authentication
url = "https://api.onegov.nsw.gov.au/oauth/client_credential/accesstoken"
headers = {
    "content-type": "application/json",
    "authorization": authorization_header
}

# Define the request parameters
query = {
    "grant_type": "client_credentials"
}

response = requests.request("GET", url, headers=headers, params=query)
security_result = response.json()
print("Security Result:", security_result)

# Ensure access_token is present before proceeding
if "access_token" not in security_result:
    print("access_token not found in the response.")
    exit()

# API details
base_url = "https://api.onegov.nsw.gov.au"
endpoint_url = "/FuelPriceCheck/v1/fuel/prices"
headers = {
    "content-type": "application/json",
    "authorization": "Bearer " + security_result["access_token"],  
    "apikey": api_key,
    "transactionid": "Next",
    "requesttimestamp": datetime.now().strftime('%d/%m/%Y %H:%M:%S %p'), 
    "states": "NSW"
}

def fetch_price_data():
    response = requests.get(base_url + endpoint_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def publish_to_mqtt(data):
    mqttClient.publish(mqttTopic, str(data))

if __name__ == "__main__":
    while True:
        data = fetch_price_data()
        if data:
            print("Fetched price data successfully!")
            publish_to_mqtt(data)
        else:
            print("Failed to fetch price data.")
        time.sleep(60)  # Wait for 60 seconds before fetching again

