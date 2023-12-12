import json
import csv
import os
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData

# MQTT settings
mqttBroker = "127.0.0.1"
topic = "COMP5339/zzha9486_cleaned_prices"
username = "data_ingester"
csv_file_path = "fuel_prices.csv"  # Define the CSV file path

# Database settings
metadata = MetaData()
fuel_price_table = Table(
    'fuel_prices', metadata,
    Column('stationcode', String),
    Column('fueltype', String),
    Column('price', Float),
    Column('lastupdated', String)
)

def pgconnect():
    # Database connection parameters
    host = "localhost"
    user = "postgres"
    psw = "123456789"

    try:
        db = create_engine(f"postgresql+psycopg2://{user}:{psw}@{host}/{user}", echo=False)
        conn = db.connect()
        print("Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        print("Unable to connect to the database.")
        print(e)
        return None

def create_table_if_not_exists():
    conn = pgconnect()
    if conn:
        try:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS fuel_prices (
                stationcode VARCHAR(50) NOT NULL,
                fueltype VARCHAR(50) NOT NULL,
                price FLOAT NOT NULL,
                lastupdated VARCHAR(50) NOT NULL
            );
            """)
            print("Checked 'fuel_prices' table. It exists or was created successfully.")
        except Exception as e:
            print("Error checking or creating table 'fuel_prices'.")
            print(e)
        finally:
            conn.close()

def insert_data_to_db(data):
    conn = pgconnect()
    if conn:
        try:
            for fuel, stations in data.items():
                for station in stations:
                    ins = fuel_price_table.insert().values(
                        stationcode=station["stationcode"],
                        fueltype=station["fueltype"],
                        price=station["price"],
                        lastupdated=station["lastupdated"]
                    )
                    conn.execute(ins)
            print("Data inserted successfully.")
        except Exception as e:
            print("Error inserting data into the database.")
            print(e)
        finally:
            conn.close()

def save_data_to_csv(data):
    file_exists = os.path.isfile(csv_file_path)
    with open(csv_file_path, 'a', newline='') as csvfile:
        fieldnames = ['stationcode', 'fueltype', 'price', 'lastupdated']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()  # file doesn't exist yet, write a header
        
        for fuel, stations in data.items():
            for station in stations:
                writer.writerow({
                    'stationcode': station["stationcode"],
                    'fueltype': station["fueltype"],
                    'price': station["price"],
                    'lastupdated': station["lastupdated"]
                })
    print("Data saved to CSV file successfully.")

def on_message(client, userdata, message):
    print(f"Received message from {message.topic}:")
    payload = message.payload.decode("utf-8")
    print(payload)

    try:
        data = json.loads(payload)
        print(json.dumps(data, indent=4))
        insert_data_to_db(data)
        save_data_to_csv(data)
    except json.JSONDecodeError as e:
        print("Payload is not a valid JSON.")
        print(e)

# Check or create the table in the database
create_table_if_not_exists()

# MQTT client settings
client = mqtt.Client("Fuel_Price_Subscriber")
client.username_pw_set(username)
client.connect(mqttBroker)
client.subscribe(topic)
client.on_message = on_message

print("Connected to MQTT Broker.")
print(f"Subscribed to {topic}.")
print("Listening for incoming messages...")

# Keep listening for messages
client.loop_forever()

