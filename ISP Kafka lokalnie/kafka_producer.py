import pandas as pd
from confluent_kafka import Producer
import socket
import time

# Konfiguracja producenta Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',  # Adres serwera Kafka
    'client.id': socket.gethostname()
}

# Inicjalizacja producenta
producer = Producer(conf)

# Funkcja do wysyłania wiadomości do Kafki
def kafka_delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Funkcja do czytania pliku CSV i wysyłania danych do Kafki
def send_csv_to_kafka(csv_file, topic):
    # Wczytanie pliku CSV
    df = pd.read_csv(csv_file)

    # Iteracja przez każdą linię w DataFrame
    for index, row in df.iterrows():
        # Konwersja wiersza na ciąg znaków (np. JSON)
        value = row.to_json()

        # Wysyłanie wiadomości do Kafki
        producer.produce(topic, value.encode('utf-8'), callback=kafka_delivery_report)

        # Flush po każdej wiadomości, aby upewnić się, że wiadomość została wysłana
        producer.flush()

        print(f'Sent: {value}')
        time.sleep(5)
# Użycie funkcji
csv_file = 'data1.csv'  # Ścieżka do pliku CSV
topic = 'quickstart-events'    # Nazwa topiku w Kafka

send_csv_to_kafka(csv_file, topic)
