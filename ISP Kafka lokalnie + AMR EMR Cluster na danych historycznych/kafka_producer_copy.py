import json
import time
import requests
from confluent_kafka import Producer

# Konfiguracja Kafka
KAFKA_BROKER = "localhost:9092"
TOPIC = "quickstart-events"

producer_config = {
    "bootstrap.servers": KAFKA_BROKER
}

producer = Producer(producer_config)

# Konfiguracja Open-Meteo
API_URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {
    "latitude": 52.2298,  # Warszawa
    "longitude": 21.0122,
    "current_weather": "true"
}

def fetch_weather():
    """Pobiera dane pogodowe z Open-Meteo."""
    try:
        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status()
        return response.json().get("current_weather", {})
    except requests.RequestException as e:
        print(f"Błąd podczas pobierania danych pogodowych: {e}")
        return None

def delivery_report(err, msg):
    """Obsługuje potwierdzenia dostarczenia wiadomości Kafka."""
    if err is not None:
        print(f"Błąd dostarczenia wiadomości: {err}")
    else:
        print(f"Wysłano wiadomość: {msg.value()} na {msg.topic()} [partycja {msg.partition()}]")

if __name__ == "__main__":
    while True:
        weather_data = fetch_weather()
        if weather_data:
            message = json.dumps(weather_data)
            producer.produce(TOPIC, value=message, callback=delivery_report)
            producer.flush()  # Wysyła dane natychmiast
        time.sleep(60)  # Pobieranie danych co minutę
