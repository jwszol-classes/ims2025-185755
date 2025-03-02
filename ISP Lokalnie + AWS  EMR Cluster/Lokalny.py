import requests
import pandas as pd
from IPython.display import display, clear_output
import time
from threading import Timer

# Funkcja do pobierania danych pogodowych
def fetch_weather_data(latitude, longitude):
    # Pobieranie danych na 7 dni do przodu
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m,rain,snowfall&forecast_days=7"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Błąd podczas pobierania danych.")
        return None

# Funkcja do przetwarzania danych
def process_weather_data(data):
    hourly_data = data['hourly']
    df = pd.DataFrame(hourly_data)
    df['time'] = pd.to_datetime(df['time'])
    return df

# Funkcja do wyświetlania danych w tabeli
def display_weather_data(df):
    pd.set_option('display.max_columns', None)  # Pokazuje wszystkie kolumny
    pd.set_option('display.width', 1000)        # Zwiększa szerokość wyświetlania
    clear_output(wait=True)  # Czyści poprzednie dane
    display(df)  # Wyświetla wszystkie dane (możesz użyć df.head(20) dla pierwszych 20 wierszy)

latitude = 52.52  # Szerokość geograficzna (np. Berlin)
longitude = 13.41  # Długość geograficzna

# Dynamiczne odświeżanie danych
def dynamic_weather_display(interval=60):
    data = fetch_weather_data(latitude, longitude)
    if data:
        df = process_weather_data(data)
        display_weather_data(df)
    Timer(interval, dynamic_weather_display, [interval]).start()

# Uruchom dynamiczne wyświetlanie danych z interwałem co 60 sekund
dynamic_weather_display()