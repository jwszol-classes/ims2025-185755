import requests
import pandas as pd
from IPython.display import display, clear_output
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import time
from threading import Timer

# Funkcja do pobierania danych pogodowych
def fetch_weather_data(latitude, longitude):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m,rain,snowfall"
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
    display(df.head(20))     # Wyświetla pierwsze 20 wierszy danych

# Funkcja do tworzenia wykresów
def create_plots(df):
    clear_output(wait=True)  # Czyści poprzednie wykresy

    fig = make_subplots(rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.1, subplot_titles=("Temperature (°C)", "Relative Humidity (%)", "Wind Speed (m/s)", "Rain (mm)", "Snowfall (mm)"))

    # Temperature plot
    fig.add_trace(go.Scatter(x=df['time'], y=df['temperature_2m'], mode='lines', name='Temperature (°C)'), row=1, col=1)

    # Relative Humidity plot
    fig.add_trace(go.Scatter(x=df['time'], y=df['relativehumidity_2m'], mode='lines', name='Relative Humidity (%)'), row=2, col=1)

    # Wind Speed plot
    fig.add_trace(go.Scatter(x=df['time'], y=df['windspeed_10m'], mode='lines', name='Wind Speed (m/s)'), row=3, col=1)

    # Rain plot
    fig.add_trace(go.Scatter(x=df['time'], y=df['rain'], mode='lines', name='Rain (mm)'), row=4, col=1)

    # Snowfall plot
    fig.add_trace(go.Scatter(x=df['time'], y=df['snowfall'], mode='lines', name='Snowfall (mm)'), row=5, col=1)

    fig.update_layout(height=1000, showlegend=False)
    fig.show()

latitude = 52.52  # Szerokość geograficzna (np. Berlin)
longitude = 13.41  # Długość geograficzna

# Dynamiczne odświeżanie danych
def dynamic_weather_display(interval=10):  # 10 sekund
    data = fetch_weather_data(latitude, longitude)
    if data:
        df = process_weather_data(data)
        display_weather_data(df)
        create_plots(df)
    Timer(interval, dynamic_weather_display, [interval]).start()

# Uruchom dynamiczne wyświetlanie danych z interwałem co 10 sekund
dynamic_weather_display()