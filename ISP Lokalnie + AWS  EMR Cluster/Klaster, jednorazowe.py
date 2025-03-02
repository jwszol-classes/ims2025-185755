sc.install_pypi_package("ipywidgets")
sc.install_pypi_package("jupyterlab_widgets")
sc.install_pypi_package("openmeteo-requests")
sc.install_pypi_package("requests-cache")
sc.install_pypi_package("retry-requests")
sc.install_pypi_package("numpy")
sc.install_pypi_package("pandas")
sc.install_pypi_package("matplotlib")
sc.install_pypi_package("boto3")
sc.install_pypi_package("IPython")
sc.list_packages()






import requests
import pandas as pd
from IPython.display import display

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
    display(df.head(20))  # Wyświetla pierwsze 20 wierszy danych

# Pobranie i wyświetlenie danych jednorazowo
latitude = 52.52  # Szerokość geograficzna (np. Berlin)
longitude = 13.41  # Długość geograficzna

data = fetch_weather_data(latitude, longitude)
if data:
    df = process_weather_data(data)
    display_weather_data(df)