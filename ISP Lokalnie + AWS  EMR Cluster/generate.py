import pandas as pd

# Dane przykładowe
data = {
    'column1': [1, 2, 3],
    'column2': ['A', 'B', 'C']
}

# Tworzenie DataFrame
df = pd.DataFrame(data)

# Zapis do pliku CSV
df.to_csv('data.csv', index=False)

print('Plik data.csv został wygenerowany.')
