from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import datetime

def kafka_consumer(group_id, bootstrap_servers='localhost:9092'):
    conf = {'bootstrap.servers': bootstrap_servers, 'group.id': group_id}
    min_values = {'temperature': 110, 'windspeed': 110, 'winddirection': 110}
    max_values = {'temperature': -1, 'windspeed': -1, 'winddirection': -1}
    sum_values = {'temperature': 0, 'windspeed': 0, 'winddirection': 0}
    count_values = {'temperature': 0, 'windspeed': 0, 'winddirection': 0}

    consumer = Consumer(conf)
    consumer.subscribe(['quickstart-events'])

    spark = SparkSession.builder \
        .appName("KafkaStreamingDemo") \
        .getOrCreate()

    schema = StructType([
        StructField("time", StringType(), True),
        StructField("interval", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("winddirection", DoubleType(), True),
        StructField("is_day", IntegerType(), True),
        StructField("weathercode", IntegerType(), True)
    ])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print("Consumer error:", msg.error())
                    break

            # Otrzymana wiadomość w formacie JSON
            json_data = msg.value().decode('utf-8')
            print(f"Received Data: {json_data}")  # Wyświetlanie otrzymanych danych JSON

            # Przetwarzanie danych
            json_df = spark.read.json(spark.sparkContext.parallelize([json_data]), schema)

            # Pobierz datę i czas z wiadomości (zakładając, że jest pole "time")
            for row in json_df.collect():
                received_time = row['time']  # Pobieramy czas z wiadomości (przykład: "2021-04-19T09:00")
                
                # Konwersja tekstu na obiekt datetime
                try:
                    message_time = datetime.strptime(received_time, "%Y-%m-%dT%H:%M")
                except ValueError:
                    print(f"Error parsing time: {received_time}")
                    message_time = datetime.now()  # Jeśli nie uda się parsować, używamy aktualnego czasu

                # Teraz możemy używać message_time w dalszej części kodu
                current_time = message_time.strftime("%Y-%m-%d %H:%M:%S")

                # Przetwarzanie danych numerycznych
                numerical_columns = ['temperature', 'windspeed', 'winddirection']
                for column in numerical_columns:
                    value = row[column]
                    min_values[column] = min(min_values[column], value)
                    max_values[column] = max(max_values[column], value)
                    sum_values[column] += value
                    count_values[column] += 1

            # Calculate averages
            avg_values = {key: sum_values[key] / count_values[key] if count_values[key] != 0 else None for key in sum_values}

            # Write to the output file
            with open("min_max_avg_values.txt", "a") as output_file:  # Use "a" to append to the file
                output_file.write(f"{current_time} - Min/Max/Avg Values:\n")
                for column in numerical_columns:
                    output_file.write(f"{column}: Min={min_values[column]}, Max={max_values[column]}, Avg={avg_values[column]}\n")

    except Exception as e:
        print("An error occurred:", e)
    finally:
        consumer.close()

if __name__ == "__main__":
    kafka_consumer('Weather')
