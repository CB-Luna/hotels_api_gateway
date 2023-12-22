import pyspark
import csv
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, IntegerType, StringType, FloatType
from pyspark.sql.functions import col, avg, sum
from pyspark.sql import Row
from pyspark.sql import SQLContext
from datetime import datetime
from dateutil import tz

 # Crea una sesión de Spark
spark = SparkSession.builder.appName("MesesPorImporteTotal").getOrCreate()
# Crea un SQLContext a partir de la sesión de Spark
sqlContext = SQLContext(spark)
# Define el esquema personalizado
schema = StructType([
    StructField("rev_parup", IntegerType(), False),
    StructField("super_chain_name", StringType(), False),
    StructField("property_name", StringType(), False),
    StructField("street_address", StringType(), False),
    StructField("hotel_country", StringType(), False),
    StructField("region", StringType(), False),
    StructField("number_rooms", IntegerType(), False),
    StructField("room_nights", IntegerType(), False),
    StructField("supplemental_charges", FloatType(), False),
    StructField("total_hotel_cost", FloatType(), False),
    StructField("total_hotel_charges", FloatType(), False),
    StructField("booking_date", StringType(), False)
])

# URL del servidor que contiene el archivo CSV remoto
servidor_remoto_url = "https://lagiinfnnpqlsydoriqt.supabase.co/storage/v1/object/public/hotels/source/"
# Nombre del archivo CSV en el servidor remoto
archivo_csv_remoto = "Historical-Data-Hotels.csv?t=2023-12-22T16%3A38%3A24.734Z"
# URL completa del archivo CSV en el servidor remoto
url_csv_remoto = servidor_remoto_url + archivo_csv_remoto

# Función principal del Proceso
if __name__ == "__main__":
    try:
        print("Inicia Proceso de recuperación")
        # Realiza una solicitud HTTP GET para obtener el contenido del archivo CSV
        response = requests.get(url_csv_remoto)
        # Verifica si la solicitud se completó exitosamente
        if response.status_code == 200:
            print("Sí se pudo recuperar el archivo CSV")
            # Decodifica el contenido de la respuesta como texto CSV
            csv_content = response.text

            # Analiza el contenido CSV
            csv_reader = csv.reader(csv_content.splitlines())
            # Salta las primeras tres filas
            next(csv_reader)
            next(csv_reader)
            next(csv_reader)
            rows = list(csv_reader)
            # Convierte las filas a objetos Row con el esquema especificado
            data_rows = [Row(
                rev_parup=int(row[0]) if row[0] else 0,  # Convierte a Integer, maneja el caso en que sea None o vacío
                super_chain_name=row[4] if row[4] else "NO_SUPER_CHAIN_NAME",
                property_name=row[7] if row[7] else "NO_PROPERTY_NAME",
                street_address=row[8] if row[8] else "NO_STREET_ADDRESS",
                hotel_country=row[23] if row[23] else "NO_HOTEL_COUNTRY",
                region=row[24] if row[24] else "NO_REGION",
                number_rooms=int(row[31]) if row[31] else 0,  # Convierte a Integer, maneja el caso en que sea None o vacío
                room_nights=int(row[32]) if row[32] else 0,  # Convierte a Integer, maneja el caso en que sea None o vacío
                supplemental_charges=float(row[34]) if row[34] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                total_hotel_cost=float(row[33]) if row[33] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                total_hotel_charges=float(row[35]) if row[35] else 0.0,  # Convierte a Float, maneja el caso en que sea None o vacío
                booking_date=row[2] if row[2] else "NO_BOOKING_DATE",
                ) for row in rows]
            # Crea un DataFrame a partir de las filas y el esquema
            df = spark.createDataFrame(data_rows, schema=schema)
            df.show(4)
            spark.stop()
        else:
            print(f"No se pudo obtener el archivo CSV del servidor remoto. Código de estado: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la solicitud HTTP: {e}")
    except Exception as e:
        print(f"Ocurrió un error: {e}")