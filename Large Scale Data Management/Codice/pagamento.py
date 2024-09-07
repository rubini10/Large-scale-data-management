from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType, LongType
import time
import itertools

# spark cluster standalone
session = SparkSession.builder.config("spark.jars", "C:\jar\hadoop-aws-3.3.1.jar, C:\jar\hadoop-common-3.3.1.jar, C:\jar\jetty-util-ajax-9.4.51.v20230217.jar, C:\jar\jetty-util-9.3.24.v20180605.jar, C:\jar\hadoop-azure-3.3.1.jar, C:\jar/azure-storage-8.6.6.jar").master("spark://10.9.22.11:7077").appName("Distretti").getOrCreate()
# local mode
#session = SparkSession.builder.config("spark.jars", "C:\jar\hadoop-aws-3.3.1.jar, C:\jar\hadoop-common-3.3.1.jar, C:\jar\jetty-util-ajax-9.4.51.v20230217.jar, C:\jar\jetty-util-9.3.24.v20180605.jar, C:\jar\hadoop-azure-3.3.1.jar, C:\jar/azure-storage-8.6.6.jar").getOrCreate()

session.conf.set(
    "fs.azure.account.key.clustersparkstoragea.dfs.core.windows.net",
	"bchJyDadU8wc3r7vZTjG9IuM59/Zr8ghPcgVTqfBKsm0AV+Xm0cq4vaOC6YWxRBcKK0UyEwsPyGD+AStQF2mfA=="
)

#enter credentials
account_name = 'clustersparkstoragea'
account_key = 'bchJyDadU8wc3r7vZTjG9IuM59/Zr8ghPcgVTqfBKsm0AV+Xm0cq4vaOC6YWxRBcKK0UyEwsPyGD+AStQF2mfA=='
container_name = 'dati'

account_url = "abfss://dati@clustersparkstoragea.dfs.core.windows.net"


#create a client to interact with blob storage
connect_str = "DefaultEndpointsProtocol=https;AccountName=clustersparkstoragea;AccountKey=bchJyDadU8wc3r7vZTjG9IuM59/Zr8ghPcgVTqfBKsm0AV+Xm0cq4vaOC6YWxRBcKK0UyEwsPyGD+AStQF2mfA==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

#use the client to connect to the container
container_client = blob_service_client.get_container_client(container_name)

#get a list of all blob files in the container
blob_list = []
for blob_i in container_client.list_blobs():
    blob_list.append(blob_i.name)

# LOAD

start_time = time.time()     

schema = StructType([
    StructField("VendorID", DoubleType(), nullable=True),
    StructField("tpep_pickup_datetime", TimestampType(), nullable=True),
    StructField("tpep_dropoff_datetime", TimestampType(), nullable=True),
    StructField("passenger_count", DoubleType(), nullable=True),
    StructField("trip_distance", DoubleType(), nullable=True),
    StructField("RatecodeID", DoubleType(), nullable=True),
    StructField("store_and_fwd_flag", StringType(), nullable=True),
    StructField("PULocationID", LongType(), nullable=True),
    StructField("DOLocationID", LongType(), nullable=True),
    StructField("payment_type", LongType(), nullable=True),
    StructField("fare_amount", DoubleType(), nullable=True),
    StructField("extra", DoubleType(), nullable=True),
    StructField("mta_tax", DoubleType(), nullable=True),
    StructField("tip_amount", DoubleType(), nullable=True),
    StructField("tolls_amount", DoubleType(), nullable=True),
    StructField("improvement_surcharge", DoubleType(), nullable=True),
    StructField("total_amount", DoubleType(), nullable=True),
    StructField("congestion_surcharge", DoubleType(), nullable=True),
    StructField("airport_fee", DoubleType(), nullable=True)
])


# parquet file union
df_finale = session.createDataFrame([], schema)
blob_list = container_client.list_blobs()
for blob in blob_list:
    if blob.name.endswith(".parquet") and not(blob.name.startswith("output")):
        blob_path = f"{account_url}/{blob.name}"
        sdf = session.read.parquet(blob_path)
        df_finale = df_finale.union(sdf)

end_time = time.time()

elapsed_time = end_time - start_time

minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("Load")
print(elapsed_time)
print("Tempo trascorso:", minutes, "minuti e", seconds, "secondi")

# QUERY

start_time = time.time()  

df = df_finale.filter("Passenger_count != 0")
df.createOrReplaceTempView("data")


query = """
    SELECT
        payment_type,
        AVG(trip_distance) AS avg_trip_distance,
        AVG(fare_amount) AS avg_fare_amount,
        AVG(tip_amount) AS avg_tip_amount,
        AVG(total_amount) AS avg_total_amount,
        COUNT(passenger_count) AS num_trips
    FROM data
    GROUP BY payment_type
"""

result = session.sql(query)

end_time = time.time()

elapsed_time = end_time - start_time

minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("Query")
print(elapsed_time)
print("Tempo trascorso:", minutes, "minuti e", seconds, "secondi")

# WRITE

start_time = time.time() 

result.write.parquet("abfss://dati@clustersparkstoragea.dfs.core.windows.net/output/output_pagamento_3W_100.parquet")

end_time = time.time()

elapsed_time = end_time - start_time

minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("Write")
print(elapsed_time)
print("Tempo trascorso:", minutes, "minuti e", seconds, "secondi")

#time.sleep(1000000000)

session.stop()