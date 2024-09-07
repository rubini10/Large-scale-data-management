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
container_name = 'xxxxxxxxxxxxxxxxx'
account_url = "abfss://xxxxxxxxxxxxxxxxxxxxxxxx"

#create a client to interact with blob storage
connect_str = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
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


departments_df = session.read.csv("abfss://dati@clustersparkstoragea.dfs.core.windows.net/dipartimenti.csv", header=True)

end_time = time.time()

elapsed_time = end_time - start_time

minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("Load")
print(elapsed_time)
print("Tempo trascorso:", minutes, "minuti e", seconds, "secondi")

# JOIN

start_time = time.time()    

# Union with the final DataFrame df_final for PULocationID
# Rename columns to avoid conflicts
departments_df = departments_df.withColumnRenamed("LocationID", "DipLocationID") \
    .withColumnRenamed("Borough", "DipBorough")
df_finale = df_finale.withColumnRenamed("PULocationID", "PULocationID_temp") \
    .withColumnRenamed("DOLocationID", "DOLocationID_temp")

df_finale_dipartimenti = df_finale.join(departments_df, df_finale.PULocationID_temp == departments_df.DipLocationID, "left") \
    .drop("DipLocationID").withColumnRenamed("DipBorough", "PUBorough") \
    .join(departments_df, df_finale.DOLocationID_temp == departments_df.DipLocationID, "left") \
    .drop("DipLocationID").withColumnRenamed("DipBorough", "DOBorough")

end_time = time.time()

elapsed_time = end_time - start_time

minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("Merge")
print(elapsed_time)
print("Tempo trascorso:", minutes, "minuti e", seconds, "secondi")

# QUERY

start_time = time.time()  


df_finale_dipartimenti = df_finale_dipartimenti.filter("Passenger_count != 0")
df_finale_dipartimenti.createOrReplaceTempView("data")

query = """
    SELECT
        c.PUBorough,
        c.DOBorough,
        COUNT(c.PULocationID_temp) AS trips_number,
        AVG(c.total_amount) AS avg_total_amount,
        AVG(c.tip_amount) AS avg_tip_amount,
        AVG(c.fare_amount) AS avg_fare_ampunt,
        CASE WHEN c.PUBorough = c.DOBorough THEN 1 ELSE 0 END AS coincidence
    FROM
        data c
    GROUP BY
        c.PUBorough, c.DOBorough
"""


df = session.sql(query)


end_time = time.time()

elapsed_time = end_time - start_time

minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("Query")
print(elapsed_time)
print("Tempo trascorso:", minutes, "minuti e", seconds, "secondi")

# WRITE

start_time = time.time() 

df.write.parquet("abfss://dati@clustersparkstoragea.dfs.core.windows.net/output/output_distretti_3W.parquet")

end_time = time.time()

elapsed_time = end_time - start_time


minutes = int(elapsed_time // 60)
seconds = int(elapsed_time % 60)

print("Write")
print(elapsed_time)
print("Tempo trascorso:", minutes, "minuti e", seconds, "secondi")


#time.sleep(1000000000)

session.stop()
