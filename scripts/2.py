import pyspark
from pyspark.sql import SparkSession
import pprint
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, FloatType, MapType
from pyspark.sql import Window
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import lead, udf, struct, col, from_json

def To_numb(x):
  x['is_open'] = int(x['is_open'])
  x['review_count'] = int(x['review_count'])
  x['latitude'] = float(x['latitude'])
  x['longitude'] = float(x['longitude'])
  x['stars'] = float(x['stars'])
  return x

#UDF
parse_dict = udf(lambda x: eval(x), MapType(StringType(), StringType()))


sc = pyspark.SparkContext()

#PACKAGE_EXTENSIONS= ('gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar')

bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquerry/pyspark_input'.format(bucket)
output_directory = 'gs://{}/pyspark_demo_output'.format(bucket)

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('Yelp_Businesses') \
  .getOrCreate()

conf={
      'mapred.bq.project.id':project,
      'mapred.bq.gcs.bucket':bucket,
      'mapred.bq.temp.gcs.path':input_directory,
      'mapred.bq.input.project.id': "final-project-cs512-381002",
      'mapred.bq.input.dataset.id': 'Final_project_dataset',
      'mapred.bq.input.table.id': 'yelp_academic_dataset_business',
  }

## pull table from big query
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf = conf)

## convert table to a json like object, turn PosTime and Fseen back into numbers... not sure why they changed
vals = table_data.values()
vals = vals.map(lambda line: json.loads(line))
vals = vals.map(To_numb)

#schema
schema = StructType([
    StructField("business_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("hours", StringType(), True),
    StructField("is_open", IntegerType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("name", StringType(), True), 
    StructField("review_count", IntegerType(), True),
    StructField("stars", FloatType(), True),
    StructField("categories", StringType(), True),
    StructField("state", StringType(), True)
])

## creating dataframe
df1 = spark.createDataFrame(vals, schema=schema)

df1 = df1.withColumn("hours_dict", parse_dict("hours"))

df1.show(10, truncate=False)

filtered_businesses = df1.filter((col("stars") >= 4) & (col("hours_dict").getItem('Sunday').isNotNull()))

filtered_businesses.show(10,truncate=False)

# Group the filtered businesses by state and count the number of businesses
business_count = filtered_businesses.groupBy("state").count()

business_count = business_count.withColumnRenamed('state','U.S. State')
business_count = business_count.withColumnRenamed('count','No. of Businesses opened on Sunday & highly rated')

# Show the results
business_count.show(20, truncate=False)

## deletes the temporary files
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)n