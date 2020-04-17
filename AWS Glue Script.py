import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import trim, regexp_replace, col
from awsglue.dynamicframe import DynamicFrame
import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "airbnb-listings", table_name = "airbnb_staging", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "airbnb-listings", table_name = "airbnb_staging", transformation_ctx = "datasource0")

df = datasource0.toDF()
df = df.withColumn("cleaning_fee", trim(df.cleaning_fee)).withColumn("extra_people", trim(df.extra_people)).withColumn("monthly_price", trim(df.monthly_price)).withColumn("price", trim(df.price)).withColumn("security_deposit", trim(df.security_deposit)).withColumn("weekly_price", trim(df.weekly_price)).withColumn("host_response_rate", trim(df.host_response_rate))

df = df.fillna('0', ['cleaning_fee', 'extra_people', 'monthly_price', 'price', 'security_deposit', 'weekly_price'])
df = df.fillna(datetime.datetime.now().strftime("%M/%d/%Y"), ['last_scraped'])
print("Done Filling zeros")
print(df.columns)

df = df.withColumn("cleaning_fee",regexp_replace(col("cleaning_fee"), "[^\d*\.?\d+]", ""))\
        .withColumn("extra_people",regexp_replace(col("extra_people"), "[^\d*\.?\d+]", ""))\
        .withColumn("monthly_price",regexp_replace(col("monthly_price"), "[^\d*\.?\d+]", ""))\
        .withColumn("price",regexp_replace(col("price"), "[^\d*\.?\d+]", ""))\
        .withColumn("security_deposit",regexp_replace(col("security_deposit"), "[^\d*\.?\d+]", ""))\
        .withColumn("weekly_price",regexp_replace(col("weekly_price"), "[^\d*\.?\d+]", ""))\
        .withColumn("weekly_price",regexp_replace(col("weekly_price"), "N/A", ""))\
        .withColumn("host_response_rate",regexp_replace(col("host_response_rate"), "[^\d+]", ""))
print("cleaning_fee Head")
df.show()
datasource1 = DynamicFrame.fromDF(df, glueContext, "nested")

## @type: ApplyMapping
## @args: [mapping = [("id", "long", "id", "long"), ("last_scraped", "string", "last_scraped", "string"), ("host_id", "long", "host_id", "long"), ("host_name", "string", "host_name", "string"), ("host_since", "string", "host_since", "string"), ("host_response_time", "string", "host_response_time", "string"), ("host_response_rate", "string", "host_response_rate", "string"), ("host_acceptance_rate", "string", "host_acceptance_rate", "string"), ("host_neighbourhood", "string", "host_neighbourhood", "string"), ("host_total_listings_count", "double", "host_total_listings_count", "double"), ("host_identity_verified", "string", "host_identity_verified", "string"), ("neighbourhood_group_cleansed", "string", "neighbourhood_group_cleansed", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("zipcode", "long", "zipcode", "long"), ("country_code", "string", "country_code", "string"), ("country", "string", "country", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("property_type", "string", "property_type", "string"), ("room_type", "string", "room_type", "string"), ("accommodates", "long", "accommodates", "long"), ("bathrooms", "double", "bathrooms", "double"), ("bedrooms", "double", "bedrooms", "double"), ("beds", "double", "beds", "double"), ("bed_type", "string", "bed_type", "string"), ("square_feet", "double", "square_feet", "double"), ("price", "string", "price", "string"), ("weekly_price", "string", "weekly_price", "string"), ("monthly_price", "string", "monthly_price", "string"), ("security_deposit", "string", "security_deposit", "string"), ("cleaning_fee", "string", "cleaning_fee", "string"), ("guests_included", "long", "guests_included", "long"), ("extra_people", "string", "extra_people", "string"), ("calendar_last_scraped", "string", "calendar_last_scraped", "string"), ("number_of_reviews", "long", "number_of_reviews", "long"), ("first_review", "string", "first_review", "string"), ("last_review", "string", "last_review", "string"), ("review_scores_value", "double", "review_scores_value", "double"), ("instant_bookable", "string", "instant_bookable", "string"), ("is_business_travel_ready", "string", "is_business_travel_ready", "string"), ("cancellation_policy", "string", "cancellation_policy", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource1, mappings = [("id", "long", "id", "long"), ("last_scraped", "string", "last_scraped", "timestamp"), ("host_id", "long", "host_id", "long"), ("host_name", "string", "host_name", "string"), ("host_since", "string", "host_since", "string"), ("host_response_time", "string", "host_response_time", "string"), ("host_response_rate", "string", "host_response_rate", "float"), ("host_acceptance_rate", "string", "host_acceptance_rate", "string"), ("host_neighbourhood", "string", "host_neighbourhood", "string"), ("host_total_listings_count", "double", "host_total_listings_count", "double"), ("host_identity_verified", "string", "host_identity_verified", "string"), ("neighbourhood_group_cleansed", "string", "neighbourhood_group_cleansed", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("zipcode", "long", "zipcode", "long"), ("country_code", "string", "country_code", "string"), ("country", "string", "country", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("property_type", "string", "property_type", "string"), ("room_type", "string", "room_type", "string"), ("accommodates", "long", "accommodates", "long"), ("bathrooms", "double", "bathrooms", "double"), ("bedrooms", "double", "bedrooms", "double"), ("beds", "double", "beds", "double"), ("bed_type", "string", "bed_type", "string"), ("square_feet", "double", "square_feet", "double"), ("price", "string", "price", "float"), ("weekly_price", "string", "weekly_price", "float"), ("monthly_price", "string", "monthly_price", "float"), ("security_deposit", "string", "security_deposit", "float"), ("cleaning_fee", "string", "cleaning_fee", "float"), ("guests_included", "long", "guests_included", "long"), ("extra_people", "string", "extra_people", "float"), ("calendar_last_scraped", "string", "calendar_last_scraped", "string"), ("number_of_reviews", "long", "number_of_reviews", "long"), ("first_review", "string", "first_review", "timestamp"), ("last_review", "string", "last_review", "timestamp"), ("review_scores_value", "double", "review_scores_value", "double"), ("instant_bookable", "string", "instant_bookable", "string"), ("is_business_travel_ready", "string", "is_business_travel_ready", "string"), ("cancellation_policy", "string", "cancellation_policy", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://airbnb-final"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "s3://airbnb-final"}, format = "parquet", transformation_ctx = "datasink4")
job.commit()