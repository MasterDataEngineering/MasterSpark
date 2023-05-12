from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import expr

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


class Rowprocessor:

    def __init__(self,spark):
        self.spark=spark

    def get_data(self):
        data = self.spark.read.parquet("s3://masterscraperbucket/data/test.parquet")
        return data

    def process_data(self, data):
        filtered_data = data \
            .withColumn("company_name", col("data.companyName")) \
            .withColumn("description", col("data.description")) \
            .withColumn("employements_status", col("data.employmentStatus")) \
            .withColumn("experience_level", col("data.experienceLevel")) \
            .withColumn("site", col("data.basicCompanyInfo.headquarters")) \
            .withColumn("industries", col("data.industries")) \
            .withColumn("Included", col("included")) \
            .withColumn("mini_job", expr("filter(included, x -> x.`$type` like \"%MiniJob%\")").getItem(0)) \
            .withColumn("job_title", col("mini_job").title) \
            .drop(col("mini_job")) \
            .drop(col("data")) \
            .drop(col("included"))
        return filtered_data

