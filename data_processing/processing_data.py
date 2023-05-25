from pyspark.sql.functions import col
from pyspark.sql.functions import expr


# "s3://masterscraperbucket/data/test.parquet"

class Rowprocessor:

    def __init__(self, spark, input_path):
        self.spark = spark
        self.input_path = input_path

    def get_data(self):
        data = self.spark.read.parquet(self.input_path)
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
