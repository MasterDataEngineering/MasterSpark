
from data_processing.cli import parser
from pyspark.sql import SparkSession
from data_processing.processing_data import Rowprocessor

args = parser.parse_args()
input_dir = args.input_path
output_dir = args.output_path
partitions = args.output_partitions

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


processor = Rowprocessor(spark=spark, input_path=input_dir)

dirty_data = processor.get_data()
clean_data = processor.process_data(dirty_data)
clean_data.coalesce(partitions).write.parquet("s3://masterscraperbucket/clean_data/")

