import pandas as pd
import pyarrow as pa
import pyarrow.parquet

df=pd.read_json("/home/peppe/Master-DE-Scaper/datatry.json")

table_write = pa.Table.from_pandas(df, preserve_index=False)
pyarrow.parquet.write_table(table_write, 's3://masterscraperbucket/data/test.parquet')
##table_read = pyarrow.parquet.read_table('test.parquet')
##table_read.to_pandas()