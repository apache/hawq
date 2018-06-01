#
# Used to convert the Open Street Map dump data, which I have in .gz files,
# delimited by '<', in S3
#
# To to this, I first set up a Spark cluster in Amazon EC2 "EMR", then logged
# into the master node via SSH, then started the pyspark shell.
#
# See:
#   http://spark.apache.org/docs/latest/sql-programming-guide.html#programmatically-specifying-the-schema
#   https://spark.apache.org/docs/1.6.2/api/python/_modules/pyspark/sql/types.html
#   https://stackoverflow.com/questions/33129918/pyspark-typeerror-integertype-can-not-accept-object-in-type-type-unicode
#

import datetime

# Handle potential junk data for FLOAT type
def get_float(val, defaultVal=0.0):
  try:
    rv = float(val)
  except ValueError:
    rv = defaultVal
  return rv

# And, for LONG type
def get_long(val, defaultVal=-1L):
  try:
    rv = long(val)
  except ValueError:
    rv = defaultVal
  return rv

lines = sc.textFile('s3://goddard.datasets/osm_exerpt_0*')
lines.count() # Should be 5327647
cols = lines.map(lambda l: l.split('<'))

typedCols = cols.map(lambda c: (
  get_long(c[0], -1L), # LONG
  datetime.datetime.strptime(c[1], '%Y-%m-%dT%H:%M:%SZ'), # TIMESTAMP
  c[2], # STRING
  get_float(c[3], 91.0), # FLOAT
  get_float(c[4], 181.0),
  c[5],
  c[6]))
df = spark.createDataFrame(typedCols)

# Writes 8 files into this bucket.  The bucket must exist, but the "from-spark" part will be created
# if it doesn't exist.
df.write.parquet('s3a://pxf-s3-devel/from-spark', mode='overwrite')

# Downsample this data set, then take 1k rows and save it for future testing.  The resulting
# file is here (in this repo): ./data/sample_from_pyspark.snappy.parquet
df = df.sample(False, 0.0002, seed=137).limit(1000)
df.write.parquet('s3a://pxf-s3-devel/sample-from-spark', mode='overwrite')

