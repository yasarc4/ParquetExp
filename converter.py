from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import sys
import os
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
try:
	filepath = sys.argv[1].strip()
	output_filename = filepath.strip().split('/')[-1]
	df = spark.read.parquet(filepath)
	pandas_df = df.limit(1000).toPandas()
	pandas_df.to_csv(output_filename+'.csv')
	print('\nSuccess!!!\n\nThe CSV file is found here..\n'+os.getcwd()+'/'+output_filename+'.csv\n')
except:
	print ("please provide the parquet path in commandline") 
	sys.exit(1)
print('*'*100)
print("Exiting!!!")
