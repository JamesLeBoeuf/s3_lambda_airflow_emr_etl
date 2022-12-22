#!/usr/bin/env python
import argparse
import ast
import pathlib
import json

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--params', required=True, help='Spark Input Parameters')
args = parser.parse_args()

calendar_schema = StructType([
	StructField('CAL_DT', DateType(), False),
	StructField('CAL_TYPE_DESC', StringType(), True),
	StructField('DAY_OF_WK_NUM', IntegerType(), True),
	StructField('DAY_OF_WK_DESC', StringType(), True),
	StructField('YR_NUM', IntegerType(), True),
	StructField('WK_NUM', IntegerType(), True),
	StructField('YR_WK_NUM', IntegerType(), True),
	StructField('MNTH_NUM', IntegerType(), True),
	StructField('YR_MNTH_NUM', IntegerType(), True),
	StructField('QTR_NUM', IntegerType(), True),
	StructField('YR_QTR_NUM', IntegerType(), True)
])

inventory_schema = StructType([
	StructField('CAL_DT', DateType(), False),
	StructField('STORE_KEY', IntegerType(), True),
	StructField('PROD_KEY', IntegerType(), True),
	StructField('INVENTORY_ON_HAND_QTY', DoubleType(), True),
	StructField('INVENTORY_ON_ORDER_QTY', DoubleType(), True),
	StructField('OUT_OF_STOCK_FLG', IntegerType(), True),
	StructField('WASTE_QTY', DoubleType(), True),
	StructField('PROMOTION_FLG', BooleanType(), True),
	StructField('NEXT_DELIVERY_DT', DateType(), True)
])

product_schema = StructType([
	StructField('PROD_KEY', IntegerType(), False),
	StructField('PROD_NAME', StringType(), True),
	StructField('VOL', DoubleType(), True),
	StructField('WGT', DoubleType(), True),
	StructField('BRAND_NAME', StringType(), True),
	StructField('STATUS_CODE', IntegerType(), True),
	StructField('STATUS_CODE_NAME', StringType(), True),
	StructField('CATEGORY_KEY', IntegerType(), True),
	StructField('CATEGORY_NAME', StringType(), True),
	StructField('SUBCATEGORY_KEY', IntegerType(), True),
	StructField('SUBCATEGORY_NAME', StringType(), True)
])

sales_schema = StructType([
	StructField('TRANS_ID', IntegerType(), False),
	StructField('PROD_KEY', IntegerType(), True),
	StructField('STORE_KEY', IntegerType(), True),
	StructField('TRANS_DT', DateType(), True),
	StructField('TRANS_TIME', IntegerType(), True),
	StructField('SALES_QTY', DoubleType(), True),
	StructField('SALES_PRICE', DoubleType(), True),
	StructField('SALES_AMT', DoubleType(), True),
	StructField('DISCOUNT', DoubleType(), True),
	StructField('SALES_COST', DoubleType(), True),
	StructField('SALES_MGRN', DoubleType(), True),
	StructField('SHIP_COST', DoubleType(), True)
])

store_schema = StructType([
	StructField('STORE_KEY', IntegerType(), False),
	StructField('STORE_NUM', IntegerType(), True),
	StructField('STORE_DESC', StringType(), True),
	StructField('ADDR', StringType(), True),
	StructField('CITY', StringType(), True),
	StructField('REGION', StringType(), True),
	StructField('CNTRY_CD', StringType(), True),
	StructField('CNTRY_NM', StringType(), True),
	StructField('POSTAL_ZIP_CD', StringType(), True),
	StructField('PROV_STATE_DESC', StringType(), True),
	StructField('PROV_STATE_CD', StringType(), True),
	StructField('STORE_TYPE_CD', StringType(), True),
	StructField('STORE_TYPE_DESC', StringType(), True),
	StructField('FRNCHS_FLG', StringType(), True),
	StructField('STORE_SIZE', StringType(), True),
	StructField('MARKET_KEY', IntegerType(), True),
	StructField('MARKET_NAME', StringType(), True),
	StructField('SUBMARKET_KEY', IntegerType(), True),
	StructField('SUBMARKET_NAME', StringType(), True),
	StructField('LATITUDE', DecimalType(precision=10, scale=6), True),
	StructField('LONGITUDE', DecimalType(precision=10, scale=6), True)
])

def create_daily_sales_stg_table(sales_df):
	daily_sales_stg = sales_df.groupBy(
		col('TRANS_DT').alias('CAL_DT'),
		col('STORE_KEY').alias('STORE_KEY'),
		col('PROD_KEY').alias('PROD_KEY')
	)

	daily_sales_stg = daily_sales_stg.agg(
		F.sum('SALES_QTY').alias('SALES_QTY'),
		F.sum('SALES_AMT').alias('SALES_AMT'),
		F.avg('SALES_PRICE').alias('SALES_PRICE'),
		F.sum('SALES_COST').alias('SALES_COST'),
		F.sum('SALES_MGRN').alias('SALES_MGRN'),
		F.avg('DISCOUNT').alias('DISCOUNT'),
		F.sum('SHIP_COST').alias('SHIP_COST')
	)

	daily_sales_stg = daily_sales_stg.orderBy(
		col('CAL_DT'),
		col('STORE_KEY'),
		col('PROD_KEY')
	)

	return daily_sales_stg

def create_daily_inventory_stg_table(inventory_df):
	daily_inventory_stg = inventory_df \
	.withColumn('CAL_DT', col('CAL_DT')) \
	.withColumn('STORE_KEY', col('STORE_KEY')) \
	.withColumn('PROD_KEY', col('PROD_KEY')) \
	.withColumnRenamed('INVENTORY_ON_HAND_QTY', 'STOCK_ON_HAND_QTY') \
	.withColumnRenamed('INVENTORY_ON_ORDER_QTY', 'ORDERED_STOCK') \
	.withColumn('OUT_OF_STOCK_FLG', col('OUT_OF_STOCK_FLG')) \
	.withColumn('WASTE_QTY', col('WASTE_QTY')) \
	.withColumn('PROMOTION_FLG', col('PROMOTION_FLG')) \
	.withColumn('NEXT_DELIVERY_DT', col('NEXT_DELIVERY_DT'))

	return daily_inventory_stg

def create_sales_inv_store_dy_table(daily_sales_stg, daily_inventory_stg):
	sales_inv_joined_df = daily_sales_stg.alias('sales').join(
		daily_inventory_stg.alias('inventory'),
		[col('sales.CAL_DT') == col('inventory.CAL_DT'),
		col('sales.STORE_KEY') == col('inventory.STORE_KEY'),
		col('sales.PROD_KEY') == col('inventory.PROD_KEY')],
		how='full'
	)

	sales_inv_store_dy = sales_inv_joined_df.select(
		F.coalesce(col('sales.CAL_DT'), col('inventory.CAL_DT')).alias('CAL_DT'),
		F.coalesce(col('sales.STORE_KEY'), col('inventory.STORE_KEY')).alias('STORE_KEY'),
		F.coalesce(col('sales.PROD_KEY'), col('inventory.PROD_KEY')).alias('PROD_KEY'),
		F.coalesce(col('sales.SALES_QTY'), F.lit(0)).alias('SALES_QTY'),
		F.coalesce(col('sales.SALES_PRICE'), F.lit(0)).alias('SALES_PRICE'),
		F.coalesce(col('sales.SALES_AMT'), F.lit(0)).alias('SALES_AMT'),
		F.coalesce(col('sales.DISCOUNT'), F.lit(0)).alias('DISCOUNT'),
		F.coalesce(col('sales.SALES_COST'), F.lit(0)).alias('SALES_COST'),
		F.coalesce(col('sales.SALES_MGRN'), F.lit(0)).alias('SALES_MGRN'),
		F.coalesce(col('inventory.STOCK_ON_HAND_QTY'), F.lit(0)).alias('STOCK_ON_HAND_QTY'),
		F.coalesce(col('inventory.ORDERED_STOCK'), F.lit(0)).alias('ORDERED_STOCK_QTY'),
		F.when(col('inventory.OUT_OF_STOCK_FLG') == F.lit(1), True).otherwise(False).alias('OUT_OF_STOCK_FLG'),
		F.when(col('inventory.OUT_OF_STOCK_FLG') == F.lit(1), False).otherwise(True).alias('IN_STOCK_FLG'),
		F.when(col('inventory.STOCK_ON_HAND_QTY') < col('sales.SALES_QTY'), True).otherwise(False).alias('LOW_STOCK_FLG')
	)

	return sales_inv_store_dy

def create_sales_inv_store_wk_table(sales_inv_store_dy, calendar_df):
	sales_inv_store_wk = sales_inv_store_dy.alias('sales_day').join(
		calendar_df.alias('calendar'),
		[col('sales_day.CAL_DT') == col('calendar.CAL_DT')]
	)

	sales_inv_store_wk = sales_inv_store_wk.groupBy(
		col('YR_NUM'),
		col('WK_NUM'),
		col('STORE_KEY'),
		col('PROD_KEY')    
	)

	sales_inv_store_wk = sales_inv_store_wk.agg(
		F.sum('SALES_QTY').alias('WK_SALES_QTY'),
		F.avg('SALES_PRICE').alias('AVG_SALES_PRICE'),
		F.sum('SALES_AMT').alias('WK_SALES_AMT'),
		F.sum('DISCOUNT').alias('WK_DISCOUNT'),
		F.sum('SALES_COST').alias('WK_SALES_COST'),
		F.sum('SALES_MGRN').alias('WK_SALES_MGRN'),
		F.sum(F.when(col('calendar.DAY_OF_WK_NUM') == F.lit(6), col('sales_day.STOCK_ON_HAND_QTY')).otherwise(F.lit(0))).alias('EOP_STOCK_ON_HAND_QTY'),
		F.sum(F.when(col('calendar.DAY_OF_WK_NUM') == F.lit(6), col('sales_day.ORDERED_STOCK_QTY')).otherwise(F.lit(0))).alias('EOP_ORDERED_STOCK_QTY'),
		# count doesn't sum Trues, it only counts the number of non null values. To count the True values, you need to convert the conditions to 1 / 0 and then sum:
		F.sum(F.when(col('sales_day.OUT_OF_STOCK_FLG') == True, F.lit(1)).otherwise(F.lit(0))).alias('OUT_OF_STOCK_TIMES'),
		F.sum(F.when(col('sales_day.IN_STOCK_FLG') == True, F.lit(1)).otherwise(F.lit(0))).alias('IN_STOCK_TIMES'),
		F.sum(F.when(col('sales_day.LOW_STOCK_FLG') == True, F.lit(1)).otherwise(F.lit(0))).alias('LOW_STOCK_TIMES')
	)

	sales_inv_store_wk = sales_inv_store_wk.orderBy(
		col('YR_NUM'),
		col('WK_NUM'),
		col('STORE_KEY'),
		col('PROD_KEY') 
	)

	return sales_inv_store_wk

def parse_command_line(args):
	# Convert to key value pair
	return ast.literal_eval(args)

def create_spark_session(name='ReadWriteVal'):
    spark = SparkSession \
			.builder \
        	.appName(name) \
        	.getOrCreate()
    spark.sparkContext.setLogLevel('DEBUG')
    return spark

def clean_df(df):
	return df.fillna(value=0).na.fill('None')

def create_df(spark, schema, filepath):
  df = spark.read \
    .option('header', True) \
    .schema(schema) \
    .csv(filepath)
  return clean_df(df)

def save_csv(df, output_path):
	df.repartition(1) \
		.write \
		.mode('overwrite') \
		.option('header', True) \
		.csv(output_path)

def save_parquet(df, partition_column, output_path):
	df.write \
		.partitionBy(partition_column.upper()) \
		.mode('overwrite') \
		.parquet(output_path)

def main():
	params = parse_command_line(args.params)
	spark = create_spark_session(params['name'])
	s3_files = parse_command_line(params['input_path'])
	output_path = params['output_path']

	calendar_df = create_df(spark, calendar_schema, s3_files['calendar'])
	inventory_df = create_df(spark, inventory_schema, s3_files['inventory'])
	product_df = create_df(spark, product_schema, s3_files['product'])
	sales_df = create_df(spark, sales_schema, s3_files['sales'])
	store_df = create_df(spark, store_schema, s3_files['store'])

	daily_sales_stg = create_daily_sales_stg_table(sales_df)
	daily_inventory_stg = create_daily_inventory_stg_table(inventory_df)
	sales_inv_store_dy = create_sales_inv_store_dy_table(daily_sales_stg, daily_inventory_stg)
	sales_inv_store_wk = create_sales_inv_store_wk_table(sales_inv_store_dy, calendar_df)

	save_parquet(sales_inv_store_wk,
				 params['partition_column'],
				 f'{output_path}/sales_inv_store_wk')

	save_csv(store_df, f'{output_path}/store')
	save_csv(product_df, f'{output_path}/product')
	save_csv(calendar_df, f'{output_path}/calendar')
    
if __name__ == "__main__":
	main()
else:
	print('Importing script')
