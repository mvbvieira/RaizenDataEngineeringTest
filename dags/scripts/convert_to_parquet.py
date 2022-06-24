from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, concat_ws, col, when
import os


def convert_to_parquet(**op_kwargs):
  SOURCE_XLSX_FILE = "/opt/raizen/data/vendas-combustiveis-m3.xlsx"
  CSV_FILE = "/opt/raizen/data/csv/" + op_kwargs['table_name'] + "_vendas-combustiveis-m3.csv"

  import pandas as pd

  df_excel = pd.read_excel(SOURCE_XLSX_FILE, sheet_name = op_kwargs['sheet_name'])

  df = pd.melt(
    df_excel,
    id_vars = list(df_excel.columns[:4]),
    value_vars = list(df_excel.columns[4:-1]),
    value_name = 'volume',
    var_name = 'month')

  df.to_csv(CSV_FILE, index=False)

  spark = SparkSession.builder.getOrCreate()

  sparkDF = spark.read.format("csv").option("header", True).load(CSV_FILE)

  cols=["year", "months"]

  spark_df = sparkDF \
    .withColumn('created_at', F.current_timestamp()) \
    .withColumn('unit', lit('m3')) \
    .withColumn('table_name', lit(op_kwargs['table_name'])) \
    .withColumn("volume",col("volume").cast('double')) \
    .withColumn("months", when(col("month") == "Jan", 1)
                .when(col("month") == "Fev", 2)
                .when(col("month") == "Mar", 3)
                .when(col("month") == "Abr", 4)
                .when(col("month") == "Mai", 5)
                .when(col("month") == "Jun", 6)
                .when(col("month") == "Jul", 7)
                .when(col("month") == "Ago", 8)
                .when(col("month") == "Set", 9)
                .when(col("month") == "Out", 10)
                .when(col("month") == "Nov", 11)
                .when(col("month") == "Dez", 12)
                .otherwise(col("month"))) \
    .withColumnRenamed("COMBUSTÍVEL", "product") \
    .withColumnRenamed("ESTADO", "uf") \
    .withColumnRenamed("ANO", "year") \
    .withColumnRenamed("REGIÃO", "region")

  spark_df = spark_df.withColumn("year_month", concat_ws("-", *cols).cast("date"))

  spark_df.coalesce(1) \
      .write \
      .format("parquet") \
      .partitionBy("table_name", "year", "months") \
      .mode("append") \
      .save("/opt/raizen/data/parquet/")

  os.system("rm " + CSV_FILE)