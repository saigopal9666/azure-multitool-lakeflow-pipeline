import dlt
from pyspark.sql.functions import *

# ===================================================================
# 🧱 1. BRONZE LAYER: ADLS Gen2 నుండి రా-డేటాని ఆటో-లోడర్ ద్వారా లాగడం
# ===================================================================
@dlt.table(
    name="bronz_ecommerce_sales_v4",
    comment="Raw e-commerce sales data ingested from ADLS Gen2."
)
def bronz_ecommerce_sales_v4():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("fs.azure.account.key.fabricmayurrslake.dfs.core.windows.net", dbutils.secrets.get(scope="azure-secrets", key="storage-key")) \
        .load("abfss://bronze1@fabricmayurrslake.dfs.core.windows.net/e-commerce-sales-forecast/")

# ===================================================================
# 🛡️ 2. SILVER LAYER: క్వాలిటీ రూల్స్ (Expectations) చెక్ చేసి బ్యాడ్ డేటాని ఫిల్టర్ చేయడం
# ===================================================================
@dlt.table(
    name="silver_ecommerce_sales_v4",
    comment="Cleaned e-commerce data with dropped bad records."
)
@dlt.expect_or_drop("valid_date", "date IS NOT NULL")
@dlt.expect_or_drop("positive_sales", "sales >= 0")
def silver_ecommerce_sales_v4():
    return dlt.read_stream("bronz_ecommerce_sales_v4") \
        .withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("sales_date", to_date(col("date")))

# ===================================================================
# 🏛️ 3. GOLD LAYER: పవర్ బీఐ కోసం బిజినెస్ లెవెల్ అగ్రిగేషన్స్ చేయడం
# ===================================================================
@dlt.table(
    name="gold_ecommerce_bi_metrics_v4",
    comment="Final business metrics table for Power BI reporting."
)
def gold_ecommerce_bi_metrics_v4():
    return dlt.read("silver_ecommerce_sales_v4") \
        .groupBy("sales_date") \
        .agg(
            sum("sales").alias("total_daily_sales"),
            count("sales").alias("total_daily_orders")
        )
