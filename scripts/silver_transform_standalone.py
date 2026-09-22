from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re
import os


def to_snake_case(col_name):
    return re.sub(r'[\s\-]+', '_', col_name.strip()).lower()


spark = SparkSession.builder.appName('silver_transform_standalone').master("local[*]").getOrCreate()

# ---------------------------------------------------------------------------
# Read + clean column names
# ---------------------------------------------------------------------------
path = os.environ.get("INPUT_PATH", r'D:\Projects\sales-etl-pipeline\data\superstore.csv')
df_superstore = spark.read.csv(
    path,
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"'
)
df_superstore = df_superstore.toDF(*[to_snake_case(c) for c in df_superstore.columns])

# Convert date columns (format confirmed as M/d/yyyy, e.g. "11/8/2016")
df_superstore = df_superstore.withColumns({
    "order_date": F.to_date("order_date", "M/d/yyyy"),
    "ship_date": F.to_date("ship_date", "M/d/yyyy"),
})

df = df_superstore

print(f"Read source CSV: {path}")
print(f"  Rows     : {df.count():,}")
print(f"  Columns  : {len(df.columns)}")

# Quick sanity check — confirm key columns exist before transforming
required_cols = [
    "order_date", "ship_date", "sales",
    "profit", "discount", "quantity"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    raise Exception(f"Required columns missing from source: {missing}")

print("  ✅ All required columns present")

# ---------------------------------------------------------------------------
# Date derivations
# ---------------------------------------------------------------------------
df = df.withColumns({
    "order_year": F.year("order_date"),
    "order_month": F.month("order_date"),
    "order_quarter": F.quarter("order_date"),
    "order_day_of_week": F.dayofweek("order_date"),  # 1=Sunday, 7=Saturday
    "order_yearmonth": F.date_format("order_date", "yyyy-MM"),
})

print("=== DATE DERIVATIONS SAMPLE ===\n")
df.select(
    "order_date", "order_year", "order_month",
    "order_quarter", "order_yearmonth"
).show(5, truncate=False)

print("Date columns derived: year, month, quarter, yearmonth")

# ---------------------------------------------------------------------------
# Shipping speed
# ---------------------------------------------------------------------------
df = df.withColumn(
    "shipping_days",
    F.datediff(F.col("ship_date"), F.col("order_date"))
)

# Same Day = 0 days, Fast = 1-2, Standard = 3-4, Slow = 5+
df = df.withColumn(
    "shipping_speed",
    F.when(F.col("shipping_days") == 0, "Same Day")
     .when(F.col("shipping_days") <= 2, "Fast")
     .when(F.col("shipping_days") <= 4, "Standard")
     .otherwise("Slow")
)

print("=== SHIPPING DAYS DISTRIBUTION ===\n")
df.groupBy("shipping_speed") \
  .count() \
  .orderBy("shipping_speed") \
  .show()

print("shipping_days and shipping_speed derived")

# ---------------------------------------------------------------------------
# Profit margin + profitability flag
# ---------------------------------------------------------------------------
df = df.withColumns({
    "profit_margin": F.when(
                          F.col("sales") != 0,
                          F.round(F.col("profit") / F.col("sales"), 4)
                      ).otherwise(0.0),

    "is_profitable": F.col("profit") > 0,
})

print("=== PROFIT MARGIN STATISTICS ===\n")
df.select(
    F.round(F.min("profit_margin"), 4).alias("min_margin"),
    F.round(F.avg("profit_margin"), 4).alias("avg_margin"),
    F.round(F.max("profit_margin"), 4).alias("max_margin"),
).show()

print("=== PROFITABLE vs LOSS-MAKING ===\n")
df.groupBy("is_profitable") \
  .count() \
  .withColumnRenamed("count", "transaction_count") \
  .show()

print("profit_margin and is_profitable derived")

# ---------------------------------------------------------------------------
# Sales band + discount band
# ---------------------------------------------------------------------------
df = df.withColumn(
    "sales_band",
    F.when(F.col("sales") < 100,   "Low")
     .when(F.col("sales") < 500,   "Medium")
     .when(F.col("sales") < 2000,  "High")
     .otherwise("Very High")
)

df = df.withColumn(
    "discount_band",
    F.when(F.col("discount") == 0,    "No Discount")
     .when(F.col("discount") <= 0.2,  "Low")
     .when(F.col("discount") <= 0.4,  "Medium")
     .otherwise("High")
)

print("=== SALES BAND DISTRIBUTION ===\n")
df.groupBy("sales_band") \
  .count() \
  .orderBy("sales_band") \
  .show()

print("=== DISCOUNT BAND DISTRIBUTION ===\n")
df.groupBy("discount_band") \
  .count() \
  .orderBy("discount_band") \
  .show()

print("sales_band and discount_band derived")

# ---------------------------------------------------------------------------
# Unit price + discount amount
# ---------------------------------------------------------------------------
df = df.withColumns({
    "unit_price": F.when(
                       (F.col("quantity") > 0) & (F.col("discount") < 1),
                       F.round(
                           F.col("sales") / (
                               F.col("quantity") * (1 - F.col("discount"))
                           ), 2
                       )
                   ).otherwise(F.col("sales")),

    "discount_amount": F.round(
                            F.col("sales") * F.col("discount"), 2
                        ),
})

print("=== REVENUE DERIVATIONS SAMPLE ===\n")
df.select(
    "sales", "quantity", "discount",
    "unit_price", "discount_amount", "profit_margin"
).show(5, truncate=False)

print("unit_price and discount_amount derived")

# ---------------------------------------------------------------------------
# Schema summary
# ---------------------------------------------------------------------------
print("=== ENRICHED SCHEMA ===\n")

source_cols = []
derived_cols = []

derived_names = [
    "order_year", "order_month", "order_quarter",
    "order_day_of_week", "order_yearmonth",
    "shipping_days", "shipping_speed",
    "profit_margin", "is_profitable",
    "sales_band", "discount_band",
    "unit_price", "discount_amount",
]

for field in df.schema.fields:
    if field.name in derived_names:
        derived_cols.append(field)
    else:
        source_cols.append(field)

print(f"  Source columns  ({len(source_cols):02d}) :", [f.name for f in source_cols])
print(f"  Derived columns ({len(derived_cols):02d}) :", [f.name for f in derived_cols])
print(f"\n  Total columns   : {len(df.columns)}")

# ---------------------------------------------------------------------------
# Write output locally (standalone equivalent of saveAsTable)
# ---------------------------------------------------------------------------
output_path = os.environ.get("OUTPUT_PATH", r'D:\Projects\sales-etl-pipeline\data\silver_output')

(
    df
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"Enriched Silver written locally to: {output_path}")

# ---------------------------------------------------------------------------
# Verification — read back what was written
# ---------------------------------------------------------------------------
df_verify = spark.read.parquet(output_path)

print("=== TRANSFORMATION VERIFICATION ===\n")
print(f"  Rows in enriched output : {df_verify.count():,}")
print(f"  Columns                 : {len(df_verify.columns)}")

# Spot-check derived columns are populated (not all null)
derived_check = df_verify.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in derived_names
    if c in df_verify.columns
]).collect()[0]

print("\n  Null check on derived columns:")
all_good = True
for col_name, null_count in zip(derived_names, derived_check):
    if col_name not in df_verify.columns:
        continue
    flag = "✅" if null_count == 0 else f"⚠️  {null_count} nulls"
    print(f"    {col_name:<30} : {flag}")
    if null_count > 0:
        all_good = False

if all_good:
    print("\n  ✅ All derived columns populated correctly")

print("Transformation verification complete")

print("=" * 55)
print("  SILVER TRANSFORMATION (STANDALONE) — COMPLETE")
print("=" * 55)
print(f"  Output   : {output_path}")
print(f"  Rows     : {df_verify.count():,}")
print(f"  Derived  : {len(derived_names)} new analytical columns")
print("=" * 55)
