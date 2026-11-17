# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "78d6335e-ef6b-43d4-9b61-7a7480ea7596",
# META       "default_lakehouse_name": "SalesRecordsLakehouse",
# META       "default_lakehouse_workspace_id": "4e609ca2-4ac4-4f95-865c-809888b75e38",
# META       "known_lakehouses": [
# META         {
# META           "id": "78d6335e-ef6b-43d4-9b61-7a7480ea7596"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

TABLE_SCHEMAS = {
    "sales": {
        "sales_key": "long",
        "order_id": "long",
        "order_date_key": "int",
        "ship_date_key": "int",
        "product_key": "int",
        "geography_key": "int",
        "sales_channel_key": "int",
        "order_priority_key": "int",
        "units_sold": "int",
        "unit_price": "decimal(18,2)",
        "unit_cost": "decimal(18,2)",
        "total_revenue": "decimal(18,2)",
        "total_cost": "decimal(18,2)",
        "total_profit": "decimal(18,2)",
        "delivery_days": "int",
        "unit_margin": "decimal(18,2)",
        "profit_margin_pct": "decimal(18,4)",
    },
    "budget": {
        "budget_key": "long",
        "budget_month_key": "int",
        "product_key": "int",
        "budget_units": "long",
        "budget_revenue": "decimal(18,2)",
        "budget_cost": "decimal(18,2)",
        "budget_profit": "decimal(18,2)",
        "budget_profit_margin_pct": "decimal(18,4)",
        "budget_version": "string",
        "units_growth_factor": "decimal(6,4)",
        "revenue_growth_factor": "decimal(6,4)",
        "cost_growth_factor": "decimal(6,4)",
        "budget_method": "string",
    },
    "date": {
        "date_key": "int",
        "full_date": "date",
        "year": "int",
        "quarter_number": "int",
        "quarter_name": "string",
        "month_number": "int",
        "month_name": "string",
        "month_short_name": "string",
        "year_month": "string",
        "week_number": "int",
        "day_of_month": "int",
        "day_of_week_number": "int",
        "day_of_week_name": "string",
        "is_weekend": "boolean",
        "month_start_date": "date",
        "month_end_date": "date",
    },
    "product": {
        "product_key": "int",
        "item_type": "string",
        "product_name": "string",
        "product_sort_order": "int",
    },
    "geography": {
        "geography_key": "int",
        "country": "string",
        "region": "string",
        "geography_name": "string",
    },
    "saleschannel": {
        "sales_channel_key": "int",
        "sales_channel": "string",
        "sales_channel_name": "string",
        "channel_sort_order": "int",
    },
    "orderpriority": {
        "order_priority_key": "int",
        "order_priority_code": "string",
        "order_priority_name": "string",
        "priority_sort_order": "int",
    },
}

EXPECTED_ROWS = {
    "sales": 2_097_152,
    "budget": 1_404,
    "date": 3_956,
    "product": 12,
    "geography": 185,
    "saleschannel": 2,
    "orderpriority": 4,
}

for table_name, column_types in TABLE_SCHEMAS.items():
    source_table = f"dbo.{table_name}"
    temporary_table = f"dbo.{table_name}_typed_tmp"

    source_df = spark.table(source_table)

    typed_df = source_df.select(
        *[
            F.col(column_name).cast(data_type).alias(column_name)
            for column_name, data_type in column_types.items()
        ]
    )

    typed_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(temporary_table)

    spark.table(temporary_table).write.format("delta").mode(
        "overwrite"
    ).option(
        "overwriteSchema", "true"
    ).saveAsTable(source_table)

    spark.sql(f"DROP TABLE {temporary_table}")

print("DATA TYPE VALIDATION")
print("=" * 80)

all_tests_passed = True

for table_name, expected_rows in EXPECTED_ROWS.items():
    dataframe = spark.table(f"dbo.{table_name}")
    actual_rows = dataframe.count()
    status = "PASS" if actual_rows == expected_rows else "FAIL"

    all_tests_passed = all_tests_passed and status == "PASS"

    print(
        f"{status}: dbo.{table_name} | "
        f"expected={expected_rows:,} | actual={actual_rows:,}"
    )

print("\nSALES SCHEMA")
spark.table("dbo.sales").printSchema()

print("\nBUDGET SCHEMA")
spark.table("dbo.budget").printSchema()

print("\nFINAL STATUS")
print(
    "VALIDATION STATUS: "
    f"{'PASS' if all_tests_passed else 'FAIL'}"
)

if not all_tests_passed:
    raise ValueError("Data type validation failed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
