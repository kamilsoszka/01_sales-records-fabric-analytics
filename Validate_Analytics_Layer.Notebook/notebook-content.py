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

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

spark = SparkSession.builder.getOrCreate()

EXPECTED_TABLES = {
    "date": 16,
    "geography": 4,
    "orderpriority": 4,
    "product": 4,
    "saleschannel": 4,
    "budget": 13,
    "sales": 17
}

VALIDATION_TABLE = "analytics_layer_validation"
validation_run_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

results = []

for table_name, expected_column_count in EXPECTED_TABLES.items():
    full_table_name = f"dbo.{table_name}"

    try:
        dataframe = spark.table(full_table_name)

        actual_row_count = dataframe.count()
        actual_column_count = len(dataframe.columns)

        schema_status = (
            "PASS"
            if actual_column_count == expected_column_count
            else "FAIL"
        )

        overall_status = (
            "PASS"
            if actual_row_count > 0 and schema_status == "PASS"
            else "FAIL"
        )

        results.append(
            (
                validation_run_timestamp,
                full_table_name,
                expected_column_count,
                actual_column_count,
                actual_row_count,
                schema_status,
                overall_status,
                None
            )
        )

    except Exception as exception:
        results.append(
            (
                validation_run_timestamp,
                full_table_name,
                expected_column_count,
                0,
                0,
                "FAIL",
                "FAIL",
                str(exception)[:4000]
            )
        )

validation_schema = StructType(
    [
        StructField("validation_timestamp_utc", TimestampType(), False),
        StructField("source_table", StringType(), False),
        StructField("expected_column_count", LongType(), False),
        StructField("actual_column_count", LongType(), False),
        StructField("actual_row_count", LongType(), False),
        StructField("schema_status", StringType(), False),
        StructField("validation_status", StringType(), False),
        StructField("error_message", StringType(), True)
    ]
)

validation_dataframe = spark.createDataFrame(
    results,
    schema=validation_schema
)

validation_dataframe.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(VALIDATION_TABLE)

failed_checks = validation_dataframe.filter(
    "validation_status <> 'PASS'"
).count()

validation_dataframe \
    .orderBy("source_table") \
    .show(truncate=False)

if failed_checks > 0:
    raise RuntimeError(
        f"Analytics layer validation failed. Failed checks: {failed_checks}"
    )

print(
    f"VALIDATION PASSED: {len(EXPECTED_TABLES)} of "
    f"{len(EXPECTED_TABLES)} source tables passed."
)
print(f"Evidence table created: {VALIDATION_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
