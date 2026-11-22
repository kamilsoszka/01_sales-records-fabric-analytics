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

# Microsoft Fabric Sales Records Lakehouse audit
# This notebook validates tables, schemas, primary keys, foreign keys,
# duplicate rows, null values, numeric values, and overall data quality.

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import NumericType


# ============================================================
# Configuration
# ============================================================

EXPECTED_TABLES = [
    "sales",
    "budget",
    "date",
    "geography",
    "orderpriority",
    "product",
    "saleschannel"
]

PRIMARY_KEYS = {
    "date": "date_key",
    "geography": "geography_key",
    "orderpriority": "order_priority_key",
    "product": "product_key",
    "saleschannel": "sales_channel_key"
}

FOREIGN_KEYS = [
    ("sales", "order_date_key", "date", "date_key"),
    ("sales", "ship_date_key", "date", "date_key"),
    ("sales", "geography_key", "geography", "geography_key"),
    ("sales", "order_priority_key", "orderpriority", "order_priority_key"),
    ("sales", "product_key", "product", "product_key"),
    ("sales", "sales_channel_key", "saleschannel", "sales_channel_key"),
    ("budget", "budget_month_key", "date", "date_key"),
    ("budget", "product_key", "product", "product_key")
]

AUDIT_TIMESTAMP = datetime.utcnow()
results = []


# ============================================================
# Helper functions
# ============================================================

def add_result(
    category,
    object_name,
    check_name,
    status,
    observed_value,
    expected_value,
    details
):
    results.append({
        "audit_timestamp_utc": AUDIT_TIMESTAMP,
        "category": str(category),
        "object_name": str(object_name),
        "check_name": str(check_name),
        "status": str(status),
        "observed_value": str(observed_value),
        "expected_value": str(expected_value),
        "details": str(details)
    })


def get_available_tables():
    tables = {}

    for table in spark.catalog.listTables():
        if not table.isTemporary:
            tables[table.name.lower()] = table.name

    return tables


AVAILABLE_TABLES = get_available_tables()


def resolve_table_name(table_name):
    return AVAILABLE_TABLES.get(table_name.lower())


def table_exists(table_name):
    return resolve_table_name(table_name) is not None


def get_table(table_name):
    resolved_name = resolve_table_name(table_name)

    if resolved_name is None:
        raise ValueError(f"Table not found: {table_name}")

    return spark.table(resolved_name)


def resolve_column_name(table_name, column_name):
    if not table_exists(table_name):
        return None

    dataframe = get_table(table_name)

    column_map = {
        existing_column.lower(): existing_column
        for existing_column in dataframe.columns
    }

    return column_map.get(column_name.lower())


def column_exists(table_name, column_name):
    return resolve_column_name(table_name, column_name) is not None


# ============================================================
# 1. Runtime and Lakehouse validation
# ============================================================

add_result(
    category="Runtime",
    object_name="Spark session",
    check_name="Spark session available",
    status="PASS",
    observed_value=spark.version,
    expected_value="Active Spark session",
    details="The Microsoft Fabric Spark session is active."
)

add_result(
    category="Lakehouse",
    object_name="Attached Lakehouse",
    check_name="Tables discovered",
    status="PASS" if len(AVAILABLE_TABLES) > 0 else "FAIL",
    observed_value=len(AVAILABLE_TABLES),
    expected_value="Greater than 0",
    details=(
        "Non-temporary tables discovered through the Spark catalog: "
        + ", ".join(sorted(AVAILABLE_TABLES.values()))
        if AVAILABLE_TABLES
        else "No non-temporary Lakehouse tables were discovered."
    )
)


# ============================================================
# 2. Expected table validation
# ============================================================

for expected_table in EXPECTED_TABLES:
    exists = table_exists(expected_table)
    resolved_name = resolve_table_name(expected_table)

    add_result(
        category="Table",
        object_name=expected_table,
        check_name="Expected table exists",
        status="PASS" if exists else "FAIL",
        observed_value=resolved_name if exists else False,
        expected_value=True,
        details=(
            "Expected Lakehouse table is available."
            if exists
            else "Expected Lakehouse table is missing."
        )
    )


# ============================================================
# 3. Table-level profiling
# ============================================================

for table_name in EXPECTED_TABLES:
    if not table_exists(table_name):
        continue

    dataframe = get_table(table_name)
    row_count = dataframe.count()
    column_count = len(dataframe.columns)

    add_result(
        category="Table",
        object_name=table_name,
        check_name="Row count",
        status="PASS" if row_count > 0 else "FAIL",
        observed_value=row_count,
        expected_value="Greater than 0",
        details="Physical row count in the Lakehouse table."
    )

    add_result(
        category="Table",
        object_name=table_name,
        check_name="Column count",
        status="PASS" if column_count > 0 else "FAIL",
        observed_value=column_count,
        expected_value="Greater than 0",
        details="Number of columns in the Lakehouse table."
    )

    schema_description = ", ".join(
        f"{field.name}: {field.dataType.simpleString()}"
        for field in dataframe.schema.fields
    )

    add_result(
        category="Schema",
        object_name=table_name,
        check_name="Table schema",
        status="PASS",
        observed_value=schema_description,
        expected_value="Readable schema",
        details="Column names and Spark data types."
    )

    if row_count > 0 and column_count > 0:
        duplicate_row = (
            dataframe
            .groupBy(*dataframe.columns)
            .count()
            .filter(F.col("count") > 1)
            .agg(
                F.sum(F.col("count") - 1).alias("duplicate_count")
            )
            .first()
        )

        duplicate_row_count = (
            duplicate_row["duplicate_count"]
            if duplicate_row is not None
            and duplicate_row["duplicate_count"] is not None
            else 0
        )
    else:
        duplicate_row_count = 0

    add_result(
        category="Data quality",
        object_name=table_name,
        check_name="Fully duplicated rows",
        status="PASS" if duplicate_row_count == 0 else "WARN",
        observed_value=duplicate_row_count,
        expected_value=0,
        details="Number of rows duplicated across every column."
    )

    if column_count > 0:
        null_expressions = [
            F.sum(
                F.when(F.col(f"`{column_name}`").isNull(), 1).otherwise(0)
            ).alias(column_name)
            for column_name in dataframe.columns
        ]

        null_counts = dataframe.agg(*null_expressions).first().asDict()

        for column_name, null_count in null_counts.items():
            null_count = null_count or 0

            null_percentage = (
                round((null_count / row_count) * 100, 4)
                if row_count > 0
                else 0.0
            )

            add_result(
                category="Null profile",
                object_name=f"{table_name}.{column_name}",
                check_name="Null values",
                status="PASS" if null_count == 0 else "WARN",
                observed_value=f"{null_count} ({null_percentage}%)",
                expected_value=0,
                details="Null count and percentage for the column."
            )


# ============================================================
# 4. Primary key validation
# ============================================================

for table_name, configured_primary_key in PRIMARY_KEYS.items():
    object_name = f"{table_name}.{configured_primary_key}"

    if not table_exists(table_name):
        add_result(
            category="Primary key",
            object_name=object_name,
            check_name="Primary key validation",
            status="FAIL",
            observed_value="Table missing",
            expected_value="Existing table and primary key",
            details="Primary key validation cannot run because the table is missing."
        )
        continue

    primary_key = resolve_column_name(
        table_name,
        configured_primary_key
    )

    if primary_key is None:
        add_result(
            category="Primary key",
            object_name=object_name,
            check_name="Primary key column exists",
            status="FAIL",
            observed_value=False,
            expected_value=True,
            details="Configured primary key column is missing."
        )
        continue

    dataframe = get_table(table_name)
    row_count = dataframe.count()

    profile = (
        dataframe
        .agg(
            F.sum(
                F.when(
                    F.col(f"`{primary_key}`").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_count"),
            F.countDistinct(
                F.col(f"`{primary_key}`")
            ).alias("distinct_count")
        )
        .first()
    )

    null_count = profile["null_count"] or 0
    distinct_count = profile["distinct_count"] or 0
    duplicate_key_count = max(
        row_count - null_count - distinct_count,
        0
    )

    add_result(
        category="Primary key",
        object_name=f"{table_name}.{primary_key}",
        check_name="Primary key nulls",
        status="PASS" if null_count == 0 else "FAIL",
        observed_value=null_count,
        expected_value=0,
        details="Primary key columns must not contain null values."
    )

    add_result(
        category="Primary key",
        object_name=f"{table_name}.{primary_key}",
        check_name="Primary key uniqueness",
        status="PASS" if duplicate_key_count == 0 else "FAIL",
        observed_value=duplicate_key_count,
        expected_value=0,
        details="Number of duplicate non-null primary key values."
    )


# ============================================================
# 5. Foreign key integrity validation
# ============================================================

for (
    fact_table,
    configured_foreign_key,
    dimension_table,
    configured_dimension_key
) in FOREIGN_KEYS:

    relationship_name = (
        f"{fact_table}.{configured_foreign_key} -> "
        f"{dimension_table}.{configured_dimension_key}"
    )

    if not table_exists(fact_table) or not table_exists(dimension_table):
        add_result(
            category="Relationship",
            object_name=relationship_name,
            check_name="Relationship tables exist",
            status="FAIL",
            observed_value=False,
            expected_value=True,
            details="One or both relationship tables are missing."
        )
        continue

    foreign_key = resolve_column_name(
        fact_table,
        configured_foreign_key
    )

    dimension_key = resolve_column_name(
        dimension_table,
        configured_dimension_key
    )

    if foreign_key is None or dimension_key is None:
        add_result(
            category="Relationship",
            object_name=relationship_name,
            check_name="Relationship columns exist",
            status="FAIL",
            observed_value=False,
            expected_value=True,
            details="One or both relationship columns are missing."
        )
        continue

    fact_keys = (
        get_table(fact_table)
        .select(
            F.col(f"`{foreign_key}`").alias("foreign_key")
        )
        .filter(F.col("foreign_key").isNotNull())
    )

    dimension_keys = (
        get_table(dimension_table)
        .select(
            F.col(f"`{dimension_key}`").alias("dimension_key")
        )
        .filter(F.col("dimension_key").isNotNull())
        .distinct()
    )

    total_non_null_keys = fact_keys.count()

    orphan_count = (
        fact_keys
        .join(
            dimension_keys,
            fact_keys["foreign_key"] == dimension_keys["dimension_key"],
            "left_anti"
        )
        .count()
    )

    orphan_percentage = (
        round((orphan_count / total_non_null_keys) * 100, 4)
        if total_non_null_keys > 0
        else 0.0
    )

    add_result(
        category="Relationship",
        object_name=(
            f"{fact_table}.{foreign_key} -> "
            f"{dimension_table}.{dimension_key}"
        ),
        check_name="Foreign key integrity",
        status="PASS" if orphan_count == 0 else "FAIL",
        observed_value=f"{orphan_count} ({orphan_percentage}%)",
        expected_value=0,
        details="Rows without a matching dimension key."
    )


# ============================================================
# 6. Numeric column validation
# ============================================================

for table_name in ["sales", "budget"]:
    if not table_exists(table_name):
        continue

    dataframe = get_table(table_name)

    numeric_columns = [
        field.name
        for field in dataframe.schema.fields
        if isinstance(field.dataType, NumericType)
        and not field.name.lower().endswith("_key")
    ]

    if not numeric_columns:
        add_result(
            category="Numeric profile",
            object_name=table_name,
            check_name="Numeric business columns discovered",
            status="WARN",
            observed_value=0,
            expected_value="At least one",
            details=(
                "No numeric non-key columns were discovered. "
                "Review source data types."
            )
        )

    for column_name in numeric_columns:
        negative_count = (
            dataframe
            .filter(F.col(f"`{column_name}`") < 0)
            .count()
        )

        zero_count = (
            dataframe
            .filter(F.col(f"`{column_name}`") == 0)
            .count()
        )

        add_result(
            category="Numeric profile",
            object_name=f"{table_name}.{column_name}",
            check_name="Negative values",
            status="PASS" if negative_count == 0 else "WARN",
            observed_value=negative_count,
            expected_value=0,
            details=(
                "Negative values may represent returns, adjustments, "
                "or invalid records and require business validation."
            )
        )

        add_result(
            category="Numeric profile",
            object_name=f"{table_name}.{column_name}",
            check_name="Zero values",
            status="PASS",
            observed_value=zero_count,
            expected_value="Business dependent",
            details="Zero values are reported for business validation."
        )


# ============================================================
# 7. Date range validation
# ============================================================

if table_exists("date"):
    date_dataframe = get_table("date")

    possible_date_columns = [
        field.name
        for field in date_dataframe.schema.fields
        if "date" in field.name.lower()
        and not field.name.lower().endswith("_key")
    ]

    if possible_date_columns:
        for date_column in possible_date_columns:
            date_range = (
                date_dataframe
                .agg(
                    F.min(
                        F.col(f"`{date_column}`")
                    ).alias("minimum_value"),
                    F.max(
                        F.col(f"`{date_column}`")
                    ).alias("maximum_value")
                )
                .first()
            )

            add_result(
                category="Date profile",
                object_name=f"date.{date_column}",
                check_name="Date range",
                status="PASS",
                observed_value=(
                    f"{date_range['minimum_value']} to "
                    f"{date_range['maximum_value']}"
                ),
                expected_value="Valid business date range",
                details="Minimum and maximum values found in the date column."
            )
    else:
        add_result(
            category="Date profile",
            object_name="date",
            check_name="Business date column discovered",
            status="WARN",
            observed_value=False,
            expected_value=True,
            details=(
                "No non-key column containing 'date' was discovered "
                "in the date table."
            )
        )


# ============================================================
# 8. Create final audit report
# ============================================================

if not results:
    raise RuntimeError(
        "The audit produced no results. Verify the attached Lakehouse."
    )

audit_dataframe = spark.createDataFrame(results)

audit_dataframe = (
    audit_dataframe
    .withColumn(
        "status_order",
        F.when(F.col("status") == "FAIL", F.lit(1))
        .when(F.col("status") == "WARN", F.lit(2))
        .otherwise(F.lit(3))
    )
    .orderBy(
        F.col("status_order"),
        F.col("category"),
        F.col("object_name"),
        F.col("check_name")
    )
)

summary_dataframe = (
    audit_dataframe
    .groupBy("status", "status_order")
    .count()
    .orderBy(F.col("status_order"))
    .drop("status_order")
)

failed_checks = (
    audit_dataframe
    .filter(F.col("status") == "FAIL")
    .count()
)

warning_checks = (
    audit_dataframe
    .filter(F.col("status") == "WARN")
    .count()
)

passed_checks = (
    audit_dataframe
    .filter(F.col("status") == "PASS")
    .count()
)

overall_status = (
    "FAIL"
    if failed_checks > 0
    else "WARN"
    if warning_checks > 0
    else "PASS"
)


# ============================================================
# 9. Display results
# ============================================================

print("=" * 80)
print("MICROSOFT FABRIC SALES RECORDS LAKEHOUSE AUDIT")
print("=" * 80)
print(f"AUDIT TIMESTAMP UTC: {AUDIT_TIMESTAMP}")
print(f"SPARK VERSION: {spark.version}")
print(f"DISCOVERED TABLES: {len(AVAILABLE_TABLES)}")
print(f"OVERALL AUDIT STATUS: {overall_status}")
print(f"PASSED CHECKS: {passed_checks}")
print(f"WARNING CHECKS: {warning_checks}")
print(f"FAILED CHECKS: {failed_checks}")
print("=" * 80)

print("AUDIT SUMMARY")
display(summary_dataframe)

print("FAILED CHECKS")
display(
    audit_dataframe
    .filter(F.col("status") == "FAIL")
    .drop("status_order")
)

print("WARNING CHECKS")
display(
    audit_dataframe
    .filter(F.col("status") == "WARN")
    .drop("status_order")
)

print("DETAILED AUDIT RESULTS")
display(
    audit_dataframe.drop("status_order")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric unified Lakehouse and workspace audit
# This notebook performs:
# 1. Runtime and notebook context audit
# 2. Current workspace metadata audit
# 3. Workspace item inventory
# 4. Workspace role assignment audit
# 5. Git connection audit
# 6. Lakehouse table and schema audit
# 7. Primary key and foreign key integrity audit
# 8. Null, duplicate, numeric, and date profiling
#
# The notebook is read-only. It does not modify Fabric workspace resources.

from datetime import datetime, timezone
import json
import time
from urllib.parse import urljoin

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

import notebookutils


# ============================================================
# Configuration
# ============================================================

FABRIC_API_BASE_URL = "https://api.fabric.microsoft.com/v1/"
REQUEST_TIMEOUT_SECONDS = 60
MAX_RETRY_ATTEMPTS = 3

EXPECTED_TABLES = [
    "sales",
    "budget",
    "date",
    "geography",
    "orderpriority",
    "product",
    "saleschannel"
]

PRIMARY_KEYS = {
    "date": "date_key",
    "geography": "geography_key",
    "orderpriority": "order_priority_key",
    "product": "product_key",
    "saleschannel": "sales_channel_key"
}

FOREIGN_KEYS = [
    ("sales", "order_date_key", "date", "date_key"),
    ("sales", "ship_date_key", "date", "date_key"),
    ("sales", "geography_key", "geography", "geography_key"),
    ("sales", "order_priority_key", "orderpriority", "order_priority_key"),
    ("sales", "product_key", "product", "product_key"),
    ("sales", "sales_channel_key", "saleschannel", "sales_channel_key"),
    ("budget", "budget_month_key", "date", "date_key"),
    ("budget", "product_key", "product", "product_key")
]

AUDIT_TIMESTAMP = datetime.now(timezone.utc)
results = []
workspace_items = []
workspace_roles = []
workspace_metadata = []
workspace_git = []


# ============================================================
# Generic helper functions
# ============================================================

def safe_string(value):
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return str(value)


def add_result(
    category,
    object_name,
    check_name,
    status,
    observed_value,
    expected_value,
    details
):
    results.append({
        "audit_timestamp_utc": AUDIT_TIMESTAMP,
        "category": safe_string(category),
        "object_name": safe_string(object_name),
        "check_name": safe_string(check_name),
        "status": safe_string(status),
        "observed_value": safe_string(observed_value),
        "expected_value": safe_string(expected_value),
        "details": safe_string(details)
    })


def get_context_value(context, property_name, default_value=None):
    try:
        value = getattr(context, property_name)
        return default_value if value is None else value
    except Exception:
        pass

    try:
        if isinstance(context, dict):
            return context.get(property_name, default_value)
    except Exception:
        pass

    return default_value


def flatten_dictionary(source, parent_key="", separator="."):
    flattened = {}

    if not isinstance(source, dict):
        return flattened

    for key, value in source.items():
        new_key = (
            f"{parent_key}{separator}{key}"
            if parent_key
            else str(key)
        )

        if isinstance(value, dict):
            flattened.update(
                flatten_dictionary(
                    value,
                    parent_key=new_key,
                    separator=separator
                )
            )
        else:
            flattened[new_key] = value

    return flattened


# ============================================================
# Runtime context
# ============================================================

runtime_context = notebookutils.runtime.context

CURRENT_NOTEBOOK_NAME = get_context_value(
    runtime_context,
    "currentNotebookName",
    ""
)

CURRENT_NOTEBOOK_ID = get_context_value(
    runtime_context,
    "currentNotebookId",
    ""
)

CURRENT_WORKSPACE_NAME = get_context_value(
    runtime_context,
    "currentWorkspaceName",
    ""
)

CURRENT_WORKSPACE_ID = get_context_value(
    runtime_context,
    "currentWorkspaceId",
    ""
)

DEFAULT_LAKEHOUSE_NAME = get_context_value(
    runtime_context,
    "defaultLakehouseName",
    ""
)

DEFAULT_LAKEHOUSE_ID = get_context_value(
    runtime_context,
    "defaultLakehouseId",
    ""
)

DEFAULT_LAKEHOUSE_WORKSPACE_NAME = get_context_value(
    runtime_context,
    "defaultLakehouseWorkspaceName",
    ""
)

DEFAULT_LAKEHOUSE_WORKSPACE_ID = get_context_value(
    runtime_context,
    "defaultLakehouseWorkspaceId",
    ""
)

CURRENT_RUN_ID = get_context_value(
    runtime_context,
    "currentRunId",
    ""
)

PARENT_RUN_ID = get_context_value(
    runtime_context,
    "parentRunId",
    ""
)

ROOT_RUN_ID = get_context_value(
    runtime_context,
    "rootRunId",
    ""
)

IS_FOR_PIPELINE = get_context_value(
    runtime_context,
    "isForPipeline",
    False
)

IS_FOR_INTERACTIVE = get_context_value(
    runtime_context,
    "isForInteractive",
    False
)

IS_REFERENCE_RUN = get_context_value(
    runtime_context,
    "isReferenceRun",
    False
)


CONTEXT_PROPERTIES = {
    "currentNotebookName": CURRENT_NOTEBOOK_NAME,
    "currentNotebookId": CURRENT_NOTEBOOK_ID,
    "currentWorkspaceName": CURRENT_WORKSPACE_NAME,
    "currentWorkspaceId": CURRENT_WORKSPACE_ID,
    "defaultLakehouseName": DEFAULT_LAKEHOUSE_NAME,
    "defaultLakehouseId": DEFAULT_LAKEHOUSE_ID,
    "defaultLakehouseWorkspaceName": DEFAULT_LAKEHOUSE_WORKSPACE_NAME,
    "defaultLakehouseWorkspaceId": DEFAULT_LAKEHOUSE_WORKSPACE_ID,
    "currentRunId": CURRENT_RUN_ID,
    "parentRunId": PARENT_RUN_ID,
    "rootRunId": ROOT_RUN_ID,
    "isForPipeline": IS_FOR_PIPELINE,
    "isForInteractive": IS_FOR_INTERACTIVE,
    "isReferenceRun": IS_REFERENCE_RUN
}

for property_name, property_value in CONTEXT_PROPERTIES.items():
    workspace_metadata.append({
        "section": "Runtime context",
        "property_name": property_name,
        "property_value": safe_string(property_value)
    })

add_result(
    category="Runtime",
    object_name="Spark session",
    check_name="Spark session available",
    status="PASS",
    observed_value=spark.version,
    expected_value="Active Spark session",
    details="The Microsoft Fabric Spark session is active."
)

add_result(
    category="Runtime",
    object_name="Notebook context",
    check_name="Current notebook identified",
    status="PASS" if CURRENT_NOTEBOOK_ID else "WARN",
    observed_value=(
        f"{CURRENT_NOTEBOOK_NAME} ({CURRENT_NOTEBOOK_ID})"
        if CURRENT_NOTEBOOK_ID
        else CURRENT_NOTEBOOK_NAME
    ),
    expected_value="Notebook name and ID",
    details="Notebook metadata obtained from the Fabric runtime context."
)

add_result(
    category="Workspace",
    object_name="Current workspace",
    check_name="Workspace context identified",
    status="PASS" if CURRENT_WORKSPACE_ID else "FAIL",
    observed_value=(
        f"{CURRENT_WORKSPACE_NAME} ({CURRENT_WORKSPACE_ID})"
        if CURRENT_WORKSPACE_ID
        else CURRENT_WORKSPACE_NAME
    ),
    expected_value="Workspace name and ID",
    details="Workspace metadata obtained from the Fabric runtime context."
)

add_result(
    category="Lakehouse",
    object_name="Default Lakehouse",
    check_name="Default Lakehouse identified",
    status="PASS" if DEFAULT_LAKEHOUSE_ID else "WARN",
    observed_value=(
        f"{DEFAULT_LAKEHOUSE_NAME} ({DEFAULT_LAKEHOUSE_ID})"
        if DEFAULT_LAKEHOUSE_ID
        else DEFAULT_LAKEHOUSE_NAME
    ),
    expected_value="Attached default Lakehouse",
    details="Default Lakehouse metadata obtained from runtime context."
)


# ============================================================
# Fabric REST API helpers
# ============================================================

FABRIC_ACCESS_TOKEN = None
FABRIC_HEADERS = None


def obtain_fabric_token():
    token_errors = []

    for audience in ["pbi", "https://api.fabric.microsoft.com"]:
        try:
            token = notebookutils.credentials.getToken(audience)

            if token:
                return token, audience
        except Exception as error:
            token_errors.append(
                f"{audience}: {type(error).__name__}: {error}"
            )

    raise RuntimeError(
        "Fabric token acquisition failed. "
        + " | ".join(token_errors)
    )


def fabric_api_get(relative_path, optional=False):
    if FABRIC_HEADERS is None:
        if optional:
            return None, {
                "status_code": None,
                "error": "Fabric authentication is unavailable."
            }

        raise RuntimeError("Fabric authentication is unavailable.")

    url = urljoin(FABRIC_API_BASE_URL, relative_path.lstrip("/"))
    last_error = None

    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            response = requests.get(
                url,
                headers=FABRIC_HEADERS,
                timeout=REQUEST_TIMEOUT_SECONDS
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "5"))
                time.sleep(min(retry_after, 30))
                continue

            if 200 <= response.status_code < 300:
                if not response.content:
                    return {}, None

                return response.json(), None

            response_text = response.text[:2000]

            error_details = {
                "status_code": response.status_code,
                "error": response_text,
                "url": url
            }

            if optional:
                return None, error_details

            raise RuntimeError(
                f"Fabric API request failed: "
                f"{response.status_code} {response_text}"
            )

        except requests.RequestException as error:
            last_error = error

            if attempt < MAX_RETRY_ATTEMPTS:
                time.sleep(attempt * 2)
                continue

    error_details = {
        "status_code": None,
        "error": safe_string(last_error),
        "url": url
    }

    if optional:
        return None, error_details

    raise RuntimeError(
        f"Fabric API request failed after retries: {last_error}"
    )


def fabric_api_get_paginated(relative_path):
    all_values = []
    current_path = relative_path
    page_number = 0

    while current_path:
        page_number += 1

        if page_number > 100:
            raise RuntimeError(
                "Fabric API pagination exceeded 100 pages."
            )

        payload, error = fabric_api_get(
            current_path,
            optional=False
        )

        values = payload.get("value", [])

        if isinstance(values, list):
            all_values.extend(values)

        continuation_uri = payload.get("continuationUri")
        continuation_token = payload.get("continuationToken")

        if continuation_uri:
            current_path = continuation_uri
        elif continuation_token:
            separator = "&" if "?" in relative_path else "?"
            current_path = (
                f"{relative_path}{separator}"
                f"continuationToken={continuation_token}"
            )
        else:
            current_path = None

    return all_values


# ============================================================
# Fabric REST API authentication
# ============================================================

try:
    FABRIC_ACCESS_TOKEN, TOKEN_AUDIENCE = obtain_fabric_token()

    FABRIC_HEADERS = {
        "Authorization": f"Bearer {FABRIC_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    add_result(
        category="Security",
        object_name="Fabric REST API",
        check_name="User token acquired",
        status="PASS",
        observed_value=f"Audience: {TOKEN_AUDIENCE}",
        expected_value="Valid delegated Fabric token",
        details=(
            "A delegated token was acquired through NotebookUtils. "
            "The token is not displayed or persisted."
        )
    )

except Exception as error:
    add_result(
        category="Security",
        object_name="Fabric REST API",
        check_name="User token acquired",
        status="FAIL",
        observed_value=f"{type(error).__name__}: {error}",
        expected_value="Valid delegated Fabric token",
        details=(
            "Workspace REST API checks cannot run without a token. "
            "Lakehouse checks will continue."
        )
    )


# ============================================================
# Current workspace metadata
# ============================================================

if FABRIC_HEADERS and CURRENT_WORKSPACE_ID:
    workspace_payload, workspace_error = fabric_api_get(
        f"workspaces/{CURRENT_WORKSPACE_ID}",
        optional=True
    )

    if workspace_payload:
        flattened_workspace = flatten_dictionary(workspace_payload)

        for property_name, property_value in flattened_workspace.items():
            workspace_metadata.append({
                "section": "Workspace REST API",
                "property_name": property_name,
                "property_value": safe_string(property_value)
            })

        workspace_display_name = workspace_payload.get(
            "displayName",
            CURRENT_WORKSPACE_NAME
        )

        workspace_capacity_id = workspace_payload.get("capacityId", "")
        workspace_domain_id = workspace_payload.get("domainId", "")
        workspace_description = workspace_payload.get("description", "")

        add_result(
            category="Workspace",
            object_name=workspace_display_name,
            check_name="Workspace REST metadata retrieved",
            status="PASS",
            observed_value=CURRENT_WORKSPACE_ID,
            expected_value="Accessible workspace",
            details="Current workspace metadata was retrieved successfully."
        )

        add_result(
            category="Workspace",
            object_name=workspace_display_name,
            check_name="Workspace capacity assignment",
            status="PASS" if workspace_capacity_id else "WARN",
            observed_value=workspace_capacity_id or "Not returned",
            expected_value="Assigned Fabric capacity",
            details="Capacity ID returned by the workspace API."
        )

        add_result(
            category="Workspace",
            object_name=workspace_display_name,
            check_name="Workspace domain assignment",
            status="PASS",
            observed_value=workspace_domain_id or "No domain returned",
            expected_value="Business dependent",
            details="Domain ID returned by the workspace API."
        )

        add_result(
            category="Workspace",
            object_name=workspace_display_name,
            check_name="Workspace description",
            status="PASS" if workspace_description else "WARN",
            observed_value=workspace_description or "Empty",
            expected_value="Documented workspace purpose",
            details="Workspace description supports governance and discovery."
        )

    else:
        add_result(
            category="Workspace",
            object_name=CURRENT_WORKSPACE_NAME,
            check_name="Workspace REST metadata retrieved",
            status="WARN",
            observed_value=safe_string(workspace_error),
            expected_value="HTTP 200",
            details=(
                "Workspace context remains available, but detailed REST "
                "metadata could not be retrieved."
            )
        )


# ============================================================
# Workspace item inventory
# ============================================================

if FABRIC_HEADERS and CURRENT_WORKSPACE_ID:
    try:
        items_payload = fabric_api_get_paginated(
            f"workspaces/{CURRENT_WORKSPACE_ID}/items"
        )

        for item in items_payload:
            workspace_items.append({
                "display_name": safe_string(item.get("displayName")),
                "item_type": safe_string(item.get("type")),
                "item_id": safe_string(item.get("id")),
                "workspace_id": safe_string(
                    item.get("workspaceId", CURRENT_WORKSPACE_ID)
                ),
                "folder_id": safe_string(item.get("folderId")),
                "description": safe_string(item.get("description"))
            })

        item_types = sorted({
            safe_string(item.get("type"))
            for item in items_payload
            if item.get("type")
        })

        add_result(
            category="Workspace inventory",
            object_name=CURRENT_WORKSPACE_NAME,
            check_name="Workspace items retrieved",
            status="PASS",
            observed_value=len(items_payload),
            expected_value="Greater than 0",
            details=(
                "Item types: "
                + ", ".join(item_types)
                if item_types
                else "No item types returned."
            )
        )

        item_type_counts = {}

        for item in items_payload:
            item_type = safe_string(item.get("type")) or "Unknown"
            item_type_counts[item_type] = (
                item_type_counts.get(item_type, 0) + 1
            )

        for item_type, item_count in sorted(item_type_counts.items()):
            add_result(
                category="Workspace inventory",
                object_name=item_type,
                check_name="Item count by type",
                status="PASS",
                observed_value=item_count,
                expected_value="Informational",
                details="Number of workspace items for this Fabric item type."
            )

    except Exception as error:
        add_result(
            category="Workspace inventory",
            object_name=CURRENT_WORKSPACE_NAME,
            check_name="Workspace items retrieved",
            status="FAIL",
            observed_value=f"{type(error).__name__}: {error}",
            expected_value="Accessible workspace items",
            details="Workspace item inventory could not be retrieved."
        )


# ============================================================
# Workspace role assignments
# ============================================================

if FABRIC_HEADERS and CURRENT_WORKSPACE_ID:
    try:
        roles_payload = fabric_api_get_paginated(
            f"workspaces/{CURRENT_WORKSPACE_ID}/roleAssignments"
        )

        for assignment in roles_payload:
            principal = assignment.get("principal") or {}
            user_details = principal.get("userDetails") or {}
            service_principal_details = (
                principal.get("servicePrincipalDetails") or {}
            )
            group_details = principal.get("groupDetails") or {}

            workspace_roles.append({
                "assignment_id": safe_string(assignment.get("id")),
                "role": safe_string(assignment.get("role")),
                "principal_display_name": safe_string(
                    principal.get("displayName")
                ),
                "principal_id": safe_string(principal.get("id")),
                "principal_type": safe_string(principal.get("type")),
                "user_principal_name": safe_string(
                    user_details.get("userPrincipalName")
                ),
                "service_principal_app_id": safe_string(
                    service_principal_details.get("aadAppId")
                ),
                "group_type": safe_string(
                    group_details.get("groupType")
                )
            })

        admins = [
            assignment
            for assignment in roles_payload
            if safe_string(assignment.get("role")).lower() == "admin"
        ]

        add_result(
            category="Workspace security",
            object_name=CURRENT_WORKSPACE_NAME,
            check_name="Role assignments retrieved",
            status="PASS",
            observed_value=len(roles_payload),
            expected_value="At least one assignment",
            details="Workspace role assignments were retrieved."
        )

        add_result(
            category="Workspace security",
            object_name=CURRENT_WORKSPACE_NAME,
            check_name="Workspace administrators",
            status="PASS" if len(admins) > 0 else "WARN",
            observed_value=len(admins),
            expected_value="At least one administrator",
            details="Number of assignments with the Admin workspace role."
        )

    except Exception as error:
        add_result(
            category="Workspace security",
            object_name=CURRENT_WORKSPACE_NAME,
            check_name="Role assignments retrieved",
            status="WARN",
            observed_value=f"{type(error).__name__}: {error}",
            expected_value="Member or Admin access",
            details=(
                "Role assignment retrieval requires sufficient workspace "
                "permissions. Other audits continue."
            )
        )


# ============================================================
# Workspace Git connection
# ============================================================

if FABRIC_HEADERS and CURRENT_WORKSPACE_ID:
    git_payload, git_error = fabric_api_get(
        f"workspaces/{CURRENT_WORKSPACE_ID}/git/connection",
        optional=True
    )

    if git_payload:
        flattened_git = flatten_dictionary(git_payload)

        for property_name, property_value in flattened_git.items():
            workspace_git.append({
                "property_name": property_name,
                "property_value": safe_string(property_value)
            })

        provider_details = git_payload.get("gitProviderDetails") or {}
        git_provider = (
            provider_details.get("gitProviderType")
            or git_payload.get("gitProviderType")
            or "Connected"
        )

        branch_name = (
            provider_details.get("branchName")
            or git_payload.get("branchName")
            or ""
        )

        add_result(
            category="DevOps",
            object_name=CURRENT_WORKSPACE_NAME,
            check_name="Git connection",
            status="PASS",
            observed_value=git_provider,
            expected_value="Connected Git repository",
            details=(
                f"Branch: {branch_name}"
                if branch_name
                else "Git connection metadata was returned."
            )
        )

    else:
        status_code = (
            git_error.get("status_code")
            if isinstance(git_error, dict)
            else None
        )

        git_status = "WARN"

        if status_code in [401, 403]:
            git_details = (
                "Git connection could not be read because the current "
                "identity lacks sufficient permission."
            )
        elif status_code in [404, 409]:
            git_details = (
                "No readable Git connection was returned for the workspace."
            )
        else:
            git_details = safe_string(git_error)

        add_result(
            category="DevOps",
            object_name=CURRENT_WORKSPACE_NAME,
            check_name="Git connection",
            status=git_status,
            observed_value=(
                f"HTTP {status_code}"
                if status_code
                else "Not available"
            ),
            expected_value="Connected Git repository",
            details=git_details
        )


# ============================================================
# Spark catalog helpers
# ============================================================

def get_available_tables():
    discovered_tables = {}

    for table in spark.catalog.listTables():
        if not table.isTemporary:
            discovered_tables[table.name.lower()] = table.name

    return discovered_tables


AVAILABLE_TABLES = get_available_tables()


def resolve_table_name(table_name):
    return AVAILABLE_TABLES.get(table_name.lower())


def table_exists(table_name):
    return resolve_table_name(table_name) is not None


def get_table(table_name):
    resolved_name = resolve_table_name(table_name)

    if resolved_name is None:
        raise ValueError(f"Table not found: {table_name}")

    return spark.table(resolved_name)


def resolve_column_name(table_name, column_name):
    if not table_exists(table_name):
        return None

    column_map = {
        existing_column.lower(): existing_column
        for existing_column in get_table(table_name).columns
    }

    return column_map.get(column_name.lower())


# ============================================================
# Lakehouse table discovery
# ============================================================

add_result(
    category="Lakehouse",
    object_name=DEFAULT_LAKEHOUSE_NAME or "Attached Lakehouse",
    check_name="Tables discovered",
    status="PASS" if AVAILABLE_TABLES else "FAIL",
    observed_value=len(AVAILABLE_TABLES),
    expected_value="Greater than 0",
    details=(
        ", ".join(sorted(AVAILABLE_TABLES.values()))
        if AVAILABLE_TABLES
        else "No non-temporary tables were discovered."
    )
)

for expected_table in EXPECTED_TABLES:
    resolved_name = resolve_table_name(expected_table)

    add_result(
        category="Table",
        object_name=expected_table,
        check_name="Expected table exists",
        status="PASS" if resolved_name else "FAIL",
        observed_value=resolved_name or False,
        expected_value=True,
        details=(
            "Expected Lakehouse table is available."
            if resolved_name
            else "Expected Lakehouse table is missing."
        )
    )


# ============================================================
# Table profiling
# ============================================================

for table_name in EXPECTED_TABLES:
    if not table_exists(table_name):
        continue

    dataframe = get_table(table_name)
    row_count = dataframe.count()
    column_count = len(dataframe.columns)

    add_result(
        category="Table",
        object_name=table_name,
        check_name="Row count",
        status="PASS" if row_count > 0 else "FAIL",
        observed_value=row_count,
        expected_value="Greater than 0",
        details="Physical row count in the Lakehouse table."
    )

    add_result(
        category="Table",
        object_name=table_name,
        check_name="Column count",
        status="PASS" if column_count > 0 else "FAIL",
        observed_value=column_count,
        expected_value="Greater than 0",
        details="Number of columns in the Lakehouse table."
    )

    schema_description = ", ".join(
        f"{field.name}: {field.dataType.simpleString()}"
        for field in dataframe.schema.fields
    )

    add_result(
        category="Schema",
        object_name=table_name,
        check_name="Table schema",
        status="PASS",
        observed_value=schema_description,
        expected_value="Readable schema",
        details="Column names and Spark data types."
    )

    if row_count > 0 and column_count > 0:
        duplicate_profile = (
            dataframe
            .groupBy(*dataframe.columns)
            .count()
            .filter(F.col("count") > 1)
            .agg(
                F.sum(F.col("count") - 1).alias("duplicate_count")
            )
            .first()
        )

        duplicate_count = (
            duplicate_profile["duplicate_count"]
            if duplicate_profile
            and duplicate_profile["duplicate_count"] is not None
            else 0
        )
    else:
        duplicate_count = 0

    add_result(
        category="Data quality",
        object_name=table_name,
        check_name="Fully duplicated rows",
        status="PASS" if duplicate_count == 0 else "WARN",
        observed_value=duplicate_count,
        expected_value=0,
        details="Rows duplicated across every column."
    )

    null_expressions = [
        F.sum(
            F.when(
                F.col(f"`{column_name}`").isNull(),
                1
            ).otherwise(0)
        ).alias(column_name)
        for column_name in dataframe.columns
    ]

    null_counts = dataframe.agg(*null_expressions).first().asDict()

    for column_name, null_count in null_counts.items():
        null_count = null_count or 0
        null_percentage = (
            round((null_count / row_count) * 100, 4)
            if row_count > 0
            else 0.0
        )

        add_result(
            category="Null profile",
            object_name=f"{table_name}.{column_name}",
            check_name="Null values",
            status="PASS" if null_count == 0 else "WARN",
            observed_value=f"{null_count} ({null_percentage}%)",
            expected_value=0,
            details="Null count and percentage for the column."
        )


# ============================================================
# Primary key validation
# ============================================================

for table_name, configured_key in PRIMARY_KEYS.items():
    resolved_key = resolve_column_name(table_name, configured_key)

    if not table_exists(table_name) or resolved_key is None:
        add_result(
            category="Primary key",
            object_name=f"{table_name}.{configured_key}",
            check_name="Primary key available",
            status="FAIL",
            observed_value=False,
            expected_value=True,
            details="Table or configured primary key column is missing."
        )
        continue

    dataframe = get_table(table_name)
    row_count = dataframe.count()

    profile = dataframe.agg(
        F.sum(
            F.when(
                F.col(f"`{resolved_key}`").isNull(),
                1
            ).otherwise(0)
        ).alias("null_count"),
        F.countDistinct(
            F.col(f"`{resolved_key}`")
        ).alias("distinct_count")
    ).first()

    null_count = profile["null_count"] or 0
    distinct_count = profile["distinct_count"] or 0
    duplicate_key_count = max(
        row_count - null_count - distinct_count,
        0
    )

    add_result(
        category="Primary key",
        object_name=f"{table_name}.{resolved_key}",
        check_name="Primary key nulls",
        status="PASS" if null_count == 0 else "FAIL",
        observed_value=null_count,
        expected_value=0,
        details="Primary keys must not contain null values."
    )

    add_result(
        category="Primary key",
        object_name=f"{table_name}.{resolved_key}",
        check_name="Primary key uniqueness",
        status="PASS" if duplicate_key_count == 0 else "FAIL",
        observed_value=duplicate_key_count,
        expected_value=0,
        details="Duplicate non-null primary key values."
    )


# ============================================================
# Foreign key validation
# ============================================================

for (
    fact_table,
    configured_foreign_key,
    dimension_table,
    configured_dimension_key
) in FOREIGN_KEYS:

    foreign_key = resolve_column_name(
        fact_table,
        configured_foreign_key
    )

    dimension_key = resolve_column_name(
        dimension_table,
        configured_dimension_key
    )

    relationship_name = (
        f"{fact_table}.{configured_foreign_key} -> "
        f"{dimension_table}.{configured_dimension_key}"
    )

    if foreign_key is None or dimension_key is None:
        add_result(
            category="Relationship",
            object_name=relationship_name,
            check_name="Relationship columns available",
            status="FAIL",
            observed_value=False,
            expected_value=True,
            details="One or both relationship columns are missing."
        )
        continue

    fact_keys = (
        get_table(fact_table)
        .select(F.col(f"`{foreign_key}`").alias("foreign_key"))
        .filter(F.col("foreign_key").isNotNull())
    )

    dimension_keys = (
        get_table(dimension_table)
        .select(F.col(f"`{dimension_key}`").alias("dimension_key"))
        .filter(F.col("dimension_key").isNotNull())
        .distinct()
    )

    total_non_null_keys = fact_keys.count()

    orphan_count = (
        fact_keys
        .join(
            dimension_keys,
            fact_keys["foreign_key"] == dimension_keys["dimension_key"],
            "left_anti"
        )
        .count()
    )

    orphan_percentage = (
        round((orphan_count / total_non_null_keys) * 100, 4)
        if total_non_null_keys > 0
        else 0.0
    )

    add_result(
        category="Relationship",
        object_name=(
            f"{fact_table}.{foreign_key} -> "
            f"{dimension_table}.{dimension_key}"
        ),
        check_name="Foreign key integrity",
        status="PASS" if orphan_count == 0 else "FAIL",
        observed_value=f"{orphan_count} ({orphan_percentage}%)",
        expected_value=0,
        details="Rows without a matching dimension key."
    )


# ============================================================
# Numeric profiling
# ============================================================

for table_name in ["sales", "budget"]:
    if not table_exists(table_name):
        continue

    dataframe = get_table(table_name)

    numeric_columns = [
        field.name
        for field in dataframe.schema.fields
        if isinstance(field.dataType, NumericType)
        and not field.name.lower().endswith("_key")
    ]

    for column_name in numeric_columns:
        negative_count = (
            dataframe
            .filter(F.col(f"`{column_name}`") < 0)
            .count()
        )

        zero_count = (
            dataframe
            .filter(F.col(f"`{column_name}`") == 0)
            .count()
        )

        add_result(
            category="Numeric profile",
            object_name=f"{table_name}.{column_name}",
            check_name="Negative values",
            status="PASS" if negative_count == 0 else "WARN",
            observed_value=negative_count,
            expected_value=0,
            details=(
                "Negative values may represent returns, adjustments, "
                "or invalid records."
            )
        )

        add_result(
            category="Numeric profile",
            object_name=f"{table_name}.{column_name}",
            check_name="Zero values",
            status="PASS",
            observed_value=zero_count,
            expected_value="Business dependent",
            details="Zero values reported for business validation."
        )


# ============================================================
# Date profiling
# ============================================================

if table_exists("date"):
    date_dataframe = get_table("date")

    date_columns = [
        field.name
        for field in date_dataframe.schema.fields
        if "date" in field.name.lower()
        and not field.name.lower().endswith("_key")
    ]

    for date_column in date_columns:
        date_range = date_dataframe.agg(
            F.min(F.col(f"`{date_column}`")).alias("minimum_value"),
            F.max(F.col(f"`{date_column}`")).alias("maximum_value")
        ).first()

        add_result(
            category="Date profile",
            object_name=f"date.{date_column}",
            check_name="Date range",
            status="PASS",
            observed_value=(
                f"{date_range['minimum_value']} to "
                f"{date_range['maximum_value']}"
            ),
            expected_value="Valid business date range",
            details="Minimum and maximum values in the date column."
        )


# ============================================================
# Build output DataFrames
# ============================================================

audit_dataframe = (
    spark.createDataFrame(results)
    .withColumn(
        "status_order",
        F.when(F.col("status") == "FAIL", 1)
        .when(F.col("status") == "WARN", 2)
        .otherwise(3)
    )
    .orderBy(
        "status_order",
        "category",
        "object_name",
        "check_name"
    )
)

summary_dataframe = (
    audit_dataframe
    .groupBy("status", "status_order")
    .count()
    .orderBy("status_order")
    .drop("status_order")
)

workspace_metadata_dataframe = spark.createDataFrame(
    workspace_metadata
)

workspace_items_dataframe = (
    spark.createDataFrame(workspace_items)
    if workspace_items
    else None
)

workspace_roles_dataframe = (
    spark.createDataFrame(workspace_roles)
    if workspace_roles
    else None
)

workspace_git_dataframe = (
    spark.createDataFrame(workspace_git)
    if workspace_git
    else None
)

failed_checks = (
    audit_dataframe
    .filter(F.col("status") == "FAIL")
    .count()
)

warning_checks = (
    audit_dataframe
    .filter(F.col("status") == "WARN")
    .count()
)

passed_checks = (
    audit_dataframe
    .filter(F.col("status") == "PASS")
    .count()
)

overall_status = (
    "FAIL"
    if failed_checks > 0
    else "WARN"
    if warning_checks > 0
    else "PASS"
)


# ============================================================
# Display results
# ============================================================

print("=" * 90)
print("MICROSOFT FABRIC UNIFIED LAKEHOUSE AND WORKSPACE AUDIT")
print("=" * 90)
print(f"AUDIT TIMESTAMP UTC: {AUDIT_TIMESTAMP.isoformat()}")
print(f"WORKSPACE: {CURRENT_WORKSPACE_NAME}")
print(f"WORKSPACE ID: {CURRENT_WORKSPACE_ID}")
print(f"NOTEBOOK: {CURRENT_NOTEBOOK_NAME}")
print(f"NOTEBOOK ID: {CURRENT_NOTEBOOK_ID}")
print(f"DEFAULT LAKEHOUSE: {DEFAULT_LAKEHOUSE_NAME}")
print(f"DEFAULT LAKEHOUSE ID: {DEFAULT_LAKEHOUSE_ID}")
print(f"SPARK VERSION: {spark.version}")
print(f"DISCOVERED TABLES: {len(AVAILABLE_TABLES)}")
print(f"DISCOVERED WORKSPACE ITEMS: {len(workspace_items)}")
print(f"DISCOVERED ROLE ASSIGNMENTS: {len(workspace_roles)}")
print(f"OVERALL AUDIT STATUS: {overall_status}")
print(f"PASSED CHECKS: {passed_checks}")
print(f"WARNING CHECKS: {warning_checks}")
print(f"FAILED CHECKS: {failed_checks}")
print("=" * 90)

print("AUDIT SUMMARY")
display(summary_dataframe)

print("CURRENT WORKSPACE AND RUNTIME METADATA")
display(workspace_metadata_dataframe)

print("WORKSPACE ITEM INVENTORY")
if workspace_items_dataframe is not None:
    display(
        workspace_items_dataframe.orderBy(
            "item_type",
            "display_name"
        )
    )
else:
    print("No workspace item inventory was returned.")

print("WORKSPACE ROLE ASSIGNMENTS")
if workspace_roles_dataframe is not None:
    display(
        workspace_roles_dataframe.orderBy(
            "role",
            "principal_type",
            "principal_display_name"
        )
    )
else:
    print("No workspace role assignments were returned.")

print("WORKSPACE GIT CONNECTION")
if workspace_git_dataframe is not None:
    display(workspace_git_dataframe)
else:
    print("No readable Git connection metadata was returned.")

print("FAILED CHECKS")
display(
    audit_dataframe
    .filter(F.col("status") == "FAIL")
    .drop("status_order")
)

print("WARNING CHECKS")
display(
    audit_dataframe
    .filter(F.col("status") == "WARN")
    .drop("status_order")
)

print("DETAILED AUDIT RESULTS")
display(audit_dataframe.drop("status_order"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric Semantic Model Health audit
# Target semantic model: Sales Records Model
# Target workspace: Sales Records Analytics
#
# Outputs:
# 1. semantic_model_health_kpi
# 2. semantic_model_inventory
# 3. semantic_model_relationships
# 4. semantic_model_audit_log
#
# The notebook reads semantic model metadata through SemPy,
# calculates table row counts, builds report-ready audit tables,
# and saves the results as Delta tables in the attached Lakehouse.

from datetime import datetime, timezone
import re
import traceback

import pandas as pd
import sempy.fabric as fabric

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)


# ============================================================
# Configuration
# ============================================================

WORKSPACE_NAME = "Sales Records Analytics"
SEMANTIC_MODEL_NAME = "Sales Records Model"

KPI_TABLE_NAME = "semantic_model_health_kpi"
INVENTORY_TABLE_NAME = "semantic_model_inventory"
RELATIONSHIPS_TABLE_NAME = "semantic_model_relationships"
AUDIT_LOG_TABLE_NAME = "semantic_model_audit_log"

AUDIT_TIMESTAMP = datetime.now(timezone.utc)

FACT_TABLE_NAMES = {
    "sales",
    "budget"
}

DIMENSION_TABLE_NAMES = {
    "date",
    "geography",
    "orderpriority",
    "product",
    "saleschannel"
}

EXPECTED_TABLE_NAMES = (
    FACT_TABLE_NAMES
    | DIMENSION_TABLE_NAMES
)


# ============================================================
# Helper functions
# ============================================================

def normalize_name(value):
    if value is None:
        return ""

    return re.sub(
        r"[^a-z0-9]",
        "",
        str(value).strip().lower()
    )


def safe_string(value):
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    return str(value)


def safe_boolean(value):
    if isinstance(value, bool):
        return value

    normalized = safe_string(value).strip().lower()

    return normalized in {
        "true",
        "1",
        "yes",
        "y"
    }


def safe_integer(value, default_value=0):
    try:
        if value is None or pd.isna(value):
            return default_value

        return int(value)
    except Exception:
        return default_value


def escape_dax_identifier(value):
    return safe_string(value).replace("'", "''")


def find_column(dataframe, candidate_names, required=False):
    normalized_columns = {
        normalize_name(column): column
        for column in dataframe.columns
    }

    for candidate_name in candidate_names:
        normalized_candidate = normalize_name(candidate_name)

        if normalized_candidate in normalized_columns:
            return normalized_columns[normalized_candidate]

    if required:
        raise KeyError(
            "Required column not found. "
            f"Expected one of {candidate_names}. "
            f"Available columns: {list(dataframe.columns)}"
        )

    return None


def get_value(row, dataframe, candidate_names, default_value=""):
    column_name = find_column(
        dataframe,
        candidate_names,
        required=False
    )

    if column_name is None:
        return default_value

    value = row.get(column_name, default_value)

    try:
        if pd.isna(value):
            return default_value
    except Exception:
        pass

    return value


def evaluate_dax(dax_query):
    return fabric.evaluate_dax(
        dataset=SEMANTIC_MODEL_NAME,
        dax_string=dax_query,
        workspace=WORKSPACE_NAME
    )


def execute_metadata_query(
    query_name,
    primary_query,
    fallback_query=None
):
    try:
        result = evaluate_dax(primary_query)

        audit_events.append({
            "audit_timestamp_utc": AUDIT_TIMESTAMP,
            "audit_area": "Semantic model metadata",
            "object_name": query_name,
            "status": "PASS",
            "details": (
                f"Metadata query completed. "
                f"Returned rows: {len(result)}."
            )
        })

        return result

    except Exception as primary_error:
        if fallback_query is not None:
            try:
                result = evaluate_dax(fallback_query)

                audit_events.append({
                    "audit_timestamp_utc": AUDIT_TIMESTAMP,
                    "audit_area": "Semantic model metadata",
                    "object_name": query_name,
                    "status": "WARN",
                    "details": (
                        "The selected metadata query was not supported. "
                        "The full INFO.VIEW query completed successfully. "
                        f"Primary error: {type(primary_error).__name__}: "
                        f"{primary_error}"
                    )
                })

                return result

            except Exception as fallback_error:
                raise RuntimeError(
                    f"{query_name} failed. "
                    f"Primary error: {primary_error}. "
                    f"Fallback error: {fallback_error}."
                ) from fallback_error

        raise


def classify_table(table_name):
    normalized_table_name = safe_string(
        table_name
    ).strip().lower()

    if normalized_table_name in FACT_TABLE_NAMES:
        return "Fact"

    if normalized_table_name in DIMENSION_TABLE_NAMES:
        return "Dimension"

    return "Other"


def get_table_use_case(table_name, object_type):
    normalized_table_name = safe_string(
        table_name
    ).strip().lower()

    use_cases = {
        "sales": (
            "Sales, revenue, cost, profit, margin, units, "
            "channel, product, geography, and order analysis."
        ),
        "budget": (
            "Actual versus budget analysis for revenue, cost, "
            "profit, margin, and units."
        ),
        "date": (
            "Time intelligence, period filtering, monthly trends, "
            "year-over-year analysis, and budget alignment."
        ),
        "geography": (
            "Regional, country, territory, and geographic "
            "performance analysis."
        ),
        "orderpriority": (
            "Order urgency, fulfillment priority, and operational "
            "service-level analysis."
        ),
        "product": (
            "Product, category, portfolio, revenue, profitability, "
            "and unit performance analysis."
        ),
        "saleschannel": (
            "Channel mix, channel revenue, channel profitability, "
            "and channel performance analysis."
        )
    }

    if normalized_table_name in use_cases:
        return use_cases[normalized_table_name]

    if object_type == "Measure":
        return (
            "Reusable business calculation for KPI cards, "
            "comparisons, trends, filtering, and report analysis."
        )

    if object_type == "Column":
        return (
            "Filtering, grouping, slicing, drill-down, relationship, "
            "or detailed analytical context."
        )

    return "Semantic model reporting and analytical exploration."


def get_status(
    object_type,
    description,
    state="",
    row_count=None
):
    normalized_state = safe_string(state).strip().lower()

    if normalized_state and normalized_state not in {
        "ready",
        "valid",
        "active"
    }:
        return "ERROR"

    if object_type == "Table" and row_count is not None:
        if row_count == 0:
            return "WARNING"

    if not safe_string(description).strip():
        return "WARNING"

    return "OK"


def spark_dataframe_from_records(records, schema):
    return spark.createDataFrame(
        records,
        schema=schema
    )


# ============================================================
# Audit initialization
# ============================================================

audit_events = []

print("=" * 90)
print("MICROSOFT FABRIC SEMANTIC MODEL HEALTH AUDIT")
print("=" * 90)
print(f"WORKSPACE: {WORKSPACE_NAME}")
print(f"SEMANTIC MODEL: {SEMANTIC_MODEL_NAME}")
print(f"AUDIT TIMESTAMP UTC: {AUDIT_TIMESTAMP.isoformat()}")
print("=" * 90)


# ============================================================
# 1. Verify semantic model availability
# ============================================================

semantic_models_dataframe = fabric.list_datasets(
    workspace=WORKSPACE_NAME
)

dataset_name_column = find_column(
    semantic_models_dataframe,
    [
        "Dataset Name",
        "DatasetName",
        "Name",
        "Display Name",
        "DisplayName"
    ],
    required=True
)

available_semantic_models = (
    semantic_models_dataframe[dataset_name_column]
    .astype(str)
    .tolist()
)

semantic_model_exists = any(
    model_name.strip().lower()
    == SEMANTIC_MODEL_NAME.lower()
    for model_name in available_semantic_models
)

if not semantic_model_exists:
    raise RuntimeError(
        f"Semantic model '{SEMANTIC_MODEL_NAME}' was not found "
        f"in workspace '{WORKSPACE_NAME}'. "
        f"Available semantic models: {available_semantic_models}"
    )

audit_events.append({
    "audit_timestamp_utc": AUDIT_TIMESTAMP,
    "audit_area": "Semantic model",
    "object_name": SEMANTIC_MODEL_NAME,
    "status": "PASS",
    "details": (
        f"Semantic model found in workspace "
        f"'{WORKSPACE_NAME}'."
    )
})


# ============================================================
# 2. Read semantic model metadata
# ============================================================

tables_dataframe = execute_metadata_query(
    query_name="Tables",
    primary_query="""
EVALUATE
SELECTCOLUMNS(
    INFO.VIEW.TABLES(),
    "Table", [Name],
    "StorageMode", [StorageMode],
    "IsHidden", [IsHidden],
    "Description", [Description],
    "DataCategory", [DataCategory]
)
ORDER BY [Table]
""",
    fallback_query="""
EVALUATE
INFO.VIEW.TABLES()
"""
)

columns_dataframe = execute_metadata_query(
    query_name="Columns",
    primary_query="""
EVALUATE
SELECTCOLUMNS(
    INFO.VIEW.COLUMNS(),
    "Table", [Table],
    "Column", [Name],
    "DataType", [DataType],
    "SourceColumn", [SourceColumn],
    "IsHidden", [IsHidden],
    "IsKey", [IsKey],
    "IsUnique", [IsUnique],
    "Description", [Description],
    "DataCategory", [DataCategory]
)
ORDER BY [Table], [Column]
""",
    fallback_query="""
EVALUATE
INFO.VIEW.COLUMNS()
"""
)

measures_dataframe = execute_metadata_query(
    query_name="Measures",
    primary_query="""
EVALUATE
SELECTCOLUMNS(
    INFO.VIEW.MEASURES(),
    "Table", [Table],
    "Measure", [Name],
    "Description", [Description],
    "Expression", [Expression],
    "FormatString", [FormatString],
    "IsHidden", [IsHidden],
    "State", [State],
    "DisplayFolder", [DisplayFolder],
    "DataCategory", [DataCategory]
)
ORDER BY [Table], [Measure]
""",
    fallback_query="""
EVALUATE
INFO.VIEW.MEASURES()
"""
)

relationships_dataframe = execute_metadata_query(
    query_name="Relationships",
    primary_query="""
EVALUATE
SELECTCOLUMNS(
    INFO.VIEW.RELATIONSHIPS(),
    "Relationship", [Name],
    "FromTable", [FromTable],
    "FromColumn", [FromColumn],
    "ToTable", [ToTable],
    "ToColumn", [ToColumn],
    "IsActive", [IsActive],
    "CrossFilteringBehavior", [CrossFilteringBehavior],
    "FromCardinality", [FromCardinality],
    "ToCardinality", [ToCardinality]
)
ORDER BY [FromTable], [ToTable]
""",
    fallback_query="""
EVALUATE
INFO.VIEW.RELATIONSHIPS()
"""
)


# ============================================================
# 3. Normalize table metadata
# ============================================================

table_name_column = find_column(
    tables_dataframe,
    ["Table", "Name"],
    required=True
)

table_storage_column = find_column(
    tables_dataframe,
    ["StorageMode", "Storage Mode"]
)

table_hidden_column = find_column(
    tables_dataframe,
    ["IsHidden", "Hidden"]
)

table_description_column = find_column(
    tables_dataframe,
    ["Description"]
)

table_category_column = find_column(
    tables_dataframe,
    ["DataCategory", "Data Category"]
)

model_table_names = []

for _, table_row in tables_dataframe.iterrows():
    table_name = safe_string(
        table_row.get(table_name_column)
    ).strip()

    if table_name:
        model_table_names.append(table_name)


# ============================================================
# 4. Calculate row counts
# ============================================================

table_row_counts = {}
row_count_errors = {}

for table_name in model_table_names:
    escaped_table_name = escape_dax_identifier(table_name)

    row_count_query = f"""
EVALUATE
ROW(
    "Table", "{table_name}",
    "RowCount", COUNTROWS('{escaped_table_name}')
)
"""

    try:
        row_count_dataframe = evaluate_dax(row_count_query)

        row_count_column = find_column(
            row_count_dataframe,
            ["RowCount", "Row Count"],
            required=True
        )

        row_count = safe_integer(
            row_count_dataframe.iloc[0][row_count_column]
        )

        table_row_counts[table_name] = row_count

        audit_events.append({
            "audit_timestamp_utc": AUDIT_TIMESTAMP,
            "audit_area": "Table row count",
            "object_name": table_name,
            "status": "PASS",
            "details": f"Row count: {row_count}."
        })

    except Exception as error:
        table_row_counts[table_name] = None
        row_count_errors[table_name] = (
            f"{type(error).__name__}: {error}"
        )

        audit_events.append({
            "audit_timestamp_utc": AUDIT_TIMESTAMP,
            "audit_area": "Table row count",
            "object_name": table_name,
            "status": "WARN",
            "details": (
                "Row count could not be calculated. "
                f"{type(error).__name__}: {error}"
            )
        })


# ============================================================
# 5. Build table inventory
# ============================================================

inventory_records = []

for _, table_row in tables_dataframe.iterrows():
    table_name = safe_string(
        table_row.get(table_name_column)
    ).strip()

    description = (
        safe_string(table_row.get(table_description_column))
        if table_description_column
        else ""
    )

    storage_mode = (
        safe_string(table_row.get(table_storage_column))
        if table_storage_column
        else ""
    )

    is_hidden = (
        safe_boolean(table_row.get(table_hidden_column))
        if table_hidden_column
        else False
    )

    data_category = (
        safe_string(table_row.get(table_category_column))
        if table_category_column
        else ""
    )

    row_count = table_row_counts.get(table_name)
    table_role = classify_table(table_name)

    inventory_records.append({
        "audit_timestamp_utc": AUDIT_TIMESTAMP,
        "workspace_name": WORKSPACE_NAME,
        "semantic_model_name": SEMANTIC_MODEL_NAME,
        "object_type": "Table",
        "object_name": table_name,
        "parent_table": table_name,
        "table_role": table_role,
        "row_count": row_count,
        "data_type": "",
        "storage_mode": storage_mode,
        "is_hidden": is_hidden,
        "is_visible": not is_hidden,
        "is_key": False,
        "is_unique": False,
        "is_active": True,
        "description": description,
        "expression": "",
        "format_string": "",
        "display_folder": "",
        "data_category": data_category,
        "state": "",
        "status": get_status(
            object_type="Table",
            description=description,
            row_count=row_count
        ),
        "possible_use": get_table_use_case(
            table_name=table_name,
            object_type="Table"
        )
    })


# ============================================================
# 6. Build column inventory
# ============================================================

column_table_column = find_column(
    columns_dataframe,
    ["Table", "TableName", "Table Name"],
    required=True
)

column_name_column = find_column(
    columns_dataframe,
    ["Column", "Name", "ColumnName", "Column Name"],
    required=True
)

column_data_type_column = find_column(
    columns_dataframe,
    ["DataType", "Data Type"]
)

column_source_column = find_column(
    columns_dataframe,
    ["SourceColumn", "Source Column"]
)

column_hidden_column = find_column(
    columns_dataframe,
    ["IsHidden", "Hidden"]
)

column_key_column = find_column(
    columns_dataframe,
    ["IsKey", "Key"]
)

column_unique_column = find_column(
    columns_dataframe,
    ["IsUnique", "Unique"]
)

column_description_column = find_column(
    columns_dataframe,
    ["Description"]
)

column_category_column = find_column(
    columns_dataframe,
    ["DataCategory", "Data Category"]
)

for _, column_row in columns_dataframe.iterrows():
    parent_table = safe_string(
        column_row.get(column_table_column)
    ).strip()

    column_name = safe_string(
        column_row.get(column_name_column)
    ).strip()

    description = (
        safe_string(column_row.get(column_description_column))
        if column_description_column
        else ""
    )

    data_type = (
        safe_string(column_row.get(column_data_type_column))
        if column_data_type_column
        else ""
    )

    source_column = (
        safe_string(column_row.get(column_source_column))
        if column_source_column
        else ""
    )

    is_hidden = (
        safe_boolean(column_row.get(column_hidden_column))
        if column_hidden_column
        else False
    )

    is_key = (
        safe_boolean(column_row.get(column_key_column))
        if column_key_column
        else False
    )

    is_unique = (
        safe_boolean(column_row.get(column_unique_column))
        if column_unique_column
        else False
    )

    data_category = (
        safe_string(column_row.get(column_category_column))
        if column_category_column
        else ""
    )

    column_expression = (
        f"Source column: {source_column}"
        if source_column
        else ""
    )

    inventory_records.append({
        "audit_timestamp_utc": AUDIT_TIMESTAMP,
        "workspace_name": WORKSPACE_NAME,
        "semantic_model_name": SEMANTIC_MODEL_NAME,
        "object_type": "Column",
        "object_name": column_name,
        "parent_table": parent_table,
        "table_role": classify_table(parent_table),
        "row_count": None,
        "data_type": data_type,
        "storage_mode": "",
        "is_hidden": is_hidden,
        "is_visible": not is_hidden,
        "is_key": is_key,
        "is_unique": is_unique,
        "is_active": True,
        "description": description,
        "expression": column_expression,
        "format_string": "",
        "display_folder": "",
        "data_category": data_category,
        "state": "",
        "status": get_status(
            object_type="Column",
            description=description
        ),
        "possible_use": get_table_use_case(
            table_name=parent_table,
            object_type="Column"
        )
    })


# ============================================================
# 7. Build measure inventory
# ============================================================

measure_table_column = find_column(
    measures_dataframe,
    ["Table", "TableName", "Table Name"],
    required=True
)

measure_name_column = find_column(
    measures_dataframe,
    ["Measure", "Name", "MeasureName", "Measure Name"],
    required=True
)

measure_description_column = find_column(
    measures_dataframe,
    ["Description"]
)

measure_expression_column = find_column(
    measures_dataframe,
    ["Expression", "DAX Formula", "Formula"]
)

measure_format_column = find_column(
    measures_dataframe,
    ["FormatString", "Format String"]
)

measure_hidden_column = find_column(
    measures_dataframe,
    ["IsHidden", "Hidden"]
)

measure_state_column = find_column(
    measures_dataframe,
    ["State"]
)

measure_folder_column = find_column(
    measures_dataframe,
    ["DisplayFolder", "Display Folder"]
)

measure_category_column = find_column(
    measures_dataframe,
    ["DataCategory", "Data Category"]
)

for _, measure_row in measures_dataframe.iterrows():
    parent_table = safe_string(
        measure_row.get(measure_table_column)
    ).strip()

    measure_name = safe_string(
        measure_row.get(measure_name_column)
    ).strip()

    description = (
        safe_string(measure_row.get(measure_description_column))
        if measure_description_column
        else ""
    )

    expression = (
        safe_string(measure_row.get(measure_expression_column))
        if measure_expression_column
        else ""
    )

    format_string = (
        safe_string(measure_row.get(measure_format_column))
        if measure_format_column
        else ""
    )

    is_hidden = (
        safe_boolean(measure_row.get(measure_hidden_column))
        if measure_hidden_column
        else False
    )

    state = (
        safe_string(measure_row.get(measure_state_column))
        if measure_state_column
        else ""
    )

    display_folder = (
        safe_string(measure_row.get(measure_folder_column))
        if measure_folder_column
        else ""
    )

    data_category = (
        safe_string(measure_row.get(measure_category_column))
        if measure_category_column
        else ""
    )

    inventory_records.append({
        "audit_timestamp_utc": AUDIT_TIMESTAMP,
        "workspace_name": WORKSPACE_NAME,
        "semantic_model_name": SEMANTIC_MODEL_NAME,
        "object_type": "Measure",
        "object_name": measure_name,
        "parent_table": parent_table,
        "table_role": classify_table(parent_table),
        "row_count": None,
        "data_type": "Measure",
        "storage_mode": "",
        "is_hidden": is_hidden,
        "is_visible": not is_hidden,
        "is_key": False,
        "is_unique": False,
        "is_active": True,
        "description": description,
        "expression": expression,
        "format_string": format_string,
        "display_folder": display_folder,
        "data_category": data_category,
        "state": state,
        "status": get_status(
            object_type="Measure",
            description=description,
            state=state
        ),
        "possible_use": get_table_use_case(
            table_name=parent_table,
            object_type="Measure"
        )
    })


# ============================================================
# 8. Build relationship inventory
# ============================================================

relationship_name_column = find_column(
    relationships_dataframe,
    ["Relationship", "Name"]
)

relationship_from_table_column = find_column(
    relationships_dataframe,
    ["FromTable", "From Table"],
    required=True
)

relationship_from_column_column = find_column(
    relationships_dataframe,
    ["FromColumn", "From Column"],
    required=True
)

relationship_to_table_column = find_column(
    relationships_dataframe,
    ["ToTable", "To Table"],
    required=True
)

relationship_to_column_column = find_column(
    relationships_dataframe,
    ["ToColumn", "To Column"],
    required=True
)

relationship_active_column = find_column(
    relationships_dataframe,
    ["IsActive", "Active"]
)

relationship_filter_column = find_column(
    relationships_dataframe,
    [
        "CrossFilteringBehavior",
        "Cross Filtering Behavior"
    ]
)

relationship_from_cardinality_column = find_column(
    relationships_dataframe,
    ["FromCardinality", "From Cardinality"]
)

relationship_to_cardinality_column = find_column(
    relationships_dataframe,
    ["ToCardinality", "To Cardinality"]
)

relationship_records = []

for _, relationship_row in relationships_dataframe.iterrows():
    from_table = safe_string(
        relationship_row.get(
            relationship_from_table_column
        )
    ).strip()

    from_column = safe_string(
        relationship_row.get(
            relationship_from_column_column
        )
    ).strip()

    to_table = safe_string(
        relationship_row.get(
            relationship_to_table_column
        )
    ).strip()

    to_column = safe_string(
        relationship_row.get(
            relationship_to_column_column
        )
    ).strip()

    relationship_name = (
        safe_string(
            relationship_row.get(relationship_name_column)
        ).strip()
        if relationship_name_column
        else ""
    )

    if not relationship_name:
        relationship_name = (
            f"{from_table}.{from_column} -> "
            f"{to_table}.{to_column}"
        )

    is_active = (
        safe_boolean(
            relationship_row.get(relationship_active_column)
        )
        if relationship_active_column
        else False
    )

    cross_filtering_behavior = (
        safe_string(
            relationship_row.get(relationship_filter_column)
        )
        if relationship_filter_column
        else ""
    )

    from_cardinality = (
        safe_string(
            relationship_row.get(
                relationship_from_cardinality_column
            )
        )
        if relationship_from_cardinality_column
        else ""
    )

    to_cardinality = (
        safe_string(
            relationship_row.get(
                relationship_to_cardinality_column
            )
        )
        if relationship_to_cardinality_column
        else ""
    )

    relationship_records.append({
        "audit_timestamp_utc": AUDIT_TIMESTAMP,
        "workspace_name": WORKSPACE_NAME,
        "semantic_model_name": SEMANTIC_MODEL_NAME,
        "relationship_name": relationship_name,
        "from_table": from_table,
        "from_column": from_column,
        "to_table": to_table,
        "to_column": to_column,
        "from_cardinality": from_cardinality,
        "to_cardinality": to_cardinality,
        "cross_filtering_behavior": cross_filtering_behavior,
        "is_active": is_active,
        "status": "OK" if is_active else "WARNING"
    })


# ============================================================
# 9. Calculate KPI values
# ============================================================

table_records = [
    record
    for record in inventory_records
    if record["object_type"] == "Table"
]

column_records = [
    record
    for record in inventory_records
    if record["object_type"] == "Column"
]

measure_records = [
    record
    for record in inventory_records
    if record["object_type"] == "Measure"
]

fact_table_count = sum(
    1
    for record in table_records
    if record["table_role"] == "Fact"
)

dimension_table_count = sum(
    1
    for record in table_records
    if record["table_role"] == "Dimension"
)

visible_table_count = sum(
    1
    for record in table_records
    if record["is_visible"]
)

visible_column_count = sum(
    1
    for record in column_records
    if record["is_visible"]
)

visible_measure_count = sum(
    1
    for record in measure_records
    if record["is_visible"]
)

hidden_object_count = sum(
    1
    for record in inventory_records
    if record["is_hidden"]
)

active_relationship_count = sum(
    1
    for record in relationship_records
    if record["is_active"]
)

inactive_relationship_count = (
    len(relationship_records)
    - active_relationship_count
)

measure_without_description_count = sum(
    1
    for record in measure_records
    if not record["description"].strip()
)

column_without_description_count = sum(
    1
    for record in column_records
    if not record["description"].strip()
)

table_without_description_count = sum(
    1
    for record in table_records
    if not record["description"].strip()
)

measure_error_count = sum(
    1
    for record in measure_records
    if record["status"] == "ERROR"
)

inventory_warning_count = sum(
    1
    for record in inventory_records
    if record["status"] == "WARNING"
)

inventory_error_count = sum(
    1
    for record in inventory_records
    if record["status"] == "ERROR"
)

total_model_rows = sum(
    record["row_count"]
    for record in table_records
    if record["row_count"] is not None
)

documentable_object_count = (
    len(table_records)
    + len(column_records)
    + len(measure_records)
)

documented_object_count = sum(
    1
    for record in inventory_records
    if record["description"].strip()
)

documentation_completeness_pct = (
    round(
        documented_object_count
        / documentable_object_count
        * 100,
        2
    )
    if documentable_object_count > 0
    else 0.0
)

missing_expected_tables = sorted(
    EXPECTED_TABLE_NAMES
    - {
        record["object_name"].strip().lower()
        for record in table_records
    }
)


# ============================================================
# 10. Build KPI table
# ============================================================

kpi_records = []


def add_kpi(
    kpi_order,
    kpi_name,
    kpi_value,
    kpi_value_text,
    status,
    target,
    description,
    report_group
):
    kpi_records.append({
        "audit_timestamp_utc": AUDIT_TIMESTAMP,
        "workspace_name": WORKSPACE_NAME,
        "semantic_model_name": SEMANTIC_MODEL_NAME,
        "kpi_order": kpi_order,
        "kpi_name": kpi_name,
        "kpi_value": float(kpi_value),
        "kpi_value_text": safe_string(kpi_value_text),
        "status": status,
        "target": target,
        "description": description,
        "report_group": report_group
    })


add_kpi(
    1,
    "Tables",
    len(table_records),
    len(table_records),
    "OK" if not missing_expected_tables else "ERROR",
    "7 expected business tables",
    "Total number of tables in the semantic model.",
    "Model structure"
)

add_kpi(
    2,
    "Fact Tables",
    fact_table_count,
    fact_table_count,
    "OK" if fact_table_count == 2 else "WARNING",
    "2",
    "Tables classified as sales or budget facts.",
    "Model structure"
)

add_kpi(
    3,
    "Dimension Tables",
    dimension_table_count,
    dimension_table_count,
    "OK" if dimension_table_count == 5 else "WARNING",
    "5",
    "Tables classified as analytical dimensions.",
    "Model structure"
)

add_kpi(
    4,
    "Columns",
    len(column_records),
    len(column_records),
    "OK",
    "Informational",
    "Total number of model columns.",
    "Model structure"
)

add_kpi(
    5,
    "Measures",
    len(measure_records),
    len(measure_records),
    "ERROR" if measure_error_count > 0 else "OK",
    "No measure errors",
    "Total number of explicit measures.",
    "Model structure"
)

add_kpi(
    6,
    "Relationships",
    len(relationship_records),
    len(relationship_records),
    "OK" if relationship_records else "ERROR",
    "At least one",
    "Total number of semantic model relationships.",
    "Model structure"
)

add_kpi(
    7,
    "Active Relationships",
    active_relationship_count,
    active_relationship_count,
    "OK" if active_relationship_count > 0 else "ERROR",
    "At least one",
    "Relationships active during normal filter propagation.",
    "Relationships"
)

add_kpi(
    8,
    "Inactive Relationships",
    inactive_relationship_count,
    inactive_relationship_count,
    "OK",
    "Business dependent",
    "Relationships requiring USERELATIONSHIP or equivalent logic.",
    "Relationships"
)

add_kpi(
    9,
    "Total Model Rows",
    total_model_rows,
    f"{total_model_rows:,}",
    "WARNING" if row_count_errors else "OK",
    "All table counts available",
    "Combined physical row count across model tables.",
    "Data volume"
)

add_kpi(
    10,
    "Visible Tables",
    visible_table_count,
    visible_table_count,
    "OK",
    "Informational",
    "Tables visible to report authors.",
    "Visibility"
)

add_kpi(
    11,
    "Visible Columns",
    visible_column_count,
    visible_column_count,
    "OK",
    "Informational",
    "Columns visible to report authors.",
    "Visibility"
)

add_kpi(
    12,
    "Visible Measures",
    visible_measure_count,
    visible_measure_count,
    "OK",
    "Informational",
    "Measures visible to report authors.",
    "Visibility"
)

add_kpi(
    13,
    "Hidden Objects",
    hidden_object_count,
    hidden_object_count,
    "OK",
    "Business dependent",
    "Hidden tables, columns, and measures.",
    "Visibility"
)

add_kpi(
    14,
    "Measure Errors",
    measure_error_count,
    measure_error_count,
    "ERROR" if measure_error_count > 0 else "OK",
    "0",
    "Measures whose metadata state indicates an error.",
    "Quality"
)

add_kpi(
    15,
    "Measures Without Description",
    measure_without_description_count,
    measure_without_description_count,
    (
        "OK"
        if measure_without_description_count == 0
        else "WARNING"
    ),
    "0",
    "Explicit measures without documentation.",
    "Documentation"
)

add_kpi(
    16,
    "Columns Without Description",
    column_without_description_count,
    column_without_description_count,
    (
        "OK"
        if column_without_description_count == 0
        else "WARNING"
    ),
    "0",
    "Model columns without documentation.",
    "Documentation"
)

add_kpi(
    17,
    "Tables Without Description",
    table_without_description_count,
    table_without_description_count,
    (
        "OK"
        if table_without_description_count == 0
        else "WARNING"
    ),
    "0",
    "Model tables without documentation.",
    "Documentation"
)

add_kpi(
    18,
    "Documentation Completeness",
    documentation_completeness_pct,
    f"{documentation_completeness_pct:.2f}%",
    (
        "OK"
        if documentation_completeness_pct >= 90
        else "WARNING"
    ),
    "At least 90%",
    (
        "Percentage of tables, columns, and measures "
        "with non-empty descriptions."
    ),
    "Documentation"
)

add_kpi(
    19,
    "Inventory Warnings",
    inventory_warning_count,
    inventory_warning_count,
    "OK" if inventory_warning_count == 0 else "WARNING",
    "0",
    "Inventory objects requiring review.",
    "Quality"
)

add_kpi(
    20,
    "Inventory Errors",
    inventory_error_count,
    inventory_error_count,
    "ERROR" if inventory_error_count > 0 else "OK",
    "0",
    "Inventory objects with invalid metadata state.",
    "Quality"
)


# ============================================================
# 11. Define output schemas
# ============================================================

inventory_schema = StructType([
    StructField("audit_timestamp_utc", TimestampType(), False),
    StructField("workspace_name", StringType(), False),
    StructField("semantic_model_name", StringType(), False),
    StructField("object_type", StringType(), False),
    StructField("object_name", StringType(), False),
    StructField("parent_table", StringType(), False),
    StructField("table_role", StringType(), False),
    StructField("row_count", LongType(), True),
    StructField("data_type", StringType(), False),
    StructField("storage_mode", StringType(), False),
    StructField("is_hidden", BooleanType(), False),
    StructField("is_visible", BooleanType(), False),
    StructField("is_key", BooleanType(), False),
    StructField("is_unique", BooleanType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("description", StringType(), False),
    StructField("expression", StringType(), False),
    StructField("format_string", StringType(), False),
    StructField("display_folder", StringType(), False),
    StructField("data_category", StringType(), False),
    StructField("state", StringType(), False),
    StructField("status", StringType(), False),
    StructField("possible_use", StringType(), False)
])

relationship_schema = StructType([
    StructField("audit_timestamp_utc", TimestampType(), False),
    StructField("workspace_name", StringType(), False),
    StructField("semantic_model_name", StringType(), False),
    StructField("relationship_name", StringType(), False),
    StructField("from_table", StringType(), False),
    StructField("from_column", StringType(), False),
    StructField("to_table", StringType(), False),
    StructField("to_column", StringType(), False),
    StructField("from_cardinality", StringType(), False),
    StructField("to_cardinality", StringType(), False),
    StructField("cross_filtering_behavior", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("status", StringType(), False)
])

audit_log_schema = StructType([
    StructField("audit_timestamp_utc", TimestampType(), False),
    StructField("audit_area", StringType(), False),
    StructField("object_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("details", StringType(), False)
])


# ============================================================
# 12. Create Spark DataFrames
# ============================================================

inventory_spark_dataframe = spark_dataframe_from_records(
    inventory_records,
    inventory_schema
)

relationships_spark_dataframe = spark_dataframe_from_records(
    relationship_records,
    relationship_schema
)

kpi_spark_dataframe = spark.createDataFrame(
    pd.DataFrame(kpi_records)
)

audit_log_spark_dataframe = spark_dataframe_from_records(
    audit_events,
    audit_log_schema
)


# ============================================================
# 13. Save report-ready Delta tables
# ============================================================

(
    kpi_spark_dataframe
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(KPI_TABLE_NAME)
)

(
    inventory_spark_dataframe
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(INVENTORY_TABLE_NAME)
)

(
    relationships_spark_dataframe
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RELATIONSHIPS_TABLE_NAME)
)

(
    audit_log_spark_dataframe
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(AUDIT_LOG_TABLE_NAME)
)


# ============================================================
# 14. Final status
# ============================================================

critical_error_count = sum(
    1
    for record in kpi_records
    if record["status"] == "ERROR"
)

warning_count = sum(
    1
    for record in kpi_records
    if record["status"] == "WARNING"
)

overall_status = (
    "FAIL"
    if critical_error_count > 0
    else "WARN"
    if warning_count > 0
    else "PASS"
)


# ============================================================
# 15. Display audit results
# ============================================================

print("=" * 90)
print("SEMANTIC MODEL HEALTH AUDIT COMPLETED")
print("=" * 90)
print(f"WORKSPACE: {WORKSPACE_NAME}")
print(f"SEMANTIC MODEL: {SEMANTIC_MODEL_NAME}")
print(f"TABLES: {len(table_records)}")
print(f"FACT TABLES: {fact_table_count}")
print(f"DIMENSION TABLES: {dimension_table_count}")
print(f"COLUMNS: {len(column_records)}")
print(f"MEASURES: {len(measure_records)}")
print(f"RELATIONSHIPS: {len(relationship_records)}")
print(f"ACTIVE RELATIONSHIPS: {active_relationship_count}")
print(f"INACTIVE RELATIONSHIPS: {inactive_relationship_count}")
print(f"TOTAL MODEL ROWS: {total_model_rows:,}")
print(
    "DOCUMENTATION COMPLETENESS: "
    f"{documentation_completeness_pct:.2f}%"
)
print(f"OVERALL AUDIT STATUS: {overall_status}")
print(f"KPI WARNINGS: {warning_count}")
print(f"KPI ERRORS: {critical_error_count}")
print("=" * 90)
print("CREATED DELTA TABLES")
print(f"1. {KPI_TABLE_NAME}")
print(f"2. {INVENTORY_TABLE_NAME}")
print(f"3. {RELATIONSHIPS_TABLE_NAME}")
print(f"4. {AUDIT_LOG_TABLE_NAME}")
print("=" * 90)

print("PAGE 1 DATA: KPI OVERVIEW")
display(
    kpi_spark_dataframe.orderBy("kpi_order")
)

print("PAGE 2 DATA: MODEL INVENTORY")
display(
    inventory_spark_dataframe.orderBy(
        "object_type",
        "parent_table",
        "object_name"
    )
)

print("RELATIONSHIP INVENTORY")
display(
    relationships_spark_dataframe.orderBy(
        "from_table",
        "to_table",
        "relationship_name"
    )
)

print("AUDIT LOG")
display(
    audit_log_spark_dataframe.orderBy(
        "status",
        "audit_area",
        "object_name"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect the existing PBIR report definition without modifying it.

import subprocess
import sys

try:
    from sempy_labs.report import ReportWrapper
except ImportError:
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--quiet",
            "semantic-link-labs"
        ]
    )
    from sempy_labs.report import ReportWrapper


WORKSPACE_NAME = "Sales Records Analytics"
REPORT_NAME = "Semantic Model Health Report"


report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=True,
    show_diffs=False
)

print("=" * 80)
print("PBIR REPORT INSPECTION")
print("=" * 80)
print(f"Workspace: {WORKSPACE_NAME}")
print(f"Report: {REPORT_NAME}")

print("\nREPORT DEFINITION PATHS")
display(report.list_paths())

print("\nREPORT PAGES")
display(report.list_pages())

print("\nREPORT VISUALS")
display(report.list_visuals())

print("\nPAGES CONFIGURATION")
pages_configuration = report.get(
    "definition/pages/pages.json",
    verbose=False
)
print(pages_configuration)

print("\nREPORT CONFIGURATION")
report_configuration = report.get(
    "definition/report.json",
    verbose=False
)
print(report_configuration)

print("\nPBIR INSPECTION COMPLETED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PBIR report generation recovery script
# Fixes the invalid visual filter definition and retries
# the complete report update.

from copy import deepcopy

from sempy_labs.report import ReportWrapper


# ============================================================
# Configuration
# ============================================================

WORKSPACE_NAME = "Sales Records Analytics"
REPORT_NAME = "Semantic Model Health Report"

KPI_PAGE_NAME = "semantic_health_kpi"
INVENTORY_PAGE_NAME = "semantic_health_inventory"

PAGE_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/"
    "fabric/item/report/definition/page/2.3.1/schema.json"
)

PAGES_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/"
    "fabric/item/report/definition/"
    "pagesMetadata/1.1.0/schema.json"
)


# ============================================================
# Validate required objects from the report-generation cell
# ============================================================

REQUIRED_OBJECTS = [
    "kpi_visuals",
    "inventory_visual"
]

missing_objects = [
    object_name
    for object_name in REQUIRED_OBJECTS
    if object_name not in globals()
]

if missing_objects:
    raise RuntimeError(
        "Required objects from the report-generation cell are missing: "
        f"{missing_objects}. Keep the current Spark session active."
    )


# ============================================================
# Create valid page definition
# ============================================================

def create_valid_page(
    page_name,
    display_name
):
    return {
        "$schema": PAGE_SCHEMA,
        "name": page_name,
        "displayName": display_name,
        "displayOption": "FitToPage",
        "width": 1280,
        "height": 720
    }


# ============================================================
# Correct all KPI visual filter definitions
# ============================================================

corrected_kpi_visuals = deepcopy(kpi_visuals)

for visual_definition in corrected_kpi_visuals:
    filter_config = visual_definition.get(
        "filterConfig",
        {}
    )

    filters = filter_config.get(
        "filters",
        []
    )

    for filter_definition in filters:
        filter_definition.pop(
            "expression",
            None
        )


# ============================================================
# Connect to the unchanged PBIR report
# ============================================================

report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False,
    show_diffs=False
)


# ============================================================
# Remove all existing pages and visuals
# ============================================================

existing_pages = report.list_pages()

for _, existing_page in existing_pages.iterrows():
    existing_page_name = existing_page[
        "Page Name"
    ]

    existing_page_path = existing_page[
        "File Path"
    ]

    existing_visual_pattern = (
        f"definition/pages/"
        f"{existing_page_name}/visuals/*/visual.json"
    )

    try:
        report.remove(
            existing_visual_pattern,
            verbose=False
        )
    except Exception:
        pass

    report.remove(
        existing_page_path,
        verbose=False
    )


# ============================================================
# Add KPI Overview page
# ============================================================

kpi_page_path = (
    f"definition/pages/"
    f"{KPI_PAGE_NAME}/page.json"
)

report.add(
    kpi_page_path,
    create_valid_page(
        page_name=KPI_PAGE_NAME,
        display_name="KPI Overview"
    )
)

for visual_definition in corrected_kpi_visuals:
    visual_name = visual_definition[
        "name"
    ]

    visual_path = (
        f"definition/pages/"
        f"{KPI_PAGE_NAME}/visuals/"
        f"{visual_name}/visual.json"
    )

    report.add(
        visual_path,
        visual_definition
    )


# ============================================================
# Add Model Inventory page
# ============================================================

inventory_page_path = (
    f"definition/pages/"
    f"{INVENTORY_PAGE_NAME}/page.json"
)

report.add(
    inventory_page_path,
    create_valid_page(
        page_name=INVENTORY_PAGE_NAME,
        display_name="Model Inventory"
    )
)

inventory_visual_definition = deepcopy(
    inventory_visual
)

inventory_visual_path = (
    f"definition/pages/"
    f"{INVENTORY_PAGE_NAME}/visuals/"
    f"{inventory_visual_definition['name']}/visual.json"
)

report.add(
    inventory_visual_path,
    inventory_visual_definition
)


# ============================================================
# Update page order
# ============================================================

pages_metadata = {
    "$schema": PAGES_SCHEMA,
    "pageOrder": [
        KPI_PAGE_NAME,
        INVENTORY_PAGE_NAME
    ],
    "activePageName": KPI_PAGE_NAME
}

report.update(
    "definition/pages/pages.json",
    pages_metadata
)


# ============================================================
# Save the corrected report
# ============================================================

report.close()


# ============================================================
# Verify the saved report
# ============================================================

verification_report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=True,
    show_diffs=False
)

verified_pages = (
    verification_report.list_pages()
)

verified_visuals = (
    verification_report.list_visuals()
)

print("=" * 90)
print("SEMANTIC MODEL HEALTH REPORT GENERATED")
print("=" * 90)
print(f"WORKSPACE: {WORKSPACE_NAME}")
print(f"REPORT: {REPORT_NAME}")
print(f"PAGES CREATED: {len(verified_pages)}")
print(f"VISUALS CREATED: {len(verified_visuals)}")
print("=" * 90)

print("REPORT PAGES")

display(
    verified_pages[
        [
            "Page Display Name",
            "Active",
            "Width",
            "Height",
            "Visual Count"
        ]
    ]
)

print("REPORT VISUALS")

display(
    verified_visuals[
        [
            "Page Display Name",
            "Visual Name",
            "Type",
            "X",
            "Y",
            "Width",
            "Height"
        ]
    ]
)

print("=" * 90)
print("EXPECTED RESULT")
print("KPI Overview: 10 card visuals")
print("Model Inventory: 1 table visual")
print("Total: 2 pages and 11 visuals")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric Semantic Model Health KPI card repair
#
# This script:
# 1. Creates or updates ten explicit DAX measures.
# 2. Replaces the ten empty legacy card visuals with cardVisual visuals.
# 3. Preserves the existing Model Inventory page and table.
# 4. Verifies the resulting report definition.

import hashlib

from sempy_labs.report import ReportWrapper
from sempy_labs.tom import connect_semantic_model


# ============================================================
# Configuration
# ============================================================

WORKSPACE_NAME = "Sales Records Analytics"
SEMANTIC_MODEL_NAME = "Semantic Model Health"
REPORT_NAME = "Semantic Model Health Report"

KPI_TABLE_NAME = "semantic_model_health_kpi"
KPI_PAGE_NAME = "semantic_health_kpi"

VISUAL_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/"
    "fabric/item/report/definition/"
    "visualContainer/2.13.0/schema.json"
)


# ============================================================
# KPI definitions
# ============================================================

KPI_DEFINITIONS = [
    {
        "title": "Tables",
        "measure_name": "KPI Tables",
        "kpi_name": "Tables",
        "format_string": "#,0"
    },
    {
        "title": "Fact Tables",
        "measure_name": "KPI Fact Tables",
        "kpi_name": "Fact Tables",
        "format_string": "#,0"
    },
    {
        "title": "Dimension Tables",
        "measure_name": "KPI Dimension Tables",
        "kpi_name": "Dimension Tables",
        "format_string": "#,0"
    },
    {
        "title": "Columns",
        "measure_name": "KPI Columns",
        "kpi_name": "Columns",
        "format_string": "#,0"
    },
    {
        "title": "Measures",
        "measure_name": "KPI Measures",
        "kpi_name": "Measures",
        "format_string": "#,0"
    },
    {
        "title": "Relationships",
        "measure_name": "KPI Relationships",
        "kpi_name": "Relationships",
        "format_string": "#,0"
    },
    {
        "title": "Active Relationships",
        "measure_name": "KPI Active Relationships",
        "kpi_name": "Active Relationships",
        "format_string": "#,0"
    },
    {
        "title": "Total Model Rows",
        "measure_name": "KPI Total Model Rows",
        "kpi_name": "Total Model Rows",
        "format_string": "#,0"
    },
    {
        "title": "Documentation",
        "measure_name": "KPI Documentation Completeness",
        "kpi_name": "Documentation Completeness",
        "format_string": "0.00"
    },
    {
        "title": "Measure Errors",
        "measure_name": "KPI Measure Errors",
        "kpi_name": "Measure Errors",
        "format_string": "#,0"
    }
]


# ============================================================
# Helper functions
# ============================================================

def stable_id(value):
    return hashlib.sha1(
        value.encode("utf-8")
    ).hexdigest()[:20]


def literal_string(value):
    escaped_value = str(value).replace(
        "'",
        "''"
    )

    return {
        "expr": {
            "Literal": {
                "Value": f"'{escaped_value}'"
            }
        }
    }


def literal_boolean(value):
    return {
        "expr": {
            "Literal": {
                "Value": (
                    "true"
                    if value
                    else "false"
                )
            }
        }
    }


def literal_number(value):
    return {
        "expr": {
            "Literal": {
                "Value": f"{value}D"
            }
        }
    }


def literal_color(value):
    return {
        "solid": {
            "color": {
                "expr": {
                    "Literal": {
                        "Value": f"'{value}'"
                    }
                }
            }
        }
    }


def measure_projection(
    table_name,
    measure_name
):
    return {
        "field": {
            "Measure": {
                "Expression": {
                    "SourceRef": {
                        "Entity": table_name
                    }
                },
                "Property": measure_name
            }
        },
        "queryRef": (
            f"{table_name}.{measure_name}"
        ),
        "nativeQueryRef": measure_name
    }


def build_measure_expression(kpi_name):
    escaped_kpi_name = kpi_name.replace(
        '"',
        '""'
    )

    return (
        "CALCULATE("
        f"MAX('{KPI_TABLE_NAME}'[kpi_value]), "
        f"'{KPI_TABLE_NAME}'[kpi_name] = "
        f'"{escaped_kpi_name}"'
        ")"
    )


def create_card_visual(
    visual_name,
    title,
    measure_name,
    x,
    y,
    width,
    height,
    tab_order
):
    return {
        "$schema": VISUAL_SCHEMA,
        "name": visual_name,
        "position": {
            "x": x,
            "y": y,
            "z": tab_order,
            "width": width,
            "height": height,
            "tabOrder": tab_order
        },
        "visual": {
            "visualType": "cardVisual",
            "query": {
                "queryState": {
                    "Data": {
                        "projections": [
                            measure_projection(
                                KPI_TABLE_NAME,
                                measure_name
                            )
                        ]
                    }
                }
            },
            "objects": {
                "value": [
                    {
                        "properties": {
                            "fontSize": literal_number(30),
                            "fontColor": literal_color(
                                "#16324F"
                            )
                        },
                        "selector": {
                            "id": "default"
                        }
                    }
                ],
                "label": [
                    {
                        "properties": {
                            "show": literal_boolean(False)
                        },
                        "selector": {
                            "id": "default"
                        }
                    }
                ]
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "text": literal_string(title),
                            "fontSize": literal_number(13),
                            "fontColor": literal_color(
                                "#334155"
                            ),
                            "alignment": literal_string(
                                "center"
                            )
                        }
                    }
                ],
                "background": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#FFFFFF"
                            ),
                            "transparency": literal_number(0)
                        }
                    }
                ],
                "border": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#D7DEE8"
                            ),
                            "radius": literal_number(8)
                        }
                    }
                ],
                "dropShadow": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#CBD5E1"
                            )
                        }
                    }
                ]
            }
        }
    }


# ============================================================
# Create or update explicit DAX measures
# ============================================================

with connect_semantic_model(
    dataset=SEMANTIC_MODEL_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False
) as tom:

    existing_measures = {
        measure.Name
        for measure in tom.all_measures()
        if measure.Parent.Name == KPI_TABLE_NAME
    }

    for kpi_definition in KPI_DEFINITIONS:
        measure_name = kpi_definition[
            "measure_name"
        ]

        measure_expression = (
            build_measure_expression(
                kpi_definition["kpi_name"]
            )
        )

        if measure_name in existing_measures:
            tom.update_measure(
                table_name=KPI_TABLE_NAME,
                measure_name=measure_name,
                expression=measure_expression,
                format_string=kpi_definition[
                    "format_string"
                ]
            )
        else:
            tom.add_measure(
                table_name=KPI_TABLE_NAME,
                measure_name=measure_name,
                expression=measure_expression,
                format_string=kpi_definition[
                    "format_string"
                ]
            )

print("KPI measures created or updated successfully.")


# ============================================================
# Build corrected KPI card visuals
# ============================================================

CARD_WIDTH = 228
CARD_HEIGHT = 250
HORIZONTAL_MARGIN = 20
HORIZONTAL_GAP = 25
VERTICAL_GAP = 35
TOP_MARGIN = 80

corrected_card_visuals = []

for index, kpi_definition in enumerate(
    KPI_DEFINITIONS
):
    row_number = index // 5
    column_number = index % 5

    x_position = (
        HORIZONTAL_MARGIN
        + column_number
        * (
            CARD_WIDTH
            + HORIZONTAL_GAP
        )
    )

    y_position = (
        TOP_MARGIN
        + row_number
        * (
            CARD_HEIGHT
            + VERTICAL_GAP
        )
    )

    visual_name = (
        f"kpi_{stable_id(kpi_definition['kpi_name'])}"
    )

    corrected_card_visuals.append(
        create_card_visual(
            visual_name=visual_name,
            title=kpi_definition["title"],
            measure_name=kpi_definition[
                "measure_name"
            ],
            x=x_position,
            y=y_position,
            width=CARD_WIDTH,
            height=CARD_HEIGHT,
            tab_order=1000 + index
        )
    )


# ============================================================
# Connect to the PBIR report
# ============================================================

report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False,
    show_diffs=False
)


# ============================================================
# Remove only the existing KPI card visuals
# ============================================================

existing_kpi_visual_pattern = (
    f"definition/pages/"
    f"{KPI_PAGE_NAME}/visuals/*/visual.json"
)

try:
    report.remove(
        existing_kpi_visual_pattern,
        verbose=False
    )
except Exception:
    pass


# ============================================================
# Add corrected KPI card visuals
# ============================================================

for visual_definition in corrected_card_visuals:
    visual_name = visual_definition["name"]

    visual_path = (
        f"definition/pages/"
        f"{KPI_PAGE_NAME}/visuals/"
        f"{visual_name}/visual.json"
    )

    report.add(
        visual_path,
        visual_definition
    )


# ============================================================
# Save report changes
# ============================================================

report.close()


# ============================================================
# Verify persisted report definition
# ============================================================

verification_report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=True,
    show_diffs=False
)

verified_pages = verification_report.list_pages()
verified_visuals = verification_report.list_visuals()

kpi_visual_count = len(
    verified_visuals[
        verified_visuals[
            "Page Display Name"
        ] == "KPI Overview"
    ]
)

inventory_visual_count = len(
    verified_visuals[
        verified_visuals[
            "Page Display Name"
        ] == "Model Inventory"
    ]
)

print("=" * 90)
print("KPI CARDS REPAIRED SUCCESSFULLY")
print("=" * 90)
print(f"WORKSPACE: {WORKSPACE_NAME}")
print(f"SEMANTIC MODEL: {SEMANTIC_MODEL_NAME}")
print(f"REPORT: {REPORT_NAME}")
print(f"KPI MEASURES: {len(KPI_DEFINITIONS)}")
print(f"KPI CARD VISUALS: {kpi_visual_count}")
print(
    "MODEL INVENTORY VISUALS: "
    f"{inventory_visual_count}"
)
print("=" * 90)

print("REPORT VISUALS")

display(
    verified_visuals[
        [
            "Page Display Name",
            "Visual Name",
            "Type",
            "X",
            "Y",
            "Width",
            "Height"
        ]
    ].orderBy(
        "Page Display Name",
        "Y",
        "X"
    )
    if hasattr(
        verified_visuals,
        "orderBy"
    )
    else verified_visuals[
        [
            "Page Display Name",
            "Visual Name",
            "Type",
            "X",
            "Y",
            "Width",
            "Height"
        ]
    ].sort_values(
        [
            "Page Display Name",
            "Y",
            "X"
        ]
    )
)

print("=" * 90)
print("EXPECTED KPI VALUES")
print("Tables: 8")
print("Fact Tables: 2")
print("Dimension Tables: 5")
print("Columns: 71")
print("Measures: 19")
print("Relationships: 8")
print("Active Relationships: 7")
print("Total Model Rows: 2,102,716")
print("Documentation: 0.00")
print("Measure Errors: 0")
print("=" * 90)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric compact KPI card redesign
#
# This script:
# 1. Updates KPI measure formats directly through TOM objects.
# 2. Replaces the oversized KPI cards with compact legacy cards.
# 3. Disables automatic display units for full numbers.
# 4. Preserves the Model Inventory page and its table.
#
# Target:
# Workspace: Sales Records Analytics
# Semantic model: Semantic Model Health
# Report: Semantic Model Health Report

import hashlib

from sempy_labs.report import ReportWrapper
from sempy_labs.tom import connect_semantic_model


# ============================================================
# Configuration
# ============================================================

WORKSPACE_NAME = "Sales Records Analytics"
SEMANTIC_MODEL_NAME = "Semantic Model Health"
REPORT_NAME = "Semantic Model Health Report"

KPI_TABLE_NAME = "semantic_model_health_kpi"
KPI_PAGE_NAME = "semantic_health_kpi"

VISUAL_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/"
    "fabric/item/report/definition/"
    "visualContainer/2.13.0/schema.json"
)


# ============================================================
# KPI definitions
# ============================================================

KPI_DEFINITIONS = [
    {
        "title": "Tables",
        "measure_name": "KPI Tables",
        "format_string": "#,0",
        "accent_color": "#2563EB"
    },
    {
        "title": "Fact Tables",
        "measure_name": "KPI Fact Tables",
        "format_string": "#,0",
        "accent_color": "#7C3AED"
    },
    {
        "title": "Dimension Tables",
        "measure_name": "KPI Dimension Tables",
        "format_string": "#,0",
        "accent_color": "#7C3AED"
    },
    {
        "title": "Columns",
        "measure_name": "KPI Columns",
        "format_string": "#,0",
        "accent_color": "#0891B2"
    },
    {
        "title": "Measures",
        "measure_name": "KPI Measures",
        "format_string": "#,0",
        "accent_color": "#0891B2"
    },
    {
        "title": "Relationships",
        "measure_name": "KPI Relationships",
        "format_string": "#,0",
        "accent_color": "#2563EB"
    },
    {
        "title": "Active Relationships",
        "measure_name": "KPI Active Relationships",
        "format_string": "#,0",
        "accent_color": "#16A34A"
    },
    {
        "title": "Total Model Rows",
        "measure_name": "KPI Total Model Rows",
        "format_string": "#,0",
        "accent_color": "#2563EB"
    },
    {
        "title": "Documentation",
        "measure_name": "KPI Documentation Completeness",
        "format_string": "0.00%",
        "accent_color": "#D97706"
    },
    {
        "title": "Measure Errors",
        "measure_name": "KPI Measure Errors",
        "format_string": "#,0",
        "accent_color": "#16A34A"
    }
]


# ============================================================
# PBIR helper functions
# ============================================================

def stable_id(value):
    return hashlib.sha1(
        value.encode("utf-8")
    ).hexdigest()[:20]


def literal_string(value):
    escaped_value = str(value).replace(
        "'",
        "''"
    )

    return {
        "expr": {
            "Literal": {
                "Value": f"'{escaped_value}'"
            }
        }
    }


def literal_boolean(value):
    return {
        "expr": {
            "Literal": {
                "Value": (
                    "true"
                    if value
                    else "false"
                )
            }
        }
    }


def literal_number(value):
    return {
        "expr": {
            "Literal": {
                "Value": f"{value}D"
            }
        }
    }


def literal_integer(value):
    return {
        "expr": {
            "Literal": {
                "Value": f"{value}L"
            }
        }
    }


def literal_color(value):
    return {
        "solid": {
            "color": {
                "expr": {
                    "Literal": {
                        "Value": f"'{value}'"
                    }
                }
            }
        }
    }


def measure_projection(
    table_name,
    measure_name
):
    return {
        "field": {
            "Measure": {
                "Expression": {
                    "SourceRef": {
                        "Entity": table_name
                    }
                },
                "Property": measure_name
            }
        },
        "queryRef": (
            f"{table_name}.{measure_name}"
        ),
        "nativeQueryRef": measure_name
    }


def create_compact_card(
    visual_name,
    title,
    measure_name,
    accent_color,
    x,
    y,
    width,
    height,
    tab_order
):
    return {
        "$schema": VISUAL_SCHEMA,
        "name": visual_name,
        "position": {
            "x": x,
            "y": y,
            "z": tab_order,
            "width": width,
            "height": height,
            "tabOrder": tab_order
        },
        "visual": {
            "visualType": "card",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            measure_projection(
                                KPI_TABLE_NAME,
                                measure_name
                            )
                        ]
                    }
                }
            },
            "objects": {
                "labels": [
                    {
                        "properties": {
                            "color": literal_color(
                                "#0F2942"
                            ),
                            "fontSize": literal_number(30),
                            "fontFamily": literal_string(
                                "Segoe UI Semibold"
                            ),
                            "labelDisplayUnits": literal_number(0),
                            "labelPrecision": literal_integer(2)
                        }
                    }
                ],
                "categoryLabels": [
                    {
                        "properties": {
                            "show": literal_boolean(False)
                        }
                    }
                ]
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "text": literal_string(title),
                            "fontColor": literal_color(
                                "#475569"
                            ),
                            "fontSize": literal_number(12),
                            "fontFamily": literal_string(
                                "Segoe UI Semibold"
                            ),
                            "alignment": literal_string(
                                "center"
                            )
                        }
                    }
                ],
                "background": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#FFFFFF"
                            ),
                            "transparency": literal_number(0)
                        }
                    }
                ],
                "border": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                accent_color
                            ),
                            "radius": literal_number(10)
                        }
                    }
                ],
                "dropShadow": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#CBD5E1"
                            )
                        }
                    }
                ]
            }
        }
    }


# ============================================================
# Update measure formats directly through TOM
# ============================================================

required_formats = {
    definition["measure_name"]: definition["format_string"]
    for definition in KPI_DEFINITIONS
}

updated_measure_names = []

with connect_semantic_model(
    dataset=SEMANTIC_MODEL_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False
) as tom:

    for measure in tom.all_measures():
        parent_table_name = measure.Parent.Name
        measure_name = measure.Name

        if (
            parent_table_name == KPI_TABLE_NAME
            and measure_name in required_formats
        ):
            measure.FormatString = required_formats[
                measure_name
            ]

            updated_measure_names.append(
                measure_name
            )

missing_measure_names = sorted(
    set(required_formats)
    - set(updated_measure_names)
)

if missing_measure_names:
    raise RuntimeError(
        "Required KPI measures were not found: "
        + ", ".join(missing_measure_names)
    )

print(
    "KPI measure formats updated successfully: "
    f"{len(updated_measure_names)}"
)


# ============================================================
# Create compact KPI layout
# ============================================================

CARD_WIDTH = 228
CARD_HEIGHT = 145

LEFT_MARGIN = 20
TOP_MARGIN = 100

HORIZONTAL_GAP = 25
VERTICAL_GAP = 35

compact_card_visuals = []

for index, kpi_definition in enumerate(
    KPI_DEFINITIONS
):
    row_number = index // 5
    column_number = index % 5

    x_position = (
        LEFT_MARGIN
        + column_number
        * (
            CARD_WIDTH
            + HORIZONTAL_GAP
        )
    )

    y_position = (
        TOP_MARGIN
        + row_number
        * (
            CARD_HEIGHT
            + VERTICAL_GAP
        )
    )

    compact_card_visuals.append(
        create_compact_card(
            visual_name=(
                f"kpi_{stable_id(kpi_definition['title'])}"
            ),
            title=kpi_definition["title"],
            measure_name=kpi_definition[
                "measure_name"
            ],
            accent_color=kpi_definition[
                "accent_color"
            ],
            x=x_position,
            y=y_position,
            width=CARD_WIDTH,
            height=CARD_HEIGHT,
            tab_order=1000 + index
        )
    )


# ============================================================
# Connect to the existing report
# ============================================================

report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False,
    show_diffs=False
)


# ============================================================
# Replace only KPI Overview visuals
# ============================================================

kpi_visual_pattern = (
    f"definition/pages/"
    f"{KPI_PAGE_NAME}/visuals/*/visual.json"
)

try:
    report.remove(
        kpi_visual_pattern,
        verbose=False
    )
except Exception:
    pass

for visual_definition in compact_card_visuals:
    visual_name = visual_definition["name"]

    visual_path = (
        f"definition/pages/"
        f"{KPI_PAGE_NAME}/visuals/"
        f"{visual_name}/visual.json"
    )

    report.add(
        visual_path,
        visual_definition
    )


# ============================================================
# Save report changes
# ============================================================

report.close()


# ============================================================
# Verify persisted report
# ============================================================

verification_report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=True,
    show_diffs=False
)

verified_visuals = (
    verification_report.list_visuals()
)

kpi_visuals_dataframe = verified_visuals[
    verified_visuals[
        "Page Display Name"
    ] == "KPI Overview"
]

inventory_visuals_dataframe = verified_visuals[
    verified_visuals[
        "Page Display Name"
    ] == "Model Inventory"
]

print("=" * 90)
print("KPI OVERVIEW REDESIGNED SUCCESSFULLY")
print("=" * 90)
print(f"WORKSPACE: {WORKSPACE_NAME}")
print(f"REPORT: {REPORT_NAME}")
print(
    "UPDATED KPI MEASURE FORMATS: "
    f"{len(updated_measure_names)}"
)
print(
    "COMPACT KPI CARDS: "
    f"{len(kpi_visuals_dataframe)}"
)
print(
    "MODEL INVENTORY VISUALS: "
    f"{len(inventory_visuals_dataframe)}"
)
print(f"CARD SIZE: {CARD_WIDTH} x {CARD_HEIGHT}")
print("DISPLAY UNITS: NONE")
print("TOTAL MODEL ROWS FORMAT: #,0")
print("DOCUMENTATION FORMAT: 0.00%")
print("=" * 90)

display(
    verified_visuals[
        [
            "Page Display Name",
            "Visual Name",
            "Type",
            "X",
            "Y",
            "Width",
            "Height"
        ]
    ].sort_values(
        [
            "Page Display Name",
            "Y",
            "X"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric final Semantic Model Health report refinement
#
# This script:
# 1. Displays Total Model Rows as a complete formatted value.
# 2. Preserves the compact KPI Overview layout.
# 3. Rebuilds Model Inventory with eight essential columns.
# 4. Adds four interactive slicers.
# 5. Preserves the existing semantic model and Delta tables.

import hashlib

from sempy_labs.report import ReportWrapper
from sempy_labs.tom import connect_semantic_model


# ============================================================
# Configuration
# ============================================================

WORKSPACE_NAME = "Sales Records Analytics"
SEMANTIC_MODEL_NAME = "Semantic Model Health"
REPORT_NAME = "Semantic Model Health Report"

KPI_TABLE_NAME = "semantic_model_health_kpi"
INVENTORY_TABLE_NAME = "semantic_model_inventory"

KPI_PAGE_NAME = "semantic_health_kpi"
INVENTORY_PAGE_NAME = "semantic_health_inventory"

VISUAL_SCHEMA = (
    "https://developer.microsoft.com/json-schemas/"
    "fabric/item/report/definition/"
    "visualContainer/2.13.0/schema.json"
)


# ============================================================
# KPI definitions
# ============================================================

KPI_DEFINITIONS = [
    {
        "title": "Tables",
        "measure_name": "KPI Tables",
        "accent_color": "#2563EB"
    },
    {
        "title": "Fact Tables",
        "measure_name": "KPI Fact Tables",
        "accent_color": "#7C3AED"
    },
    {
        "title": "Dimension Tables",
        "measure_name": "KPI Dimension Tables",
        "accent_color": "#7C3AED"
    },
    {
        "title": "Columns",
        "measure_name": "KPI Columns",
        "accent_color": "#0891B2"
    },
    {
        "title": "Measures",
        "measure_name": "KPI Measures",
        "accent_color": "#0891B2"
    },
    {
        "title": "Relationships",
        "measure_name": "KPI Relationships",
        "accent_color": "#2563EB"
    },
    {
        "title": "Active Relationships",
        "measure_name": "KPI Active Relationships",
        "accent_color": "#16A34A"
    },
    {
        "title": "Total Model Rows",
        "measure_name": "KPI Total Model Rows",
        "accent_color": "#2563EB"
    },
    {
        "title": "Documentation",
        "measure_name": "KPI Documentation Completeness",
        "accent_color": "#D97706"
    },
    {
        "title": "Measure Errors",
        "measure_name": "KPI Measure Errors",
        "accent_color": "#16A34A"
    }
]


# ============================================================
# Helper functions
# ============================================================

def stable_id(value):
    return hashlib.sha1(
        value.encode("utf-8")
    ).hexdigest()[:20]


def literal_string(value):
    escaped_value = str(value).replace(
        "'",
        "''"
    )

    return {
        "expr": {
            "Literal": {
                "Value": f"'{escaped_value}'"
            }
        }
    }


def literal_boolean(value):
    return {
        "expr": {
            "Literal": {
                "Value": (
                    "true"
                    if value
                    else "false"
                )
            }
        }
    }


def literal_number(value):
    return {
        "expr": {
            "Literal": {
                "Value": f"{value}D"
            }
        }
    }


def literal_color(value):
    return {
        "solid": {
            "color": {
                "expr": {
                    "Literal": {
                        "Value": f"'{value}'"
                    }
                }
            }
        }
    }


def column_expression(
    table_name,
    column_name
):
    return {
        "Column": {
            "Expression": {
                "SourceRef": {
                    "Entity": table_name
                }
            },
            "Property": column_name
        }
    }


def column_projection(
    table_name,
    column_name,
    display_name=None
):
    projection = {
        "field": column_expression(
            table_name,
            column_name
        ),
        "queryRef": (
            f"{table_name}.{column_name}"
        ),
        "nativeQueryRef": column_name
    }

    if display_name:
        projection["displayName"] = display_name

    return projection


def measure_projection(
    table_name,
    measure_name
):
    return {
        "field": {
            "Measure": {
                "Expression": {
                    "SourceRef": {
                        "Entity": table_name
                    }
                },
                "Property": measure_name
            }
        },
        "queryRef": (
            f"{table_name}.{measure_name}"
        ),
        "nativeQueryRef": measure_name
    }


# ============================================================
# Visual generation
# ============================================================

def create_compact_card(
    visual_name,
    title,
    measure_name,
    accent_color,
    x,
    y,
    width,
    height,
    tab_order
):
    return {
        "$schema": VISUAL_SCHEMA,
        "name": visual_name,
        "position": {
            "x": x,
            "y": y,
            "z": tab_order,
            "width": width,
            "height": height,
            "tabOrder": tab_order
        },
        "visual": {
            "visualType": "card",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            measure_projection(
                                KPI_TABLE_NAME,
                                measure_name
                            )
                        ]
                    }
                }
            },
            "objects": {
                "labels": [
                    {
                        "properties": {
                            "color": literal_color(
                                "#0F2942"
                            ),
                            "fontSize": literal_number(30),
                            "fontFamily": literal_string(
                                "Segoe UI Semibold"
                            ),
                            "labelDisplayUnits": literal_number(0)
                        }
                    }
                ],
                "categoryLabels": [
                    {
                        "properties": {
                            "show": literal_boolean(False)
                        }
                    }
                ]
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "text": literal_string(title),
                            "fontColor": literal_color(
                                "#475569"
                            ),
                            "fontSize": literal_number(12),
                            "fontFamily": literal_string(
                                "Segoe UI Semibold"
                            ),
                            "alignment": literal_string(
                                "center"
                            )
                        }
                    }
                ],
                "background": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#FFFFFF"
                            ),
                            "transparency": literal_number(0)
                        }
                    }
                ],
                "border": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                accent_color
                            ),
                            "radius": literal_number(10)
                        }
                    }
                ],
                "dropShadow": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#CBD5E1"
                            )
                        }
                    }
                ]
            }
        }
    }


def create_slicer(
    visual_name,
    title,
    column_name,
    x,
    y,
    width,
    height,
    tab_order
):
    return {
        "$schema": VISUAL_SCHEMA,
        "name": visual_name,
        "position": {
            "x": x,
            "y": y,
            "z": tab_order,
            "width": width,
            "height": height,
            "tabOrder": tab_order
        },
        "visual": {
            "visualType": "slicer",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": [
                            column_projection(
                                INVENTORY_TABLE_NAME,
                                column_name,
                                title
                            )
                        ]
                    }
                }
            },
            "objects": {
                "data": [
                    {
                        "properties": {
                            "mode": literal_string(
                                "Dropdown"
                            )
                        }
                    }
                ],
                "selection": [
                    {
                        "properties": {
                            "singleSelect": literal_boolean(
                                False
                            ),
                            "selectAllCheckboxEnabled": (
                                literal_boolean(True)
                            )
                        }
                    }
                ]
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "text": literal_string(title),
                            "fontSize": literal_number(11),
                            "fontColor": literal_color(
                                "#334155"
                            ),
                            "alignment": literal_string(
                                "left"
                            )
                        }
                    }
                ],
                "background": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#FFFFFF"
                            ),
                            "transparency": literal_number(0)
                        }
                    }
                ],
                "border": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#CBD5E1"
                            ),
                            "radius": literal_number(6)
                        }
                    }
                ],
                "padding": [
                    {
                        "properties": {
                            "top": literal_number(8),
                            "bottom": literal_number(8),
                            "left": literal_number(8),
                            "right": literal_number(8)
                        }
                    }
                ]
            },
            "drillFilterOtherVisuals": True
        }
    }


def create_inventory_table():
    inventory_columns = [
        (
            "object_type",
            "Object Type"
        ),
        (
            "object_name",
            "Object Name"
        ),
        (
            "parent_table",
            "Parent Table"
        ),
        (
            "table_role",
            "Table Role"
        ),
        (
            "row_count",
            "Row Count"
        ),
        (
            "data_type",
            "Data Type"
        ),
        (
            "status",
            "Status"
        ),
        (
            "possible_use",
            "Possible Use"
        )
    ]

    projections = [
        column_projection(
            INVENTORY_TABLE_NAME,
            column_name,
            display_name
        )
        for (
            column_name,
            display_name
        ) in inventory_columns
    ]

    return {
        "$schema": VISUAL_SCHEMA,
        "name": "inventory_table",
        "position": {
            "x": 20,
            "y": 125,
            "z": 1100,
            "width": 1240,
            "height": 570,
            "tabOrder": 1100
        },
        "visual": {
            "visualType": "tableEx",
            "query": {
                "queryState": {
                    "Values": {
                        "projections": projections
                    }
                }
            },
            "objects": {
                "columnHeaders": [
                    {
                        "properties": {
                            "autoSizeColumnWidth": (
                                literal_boolean(True)
                            ),
                            "fontColor": literal_color(
                                "#FFFFFF"
                            ),
                            "backColor": literal_color(
                                "#16324F"
                            ),
                            "bold": literal_boolean(True),
                            "fontSize": literal_number(10)
                        }
                    }
                ],
                "values": [
                    {
                        "properties": {
                            "fontColorPrimary": literal_color(
                                "#1F2937"
                            ),
                            "backColorPrimary": literal_color(
                                "#FFFFFF"
                            ),
                            "backColorSecondary": literal_color(
                                "#F1F5F9"
                            ),
                            "fontSize": literal_number(9)
                        }
                    }
                ],
                "grid": [
                    {
                        "properties": {
                            "gridVertical": literal_boolean(
                                False
                            ),
                            "gridHorizontal": literal_boolean(
                                True
                            ),
                            "rowPadding": literal_number(4)
                        }
                    }
                ]
            },
            "visualContainerObjects": {
                "title": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "text": literal_string(
                                "Semantic Model Inventory"
                            ),
                            "fontSize": literal_number(15),
                            "fontColor": literal_color(
                                "#16324F"
                            ),
                            "alignment": literal_string(
                                "left"
                            )
                        }
                    }
                ],
                "background": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#FFFFFF"
                            ),
                            "transparency": literal_number(0)
                        }
                    }
                ],
                "border": [
                    {
                        "properties": {
                            "show": literal_boolean(True),
                            "color": literal_color(
                                "#D7DEE8"
                            ),
                            "radius": literal_number(6)
                        }
                    }
                ]
            }
        }
    }


# ============================================================
# Update Total Model Rows measure to formatted text
# ============================================================

total_rows_measure_found = False

with connect_semantic_model(
    dataset=SEMANTIC_MODEL_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False
) as tom:

    for measure in tom.all_measures():
        if (
            measure.Parent.Name
            == KPI_TABLE_NAME
            and measure.Name
            == "KPI Total Model Rows"
        ):
            measure.Expression = (
                "FORMAT("
                "CALCULATE("
                f"MAX('{KPI_TABLE_NAME}'[kpi_value]), "
                f"'{KPI_TABLE_NAME}'[kpi_name] = "
                '"Total Model Rows"'
                "), "
                '"#,0"'
                ")"
            )

            measure.FormatString = ""

            total_rows_measure_found = True

if not total_rows_measure_found:
    raise RuntimeError(
        "Measure 'KPI Total Model Rows' was not found."
    )

print(
    "Total Model Rows measure updated successfully."
)


# ============================================================
# Rebuild compact KPI cards
# ============================================================

CARD_WIDTH = 228
CARD_HEIGHT = 145

LEFT_MARGIN = 20
TOP_MARGIN = 100

HORIZONTAL_GAP = 25
VERTICAL_GAP = 35

kpi_visuals = []

for index, kpi_definition in enumerate(
    KPI_DEFINITIONS
):
    row_number = index // 5
    column_number = index % 5

    x_position = (
        LEFT_MARGIN
        + column_number
        * (
            CARD_WIDTH
            + HORIZONTAL_GAP
        )
    )

    y_position = (
        TOP_MARGIN
        + row_number
        * (
            CARD_HEIGHT
            + VERTICAL_GAP
        )
    )

    kpi_visuals.append(
        create_compact_card(
            visual_name=(
                f"kpi_{stable_id(kpi_definition['title'])}"
            ),
            title=kpi_definition["title"],
            measure_name=kpi_definition[
                "measure_name"
            ],
            accent_color=kpi_definition[
                "accent_color"
            ],
            x=x_position,
            y=y_position,
            width=CARD_WIDTH,
            height=CARD_HEIGHT,
            tab_order=1000 + index
        )
    )


# ============================================================
# Build inventory slicers and table
# ============================================================

slicer_definitions = [
    {
        "visual_name": "filter_object_type",
        "title": "Object Type",
        "column_name": "object_type",
        "x": 20
    },
    {
        "visual_name": "filter_parent_table",
        "title": "Parent Table",
        "column_name": "parent_table",
        "x": 330
    },
    {
        "visual_name": "filter_table_role",
        "title": "Table Role",
        "column_name": "table_role",
        "x": 640
    },
    {
        "visual_name": "filter_status",
        "title": "Status",
        "column_name": "status",
        "x": 950
    }
]

inventory_visuals = []

for index, slicer_definition in enumerate(
    slicer_definitions
):
    inventory_visuals.append(
        create_slicer(
            visual_name=slicer_definition[
                "visual_name"
            ],
            title=slicer_definition["title"],
            column_name=slicer_definition[
                "column_name"
            ],
            x=slicer_definition["x"],
            y=20,
            width=290,
            height=85,
            tab_order=1000 + index
        )
    )

inventory_visuals.append(
    create_inventory_table()
)


# ============================================================
# Connect to report
# ============================================================

report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False,
    show_diffs=False
)


# ============================================================
# Replace KPI page visuals
# ============================================================

kpi_visual_pattern = (
    f"definition/pages/"
    f"{KPI_PAGE_NAME}/visuals/*/visual.json"
)

try:
    report.remove(
        kpi_visual_pattern,
        verbose=False
    )
except Exception:
    pass

for visual_definition in kpi_visuals:
    visual_name = visual_definition["name"]

    report.add(
        (
            f"definition/pages/"
            f"{KPI_PAGE_NAME}/visuals/"
            f"{visual_name}/visual.json"
        ),
        visual_definition
    )


# ============================================================
# Replace Model Inventory page visuals
# ============================================================

inventory_visual_pattern = (
    f"definition/pages/"
    f"{INVENTORY_PAGE_NAME}/visuals/*/visual.json"
)

try:
    report.remove(
        inventory_visual_pattern,
        verbose=False
    )
except Exception:
    pass

for visual_definition in inventory_visuals:
    visual_name = visual_definition["name"]

    report.add(
        (
            f"definition/pages/"
            f"{INVENTORY_PAGE_NAME}/visuals/"
            f"{visual_name}/visual.json"
        ),
        visual_definition
    )


# ============================================================
# Save report
# ============================================================

report.close()


# ============================================================
# Verify saved report
# ============================================================

verification_report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=True,
    show_diffs=False
)

verified_pages = verification_report.list_pages()
verified_visuals = verification_report.list_visuals()

kpi_page_visuals = verified_visuals[
    verified_visuals[
        "Page Display Name"
    ] == "KPI Overview"
]

inventory_page_visuals = verified_visuals[
    verified_visuals[
        "Page Display Name"
    ] == "Model Inventory"
]

print("=" * 90)
print("SEMANTIC MODEL HEALTH REPORT REFINED")
print("=" * 90)
print(f"WORKSPACE: {WORKSPACE_NAME}")
print(f"REPORT: {REPORT_NAME}")
print(f"PAGES: {len(verified_pages)}")
print(
    f"KPI OVERVIEW VISUALS: "
    f"{len(kpi_page_visuals)}"
)
print(
    f"MODEL INVENTORY VISUALS: "
    f"{len(inventory_page_visuals)}"
)
print("MODEL INVENTORY SLICERS: 4")
print("MODEL INVENTORY TABLE COLUMNS: 8")
print(
    "TOTAL MODEL ROWS EXPECTED: "
    "2,102,716"
)
print("=" * 90)

display(
    verified_visuals[
        [
            "Page Display Name",
            "Visual Name",
            "Type",
            "X",
            "Y",
            "Width",
            "Height"
        ]
    ].sort_values(
        [
            "Page Display Name",
            "Y",
            "X"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric final report styling
#
# Updates the existing, functional Semantic Model Health Report:
# - KPI Overview: solid blue, black, and red cards
# - Model Inventory: compact black and green technical style
#
# The script does not modify data, measures, tables, or visual bindings.

import hashlib

from sempy_labs.report import ReportWrapper


# ============================================================
# Configuration
# ============================================================

WORKSPACE_NAME = "Sales Records Analytics"
REPORT_NAME = "Semantic Model Health Report"

KPI_PAGE_NAME = "semantic_health_kpi"
INVENTORY_PAGE_NAME = "semantic_health_inventory"


# ============================================================
# Helper functions
# ============================================================

def stable_id(value):
    return hashlib.sha1(
        value.encode("utf-8")
    ).hexdigest()[:20]


def literal_string(value):
    escaped_value = str(value).replace(
        "'",
        "''"
    )

    return {
        "expr": {
            "Literal": {
                "Value": f"'{escaped_value}'"
            }
        }
    }


def literal_boolean(value):
    return {
        "expr": {
            "Literal": {
                "Value": (
                    "true"
                    if value
                    else "false"
                )
            }
        }
    }


def literal_number(value):
    return {
        "expr": {
            "Literal": {
                "Value": f"{value}D"
            }
        }
    }


def literal_color(value):
    return {
        "solid": {
            "color": {
                "expr": {
                    "Literal": {
                        "Value": f"'{value}'"
                    }
                }
            }
        }
    }


def get_or_create_dictionary(
    parent_dictionary,
    key
):
    if key not in parent_dictionary:
        parent_dictionary[key] = {}

    return parent_dictionary[key]


# ============================================================
# KPI color mapping
# ============================================================

KPI_COLORS = {
    f"kpi_{stable_id('Tables')}": "#1594E5",
    f"kpi_{stable_id('Fact Tables')}": "#1594E5",
    f"kpi_{stable_id('Dimension Tables')}": "#1594E5",
    f"kpi_{stable_id('Columns')}": "#1594E5",
    f"kpi_{stable_id('Measures')}": "#1594E5",

    f"kpi_{stable_id('Relationships')}": "#050505",
    f"kpi_{stable_id('Active Relationships')}": "#050505",
    f"kpi_{stable_id('Total Model Rows')}": "#050505",

    f"kpi_{stable_id('Documentation')}": "#C44D49",
    f"kpi_{stable_id('Measure Errors')}": "#C44D49"
}


# ============================================================
# Connect to the report
# ============================================================

report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False,
    show_diffs=False
)


# ============================================================
# Style KPI Overview cards
# ============================================================

report_visuals = report.list_visuals()

kpi_visual_rows = report_visuals[
    report_visuals["Page Display Name"]
    == "KPI Overview"
]

updated_kpi_visuals = 0

for _, visual_row in kpi_visual_rows.iterrows():
    visual_path = visual_row["File Path"]
    visual_name = visual_row["Visual Name"]

    if visual_name not in KPI_COLORS:
        continue

    visual_payload = report.get(
        visual_path,
        verbose=False
    )

    visual_definition = get_or_create_dictionary(
        visual_payload,
        "visual"
    )

    visual_objects = get_or_create_dictionary(
        visual_definition,
        "objects"
    )

    visual_container_objects = (
        get_or_create_dictionary(
            visual_definition,
            "visualContainerObjects"
        )
    )

    background_color = KPI_COLORS[
        visual_name
    ]

    visual_objects["labels"] = [
        {
            "properties": {
                "color": literal_color("#FFFFFF"),
                "fontSize": literal_number(29),
                "fontFamily": literal_string(
                    "Segoe UI Semibold"
                ),
                "labelDisplayUnits": literal_number(0)
            }
        }
    ]

    visual_objects["categoryLabels"] = [
        {
            "properties": {
                "show": literal_boolean(False)
            }
        }
    ]

    existing_titles = visual_container_objects.get(
        "title",
        [
            {
                "properties": {}
            }
        ]
    )

    if not existing_titles:
        existing_titles = [
            {
                "properties": {}
            }
        ]

    title_properties = get_or_create_dictionary(
        existing_titles[0],
        "properties"
    )

    title_properties.update({
        "show": literal_boolean(True),
        "fontColor": literal_color("#FFFFFF"),
        "fontSize": literal_number(12),
        "fontFamily": literal_string(
            "Segoe UI Semibold"
        ),
        "alignment": literal_string("center")
    })

    visual_container_objects["title"] = (
        existing_titles
    )

    visual_container_objects["background"] = [
        {
            "properties": {
                "show": literal_boolean(True),
                "color": literal_color(
                    background_color
                ),
                "transparency": literal_number(0)
            }
        }
    ]

    visual_container_objects["border"] = [
        {
            "properties": {
                "show": literal_boolean(False)
            }
        }
    ]

    visual_container_objects["dropShadow"] = [
        {
            "properties": {
                "show": literal_boolean(False)
            }
        }
    ]

    report.update(
        visual_path,
        visual_payload
    )

    updated_kpi_visuals += 1


# ============================================================
# Style Model Inventory visuals
# ============================================================

inventory_visual_rows = report_visuals[
    report_visuals["Page Display Name"]
    == "Model Inventory"
]

updated_inventory_visuals = 0

for _, visual_row in inventory_visual_rows.iterrows():
    visual_path = visual_row["File Path"]
    visual_type = visual_row["Type"]

    visual_payload = report.get(
        visual_path,
        verbose=False
    )

    visual_definition = get_or_create_dictionary(
        visual_payload,
        "visual"
    )

    visual_objects = get_or_create_dictionary(
        visual_definition,
        "objects"
    )

    visual_container_objects = (
        get_or_create_dictionary(
            visual_definition,
            "visualContainerObjects"
        )
    )

    if visual_type == "tableEx":
        visual_objects["columnHeaders"] = [
            {
                "properties": {
                    "autoSizeColumnWidth": (
                        literal_boolean(True)
                    ),
                    "fontColor": literal_color(
                        "#39FF14"
                    ),
                    "backColor": literal_color(
                        "#050505"
                    ),
                    "bold": literal_boolean(True),
                    "fontSize": literal_number(10),
                    "outline": literal_string(
                        "BottomOnly"
                    ),
                    "outlineColor": literal_color(
                        "#39FF14"
                    )
                }
            }
        ]

        visual_objects["values"] = [
            {
                "properties": {
                    "fontColorPrimary": (
                        literal_color("#39FF14")
                    ),
                    "fontColorSecondary": (
                        literal_color("#39FF14")
                    ),
                    "backColorPrimary": (
                        literal_color("#050505")
                    ),
                    "backColorSecondary": (
                        literal_color("#181818")
                    ),
                    "fontSize": literal_number(9),
                    "outline": literal_string(
                        "BottomOnly"
                    ),
                    "outlineColor": literal_color(
                        "#303030"
                    )
                }
            }
        ]

        visual_objects["grid"] = [
            {
                "properties": {
                    "gridVertical": (
                        literal_boolean(False)
                    ),
                    "gridHorizontal": (
                        literal_boolean(True)
                    ),
                    "rowPadding": literal_number(3),
                    "outlineColor": literal_color(
                        "#303030"
                    )
                }
            }
        ]

        existing_titles = (
            visual_container_objects.get(
                "title",
                [
                    {
                        "properties": {}
                    }
                ]
            )
        )

        if not existing_titles:
            existing_titles = [
                {
                    "properties": {}
                }
            ]

        title_properties = (
            get_or_create_dictionary(
                existing_titles[0],
                "properties"
            )
        )

        title_properties.update({
            "show": literal_boolean(True),
            "fontColor": literal_color(
                "#39FF14"
            ),
            "fontSize": literal_number(16),
            "fontFamily": literal_string(
                "Segoe UI Semibold"
            )
        })

        visual_container_objects["title"] = (
            existing_titles
        )

    elif visual_type == "slicer":
        visual_objects["header"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "fontColor": literal_color(
                        "#39FF14"
                    ),
                    "background": literal_color(
                        "#050505"
                    ),
                    "textSize": literal_number(10)
                }
            }
        ]

        visual_objects["items"] = [
            {
                "properties": {
                    "fontColor": literal_color(
                        "#39FF14"
                    ),
                    "background": literal_color(
                        "#050505"
                    ),
                    "textSize": literal_number(10)
                }
            }
        ]

    visual_container_objects["background"] = [
        {
            "properties": {
                "show": literal_boolean(True),
                "color": literal_color("#050505"),
                "transparency": literal_number(0)
            }
        }
    ]

    visual_container_objects["border"] = [
        {
            "properties": {
                "show": literal_boolean(True),
                "color": literal_color("#303030"),
                "radius": literal_number(2)
            }
        }
    ]

    visual_container_objects["dropShadow"] = [
        {
            "properties": {
                "show": literal_boolean(False)
            }
        }
    ]

    report.update(
        visual_path,
        visual_payload
    )

    updated_inventory_visuals += 1


# ============================================================
# Style Model Inventory page background
# ============================================================

inventory_page_path = (
    f"definition/pages/"
    f"{INVENTORY_PAGE_NAME}/page.json"
)

inventory_page_payload = report.get(
    inventory_page_path,
    verbose=False
)

inventory_page_payload["objects"] = {
    "background": [
        {
            "properties": {
                "color": literal_color("#050505"),
                "transparency": literal_number(0)
            }
        }
    ]
}

report.update(
    inventory_page_path,
    inventory_page_payload
)


# ============================================================
# Save report changes
# ============================================================

report.close()


# ============================================================
# Verify persisted report
# ============================================================

verification_report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=True,
    show_diffs=False
)

verified_pages = (
    verification_report.list_pages()
)

verified_visuals = (
    verification_report.list_visuals()
)

verified_kpi_visuals = verified_visuals[
    verified_visuals["Page Display Name"]
    == "KPI Overview"
]

verified_inventory_visuals = verified_visuals[
    verified_visuals["Page Display Name"]
    == "Model Inventory"
]

print("=" * 90)
print("FINAL REPORT STYLING COMPLETED")
print("=" * 90)
print(f"WORKSPACE: {WORKSPACE_NAME}")
print(f"REPORT: {REPORT_NAME}")
print(f"PAGES: {len(verified_pages)}")
print(
    f"KPI VISUALS UPDATED: "
    f"{updated_kpi_visuals}"
)
print(
    f"INVENTORY VISUALS UPDATED: "
    f"{updated_inventory_visuals}"
)
print(
    f"VERIFIED KPI VISUALS: "
    f"{len(verified_kpi_visuals)}"
)
print(
    f"VERIFIED INVENTORY VISUALS: "
    f"{len(verified_inventory_visuals)}"
)
print("=" * 90)

display(
    verified_visuals[
        [
            "Page Display Name",
            "Visual Name",
            "Type",
            "X",
            "Y",
            "Width",
            "Height"
        ]
    ].sort_values(
        [
            "Page Display Name",
            "Y",
            "X"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read-only PBIR definition inspection
# Extracts the actual definitions needed to repair the report safely.

import json

from sempy_labs.report import ReportWrapper


WORKSPACE_NAME = "Sales Records Analytics"
REPORT_NAME = "Semantic Model Health Report"

report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=True,
    show_diffs=False
)

visuals = report.list_visuals()

kpi_visual = visuals[
    (visuals["Page Display Name"] == "KPI Overview")
    & (visuals["Type"] == "card")
].iloc[0]

slicer_visual = visuals[
    (visuals["Page Display Name"] == "Model Inventory")
    & (visuals["Type"] == "slicer")
].iloc[0]

table_visual = visuals[
    (visuals["Page Display Name"] == "Model Inventory")
    & (visuals["Type"] == "tableEx")
].iloc[0]

paths_to_inspect = {
    "KPI_CARD_VISUAL_JSON": kpi_visual["File Path"],
    "INVENTORY_SLICER_VISUAL_JSON": slicer_visual["File Path"],
    "INVENTORY_TABLE_VISUAL_JSON": table_visual["File Path"],
    "KPI_PAGE_JSON": (
        "definition/pages/semantic_health_kpi/page.json"
    ),
    "INVENTORY_PAGE_JSON": (
        "definition/pages/semantic_health_inventory/page.json"
    )
}

for section_name, file_path in paths_to_inspect.items():
    payload = report.get(
        file_path,
        verbose=False
    )

    print("=" * 100)
    print(section_name)
    print(file_path)
    print("=" * 100)
    print(
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False
        )
    )

print("=" * 100)
print("PBIR DEFINITIONS EXTRACTED SUCCESSFULLY")
print("=" * 100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric Semantic Model Health report layout repair
#
# Repairs the existing report using its actual persisted PBIR definitions.
#
# KPI Overview:
# - symmetric 5 / 3 / 2 layout
# - consistent spacing and card dimensions
# - blue structure cards
# - dark relationship and volume cards
# - amber documentation warning
# - green measure error card
#
# Model Inventory:
# - removes duplicated slicer titles
# - applies a restrained dark theme
# - uses readable colors instead of neon green
# - enables text wrapping
# - disables automatic table column sizing
# - preserves all filters, fields, measures, and bindings

from copy import deepcopy

from sempy_labs.report import ReportWrapper


# ============================================================
# Configuration
# ============================================================

WORKSPACE_NAME = "Sales Records Analytics"
REPORT_NAME = "Semantic Model Health Report"

KPI_PAGE_NAME = "semantic_health_kpi"
INVENTORY_PAGE_NAME = "semantic_health_inventory"

KPI_PAGE_PATH = (
    f"definition/pages/{KPI_PAGE_NAME}/page.json"
)

INVENTORY_PAGE_PATH = (
    f"definition/pages/{INVENTORY_PAGE_NAME}/page.json"
)


# ============================================================
# PBIR literal helpers
# ============================================================

def literal_string(value):
    escaped_value = str(value).replace("'", "''")

    return {
        "expr": {
            "Literal": {
                "Value": f"'{escaped_value}'"
            }
        }
    }


def literal_boolean(value):
    return {
        "expr": {
            "Literal": {
                "Value": "true" if value else "false"
            }
        }
    }


def literal_number(value):
    return {
        "expr": {
            "Literal": {
                "Value": f"{value}D"
            }
        }
    }


def literal_color(value):
    return {
        "solid": {
            "color": {
                "expr": {
                    "Literal": {
                        "Value": f"'{value}'"
                    }
                }
            }
        }
    }


def ensure_dictionary(parent, key):
    value = parent.get(key)

    if not isinstance(value, dict):
        value = {}
        parent[key] = value

    return value


def get_literal_value(payload, default_value=""):
    try:
        return (
            payload["expr"]["Literal"]["Value"]
            .strip("'")
        )
    except Exception:
        return default_value


# ============================================================
# Connect to existing report
# ============================================================

report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False,
    show_diffs=False
)

visuals_dataframe = report.list_visuals()


# ============================================================
# KPI layout specification
# ============================================================

KPI_LAYOUT = {
    "Tables": {
        "x": 20,
        "y": 70,
        "color": "#168BD2"
    },
    "Fact Tables": {
        "x": 273,
        "y": 70,
        "color": "#168BD2"
    },
    "Dimension Tables": {
        "x": 526,
        "y": 70,
        "color": "#168BD2"
    },
    "Columns": {
        "x": 779,
        "y": 70,
        "color": "#168BD2"
    },
    "Measures": {
        "x": 1032,
        "y": 70,
        "color": "#168BD2"
    },
    "Relationships": {
        "x": 273,
        "y": 235,
        "color": "#172033"
    },
    "Active Relationships": {
        "x": 526,
        "y": 235,
        "color": "#172033"
    },
    "Total Model Rows": {
        "x": 779,
        "y": 235,
        "color": "#172033"
    },
    "Documentation": {
        "x": 399,
        "y": 400,
        "color": "#D97706"
    },
    "Measure Errors": {
        "x": 652,
        "y": 400,
        "color": "#16803A"
    }
}

CARD_WIDTH = 228
CARD_HEIGHT = 135


# ============================================================
# Repair KPI Overview
# ============================================================

kpi_visual_rows = visuals_dataframe[
    visuals_dataframe["Page Display Name"]
    == "KPI Overview"
]

updated_kpi_count = 0
detected_kpi_titles = []

for _, visual_row in kpi_visual_rows.iterrows():
    visual_path = visual_row["File Path"]

    visual_payload = report.get(
        visual_path,
        verbose=False
    )

    visual_definition = ensure_dictionary(
        visual_payload,
        "visual"
    )

    container_objects = ensure_dictionary(
        visual_definition,
        "visualContainerObjects"
    )

    title_entries = container_objects.get(
        "title",
        []
    )

    title = ""

    if title_entries:
        title_properties = title_entries[0].get(
            "properties",
            {}
        )

        title = get_literal_value(
            title_properties.get("text", {})
        )

    detected_kpi_titles.append(title)

    if title not in KPI_LAYOUT:
        continue

    layout = KPI_LAYOUT[title]

    visual_payload["position"] = {
        "x": layout["x"],
        "y": layout["y"],
        "z": visual_row["Z"]
        if "Z" in visual_row
        else 1000 + updated_kpi_count,
        "width": CARD_WIDTH,
        "height": CARD_HEIGHT,
        "tabOrder": 1000 + updated_kpi_count
    }

    visual_objects = ensure_dictionary(
        visual_definition,
        "objects"
    )

    visual_objects["labels"] = [
        {
            "properties": {
                "color": literal_color("#FFFFFF"),
                "fontSize": literal_number(28),
                "fontFamily": literal_string(
                    "Segoe UI Semibold"
                ),
                "labelDisplayUnits": literal_number(0)
            }
        }
    ]

    visual_objects["categoryLabels"] = [
        {
            "properties": {
                "show": literal_boolean(False)
            }
        }
    ]

    container_objects["title"] = [
        {
            "properties": {
                "show": literal_boolean(True),
                "text": literal_string(title),
                "fontColor": literal_color("#FFFFFF"),
                "fontSize": literal_number(12),
                "fontFamily": literal_string(
                    "Segoe UI Semibold"
                ),
                "alignment": literal_string("center")
            }
        }
    ]

    container_objects["background"] = [
        {
            "properties": {
                "show": literal_boolean(True),
                "color": literal_color(
                    layout["color"]
                ),
                "transparency": literal_number(0)
            }
        }
    ]

    container_objects["border"] = [
        {
            "properties": {
                "show": literal_boolean(False)
            }
        }
    ]

    container_objects["dropShadow"] = [
        {
            "properties": {
                "show": literal_boolean(True),
                "color": literal_color("#64748B"),
                "transparency": literal_number(80)
            }
        }
    ]

    container_objects["padding"] = [
        {
            "properties": {
                "top": literal_number(8),
                "bottom": literal_number(8),
                "left": literal_number(8),
                "right": literal_number(8)
            }
        }
    ]

    report.update(
        visual_path,
        visual_payload
    )

    updated_kpi_count += 1


# ============================================================
# Repair KPI page background
# ============================================================

kpi_page_payload = report.get(
    KPI_PAGE_PATH,
    verbose=False
)

kpi_page_payload["objects"] = {
    "background": [
        {
            "properties": {
                "color": literal_color("#F4F7FB"),
                "transparency": literal_number(0)
            }
        }
    ]
}

report.update(
    KPI_PAGE_PATH,
    kpi_page_payload
)


# ============================================================
# Repair Model Inventory
# ============================================================

inventory_visual_rows = visuals_dataframe[
    visuals_dataframe["Page Display Name"]
    == "Model Inventory"
]

updated_slicer_count = 0
updated_table_count = 0

SLICER_POSITIONS = {
    "filter_object_type": 20,
    "filter_parent_table": 330,
    "filter_table_role": 640,
    "filter_status": 950
}

for _, visual_row in inventory_visual_rows.iterrows():
    visual_path = visual_row["File Path"]
    visual_name = visual_row["Visual Name"]
    visual_type = visual_row["Type"]

    visual_payload = report.get(
        visual_path,
        verbose=False
    )

    visual_definition = ensure_dictionary(
        visual_payload,
        "visual"
    )

    visual_objects = ensure_dictionary(
        visual_definition,
        "objects"
    )

    container_objects = ensure_dictionary(
        visual_definition,
        "visualContainerObjects"
    )

    if visual_type == "slicer":
        x_position = SLICER_POSITIONS.get(
            visual_name,
            visual_payload["position"]["x"]
        )

        visual_payload["position"] = {
            "x": x_position,
            "y": 20,
            "z": visual_payload["position"].get(
                "z",
                1000 + updated_slicer_count
            ),
            "width": 290,
            "height": 72,
            "tabOrder": 1000 + updated_slicer_count
        }

        visual_objects["data"] = [
            {
                "properties": {
                    "mode": literal_string("Dropdown")
                }
            }
        ]

        visual_objects["header"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "fontColor": literal_color("#E2E8F0"),
                    "background": literal_color("#111827"),
                    "textSize": literal_number(10)
                }
            }
        ]

        visual_objects["items"] = [
            {
                "properties": {
                    "fontColor": literal_color("#E2E8F0"),
                    "background": literal_color("#111827"),
                    "textSize": literal_number(10)
                }
            }
        ]

        container_objects["title"] = [
            {
                "properties": {
                    "show": literal_boolean(False)
                }
            }
        ]

        container_objects["background"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "color": literal_color("#111827"),
                    "transparency": literal_number(0)
                }
            }
        ]

        container_objects["border"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "color": literal_color("#334155"),
                    "radius": literal_number(6)
                }
            }
        ]

        container_objects["padding"] = [
            {
                "properties": {
                    "top": literal_number(6),
                    "bottom": literal_number(6),
                    "left": literal_number(8),
                    "right": literal_number(8)
                }
            }
        ]

        container_objects["dropShadow"] = [
            {
                "properties": {
                    "show": literal_boolean(False)
                }
            }
        ]

        updated_slicer_count += 1

    elif visual_type == "tableEx":
        visual_payload["position"] = {
            "x": 20,
            "y": 110,
            "z": visual_payload["position"].get(
                "z",
                1100
            ),
            "width": 1240,
            "height": 590,
            "tabOrder": 1100
        }

        visual_objects["columnHeaders"] = [
            {
                "properties": {
                    "autoSizeColumnWidth": (
                        literal_boolean(False)
                    ),
                    "wordWrap": literal_boolean(True),
                    "fontColor": literal_color("#FFFFFF"),
                    "backColor": literal_color("#1D4ED8"),
                    "bold": literal_boolean(True),
                    "fontSize": literal_number(10),
                    "outline": literal_string(
                        "BottomOnly"
                    ),
                    "outlineColor": literal_color(
                        "#60A5FA"
                    )
                }
            }
        ]

        visual_objects["values"] = [
            {
                "properties": {
                    "fontColorPrimary": literal_color(
                        "#E5E7EB"
                    ),
                    "fontColorSecondary": literal_color(
                        "#E5E7EB"
                    ),
                    "backColorPrimary": literal_color(
                        "#111827"
                    ),
                    "backColorSecondary": literal_color(
                        "#1F2937"
                    ),
                    "fontSize": literal_number(9),
                    "wordWrap": literal_boolean(True),
                    "outline": literal_string(
                        "BottomOnly"
                    ),
                    "outlineColor": literal_color(
                        "#374151"
                    )
                }
            }
        ]

        visual_objects["grid"] = [
            {
                "properties": {
                    "gridVertical": literal_boolean(False),
                    "gridHorizontal": literal_boolean(True),
                    "rowPadding": literal_number(4),
                    "outlineColor": literal_color(
                        "#374151"
                    )
                }
            }
        ]

        container_objects["title"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "text": literal_string(
                        "Semantic Model Inventory"
                    ),
                    "fontSize": literal_number(15),
                    "fontColor": literal_color(
                        "#FFFFFF"
                    ),
                    "fontFamily": literal_string(
                        "Segoe UI Semibold"
                    ),
                    "alignment": literal_string("left")
                }
            }
        ]

        container_objects["background"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "color": literal_color("#111827"),
                    "transparency": literal_number(0)
                }
            }
        ]

        container_objects["border"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "color": literal_color("#334155"),
                    "radius": literal_number(6)
                }
            }
        ]

        container_objects["dropShadow"] = [
            {
                "properties": {
                    "show": literal_boolean(False)
                }
            }
        ]

        updated_table_count += 1

    report.update(
        visual_path,
        visual_payload
    )


# ============================================================
# Repair inventory page background
# ============================================================

inventory_page_payload = report.get(
    INVENTORY_PAGE_PATH,
    verbose=False
)

inventory_page_payload["objects"] = {
    "background": [
        {
            "properties": {
                "color": literal_color("#0B1220"),
                "transparency": literal_number(0)
            }
        }
    ]
}

report.update(
    INVENTORY_PAGE_PATH,
    inventory_page_payload
)


# ============================================================
# Validate modifications before saving
# ============================================================

expected_kpi_titles = set(KPI_LAYOUT)
detected_kpi_title_set = {
    title
    for title in detected_kpi_titles
    if title
}

missing_kpi_titles = sorted(
    expected_kpi_titles
    - detected_kpi_title_set
)

if missing_kpi_titles:
    raise RuntimeError(
        "The following KPI cards were not found: "
        + ", ".join(missing_kpi_titles)
    )

if updated_kpi_count != 10:
    raise RuntimeError(
        "Expected 10 updated KPI cards, "
        f"but updated {updated_kpi_count}."
    )

if updated_slicer_count != 4:
    raise RuntimeError(
        "Expected 4 updated slicers, "
        f"but updated {updated_slicer_count}."
    )

if updated_table_count != 1:
    raise RuntimeError(
        "Expected 1 updated inventory table, "
        f"but updated {updated_table_count}."
    )


# ============================================================
# Save changes
# ============================================================

report.close()


# ============================================================
# Verify persisted definition
# ============================================================

verification_report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=True,
    show_diffs=False
)

verified_pages = verification_report.list_pages()
verified_visuals = verification_report.list_visuals()

verified_kpi_visuals = verified_visuals[
    verified_visuals["Page Display Name"]
    == "KPI Overview"
]

verified_inventory_visuals = verified_visuals[
    verified_visuals["Page Display Name"]
    == "Model Inventory"
]

print("=" * 90)
print("REPORT LAYOUT REPAIR COMPLETED")
print("=" * 90)
print(f"WORKSPACE: {WORKSPACE_NAME}")
print(f"REPORT: {REPORT_NAME}")
print(f"PAGES: {len(verified_pages)}")
print(f"KPI CARDS UPDATED: {updated_kpi_count}")
print(f"INVENTORY SLICERS UPDATED: {updated_slicer_count}")
print(f"INVENTORY TABLES UPDATED: {updated_table_count}")
print(f"VERIFIED KPI VISUALS: {len(verified_kpi_visuals)}")
print(
    "VERIFIED INVENTORY VISUALS: "
    f"{len(verified_inventory_visuals)}"
)
print("=" * 90)

display(
    verified_visuals[
        [
            "Page Display Name",
            "Visual Name",
            "Type",
            "X",
            "Y",
            "Width",
            "Height"
        ]
    ].sort_values(
        [
            "Page Display Name",
            "Y",
            "X"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Microsoft Fabric Semantic Model Health report layout repair
#
# Repairs the existing report using its actual persisted PBIR definitions.
#
# KPI Overview:
# - symmetric 5 / 3 / 2 layout
# - consistent spacing and card dimensions
# - blue structure cards
# - dark relationship and volume cards
# - amber documentation warning
# - green measure error card
#
# Model Inventory:
# - removes duplicated slicer titles
# - applies a restrained dark theme
# - uses readable colors instead of neon green
# - enables text wrapping
# - disables automatic table column sizing
# - preserves all filters, fields, measures, and bindings

from copy import deepcopy

from sempy_labs.report import ReportWrapper


# ============================================================
# Configuration
# ============================================================

WORKSPACE_NAME = "Sales Records Analytics"
REPORT_NAME = "Semantic Model Health Report"

KPI_PAGE_NAME = "semantic_health_kpi"
INVENTORY_PAGE_NAME = "semantic_health_inventory"

KPI_PAGE_PATH = (
    f"definition/pages/{KPI_PAGE_NAME}/page.json"
)

INVENTORY_PAGE_PATH = (
    f"definition/pages/{INVENTORY_PAGE_NAME}/page.json"
)


# ============================================================
# PBIR literal helpers
# ============================================================

def literal_string(value):
    escaped_value = str(value).replace("'", "''")

    return {
        "expr": {
            "Literal": {
                "Value": f"'{escaped_value}'"
            }
        }
    }


def literal_boolean(value):
    return {
        "expr": {
            "Literal": {
                "Value": "true" if value else "false"
            }
        }
    }


def literal_number(value):
    return {
        "expr": {
            "Literal": {
                "Value": f"{value}D"
            }
        }
    }


def literal_color(value):
    return {
        "solid": {
            "color": {
                "expr": {
                    "Literal": {
                        "Value": f"'{value}'"
                    }
                }
            }
        }
    }


def ensure_dictionary(parent, key):
    value = parent.get(key)

    if not isinstance(value, dict):
        value = {}
        parent[key] = value

    return value


def get_literal_value(payload, default_value=""):
    try:
        return (
            payload["expr"]["Literal"]["Value"]
            .strip("'")
        )
    except Exception:
        return default_value


# ============================================================
# Connect to existing report
# ============================================================

report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=False,
    show_diffs=False
)

visuals_dataframe = report.list_visuals()


# ============================================================
# KPI layout specification
# ============================================================

KPI_LAYOUT = {
    "Tables": {
        "x": 20,
        "y": 70,
        "color": "#168BD2"
    },
    "Fact Tables": {
        "x": 273,
        "y": 70,
        "color": "#168BD2"
    },
    "Dimension Tables": {
        "x": 526,
        "y": 70,
        "color": "#168BD2"
    },
    "Columns": {
        "x": 779,
        "y": 70,
        "color": "#168BD2"
    },
    "Measures": {
        "x": 1032,
        "y": 70,
        "color": "#168BD2"
    },
    "Relationships": {
        "x": 273,
        "y": 235,
        "color": "#172033"
    },
    "Active Relationships": {
        "x": 526,
        "y": 235,
        "color": "#172033"
    },
    "Total Model Rows": {
        "x": 779,
        "y": 235,
        "color": "#172033"
    },
    "Documentation": {
        "x": 399,
        "y": 400,
        "color": "#D97706"
    },
    "Measure Errors": {
        "x": 652,
        "y": 400,
        "color": "#16803A"
    }
}

CARD_WIDTH = 228
CARD_HEIGHT = 135


# ============================================================
# Repair KPI Overview
# ============================================================

kpi_visual_rows = visuals_dataframe[
    visuals_dataframe["Page Display Name"]
    == "KPI Overview"
]

updated_kpi_count = 0
detected_kpi_titles = []

for _, visual_row in kpi_visual_rows.iterrows():
    visual_path = visual_row["File Path"]

    visual_payload = report.get(
        visual_path,
        verbose=False
    )

    visual_definition = ensure_dictionary(
        visual_payload,
        "visual"
    )

    container_objects = ensure_dictionary(
        visual_definition,
        "visualContainerObjects"
    )

    title_entries = container_objects.get(
        "title",
        []
    )

    title = ""

    if title_entries:
        title_properties = title_entries[0].get(
            "properties",
            {}
        )

        title = get_literal_value(
            title_properties.get("text", {})
        )

    detected_kpi_titles.append(title)

    if title not in KPI_LAYOUT:
        continue

    layout = KPI_LAYOUT[title]

    visual_payload["position"] = {
        "x": layout["x"],
        "y": layout["y"],
        "z": visual_row["Z"]
        if "Z" in visual_row
        else 1000 + updated_kpi_count,
        "width": CARD_WIDTH,
        "height": CARD_HEIGHT,
        "tabOrder": 1000 + updated_kpi_count
    }

    visual_objects = ensure_dictionary(
        visual_definition,
        "objects"
    )

    visual_objects["labels"] = [
        {
            "properties": {
                "color": literal_color("#FFFFFF"),
                "fontSize": literal_number(28),
                "fontFamily": literal_string(
                    "Segoe UI Semibold"
                ),
                "labelDisplayUnits": literal_number(0)
            }
        }
    ]

    visual_objects["categoryLabels"] = [
        {
            "properties": {
                "show": literal_boolean(False)
            }
        }
    ]

    container_objects["title"] = [
        {
            "properties": {
                "show": literal_boolean(True),
                "text": literal_string(title),
                "fontColor": literal_color("#FFFFFF"),
                "fontSize": literal_number(12),
                "fontFamily": literal_string(
                    "Segoe UI Semibold"
                ),
                "alignment": literal_string("center")
            }
        }
    ]

    container_objects["background"] = [
        {
            "properties": {
                "show": literal_boolean(True),
                "color": literal_color(
                    layout["color"]
                ),
                "transparency": literal_number(0)
            }
        }
    ]

    container_objects["border"] = [
        {
            "properties": {
                "show": literal_boolean(False)
            }
        }
    ]

    container_objects["dropShadow"] = [
        {
            "properties": {
                "show": literal_boolean(True),
                "color": literal_color("#64748B"),
                "transparency": literal_number(80)
            }
        }
    ]

    container_objects["padding"] = [
        {
            "properties": {
                "top": literal_number(8),
                "bottom": literal_number(8),
                "left": literal_number(8),
                "right": literal_number(8)
            }
        }
    ]

    report.update(
        visual_path,
        visual_payload
    )

    updated_kpi_count += 1


# ============================================================
# Repair KPI page background
# ============================================================

kpi_page_payload = report.get(
    KPI_PAGE_PATH,
    verbose=False
)

kpi_page_payload["objects"] = {
    "background": [
        {
            "properties": {
                "color": literal_color("#F4F7FB"),
                "transparency": literal_number(0)
            }
        }
    ]
}

report.update(
    KPI_PAGE_PATH,
    kpi_page_payload
)


# ============================================================
# Repair Model Inventory
# ============================================================

inventory_visual_rows = visuals_dataframe[
    visuals_dataframe["Page Display Name"]
    == "Model Inventory"
]

updated_slicer_count = 0
updated_table_count = 0

SLICER_POSITIONS = {
    "filter_object_type": 20,
    "filter_parent_table": 330,
    "filter_table_role": 640,
    "filter_status": 950
}

for _, visual_row in inventory_visual_rows.iterrows():
    visual_path = visual_row["File Path"]
    visual_name = visual_row["Visual Name"]
    visual_type = visual_row["Type"]

    visual_payload = report.get(
        visual_path,
        verbose=False
    )

    visual_definition = ensure_dictionary(
        visual_payload,
        "visual"
    )

    visual_objects = ensure_dictionary(
        visual_definition,
        "objects"
    )

    container_objects = ensure_dictionary(
        visual_definition,
        "visualContainerObjects"
    )

    if visual_type == "slicer":
        x_position = SLICER_POSITIONS.get(
            visual_name,
            visual_payload["position"]["x"]
        )

        visual_payload["position"] = {
            "x": x_position,
            "y": 20,
            "z": visual_payload["position"].get(
                "z",
                1000 + updated_slicer_count
            ),
            "width": 290,
            "height": 72,
            "tabOrder": 1000 + updated_slicer_count
        }

        visual_objects["data"] = [
            {
                "properties": {
                    "mode": literal_string("Dropdown")
                }
            }
        ]

        visual_objects["header"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "fontColor": literal_color("#E2E8F0"),
                    "background": literal_color("#111827"),
                    "textSize": literal_number(10)
                }
            }
        ]

        visual_objects["items"] = [
            {
                "properties": {
                    "fontColor": literal_color("#E2E8F0"),
                    "background": literal_color("#111827"),
                    "textSize": literal_number(10)
                }
            }
        ]

        container_objects["title"] = [
            {
                "properties": {
                    "show": literal_boolean(False)
                }
            }
        ]

        container_objects["background"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "color": literal_color("#111827"),
                    "transparency": literal_number(0)
                }
            }
        ]

        container_objects["border"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "color": literal_color("#334155"),
                    "radius": literal_number(6)
                }
            }
        ]

        container_objects["padding"] = [
            {
                "properties": {
                    "top": literal_number(6),
                    "bottom": literal_number(6),
                    "left": literal_number(8),
                    "right": literal_number(8)
                }
            }
        ]

        container_objects["dropShadow"] = [
            {
                "properties": {
                    "show": literal_boolean(False)
                }
            }
        ]

        updated_slicer_count += 1

    elif visual_type == "tableEx":
        visual_payload["position"] = {
            "x": 20,
            "y": 110,
            "z": visual_payload["position"].get(
                "z",
                1100
            ),
            "width": 1240,
            "height": 590,
            "tabOrder": 1100
        }

        visual_objects["columnHeaders"] = [
            {
                "properties": {
                    "autoSizeColumnWidth": (
                        literal_boolean(False)
                    ),
                    "wordWrap": literal_boolean(True),
                    "fontColor": literal_color("#FFFFFF"),
                    "backColor": literal_color("#1D4ED8"),
                    "bold": literal_boolean(True),
                    "fontSize": literal_number(10),
                    "outline": literal_string(
                        "BottomOnly"
                    ),
                    "outlineColor": literal_color(
                        "#60A5FA"
                    )
                }
            }
        ]

        visual_objects["values"] = [
            {
                "properties": {
                    "fontColorPrimary": literal_color(
                        "#E5E7EB"
                    ),
                    "fontColorSecondary": literal_color(
                        "#E5E7EB"
                    ),
                    "backColorPrimary": literal_color(
                        "#111827"
                    ),
                    "backColorSecondary": literal_color(
                        "#1F2937"
                    ),
                    "fontSize": literal_number(9),
                    "wordWrap": literal_boolean(True),
                    "outline": literal_string(
                        "BottomOnly"
                    ),
                    "outlineColor": literal_color(
                        "#374151"
                    )
                }
            }
        ]

        visual_objects["grid"] = [
            {
                "properties": {
                    "gridVertical": literal_boolean(False),
                    "gridHorizontal": literal_boolean(True),
                    "rowPadding": literal_number(4),
                    "outlineColor": literal_color(
                        "#374151"
                    )
                }
            }
        ]

        container_objects["title"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "text": literal_string(
                        "Semantic Model Inventory"
                    ),
                    "fontSize": literal_number(15),
                    "fontColor": literal_color(
                        "#FFFFFF"
                    ),
                    "fontFamily": literal_string(
                        "Segoe UI Semibold"
                    ),
                    "alignment": literal_string("left")
                }
            }
        ]

        container_objects["background"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "color": literal_color("#111827"),
                    "transparency": literal_number(0)
                }
            }
        ]

        container_objects["border"] = [
            {
                "properties": {
                    "show": literal_boolean(True),
                    "color": literal_color("#334155"),
                    "radius": literal_number(6)
                }
            }
        ]

        container_objects["dropShadow"] = [
            {
                "properties": {
                    "show": literal_boolean(False)
                }
            }
        ]

        updated_table_count += 1

    report.update(
        visual_path,
        visual_payload
    )


# ============================================================
# Repair inventory page background
# ============================================================

inventory_page_payload = report.get(
    INVENTORY_PAGE_PATH,
    verbose=False
)

inventory_page_payload["objects"] = {
    "background": [
        {
            "properties": {
                "color": literal_color("#0B1220"),
                "transparency": literal_number(0)
            }
        }
    ]
}

report.update(
    INVENTORY_PAGE_PATH,
    inventory_page_payload
)


# ============================================================
# Validate modifications before saving
# ============================================================

expected_kpi_titles = set(KPI_LAYOUT)
detected_kpi_title_set = {
    title
    for title in detected_kpi_titles
    if title
}

missing_kpi_titles = sorted(
    expected_kpi_titles
    - detected_kpi_title_set
)

if missing_kpi_titles:
    raise RuntimeError(
        "The following KPI cards were not found: "
        + ", ".join(missing_kpi_titles)
    )

if updated_kpi_count != 10:
    raise RuntimeError(
        "Expected 10 updated KPI cards, "
        f"but updated {updated_kpi_count}."
    )

if updated_slicer_count != 4:
    raise RuntimeError(
        "Expected 4 updated slicers, "
        f"but updated {updated_slicer_count}."
    )

if updated_table_count != 1:
    raise RuntimeError(
        "Expected 1 updated inventory table, "
        f"but updated {updated_table_count}."
    )


# ============================================================
# Save changes
# ============================================================

report.close()


# ============================================================
# Verify persisted definition
# ============================================================

verification_report = ReportWrapper(
    report=REPORT_NAME,
    workspace=WORKSPACE_NAME,
    readonly=True,
    show_diffs=False
)

verified_pages = verification_report.list_pages()
verified_visuals = verification_report.list_visuals()

verified_kpi_visuals = verified_visuals[
    verified_visuals["Page Display Name"]
    == "KPI Overview"
]

verified_inventory_visuals = verified_visuals[
    verified_visuals["Page Display Name"]
    == "Model Inventory"
]

print("=" * 90)
print("REPORT LAYOUT REPAIR COMPLETED")
print("=" * 90)
print(f"WORKSPACE: {WORKSPACE_NAME}")
print(f"REPORT: {REPORT_NAME}")
print(f"PAGES: {len(verified_pages)}")
print(f"KPI CARDS UPDATED: {updated_kpi_count}")
print(f"INVENTORY SLICERS UPDATED: {updated_slicer_count}")
print(f"INVENTORY TABLES UPDATED: {updated_table_count}")
print(f"VERIFIED KPI VISUALS: {len(verified_kpi_visuals)}")
print(
    "VERIFIED INVENTORY VISUALS: "
    f"{len(verified_inventory_visuals)}"
)
print("=" * 90)

display(
    verified_visuals[
        [
            "Page Display Name",
            "Visual Name",
            "Type",
            "X",
            "Y",
            "Width",
            "Height"
        ]
    ].sort_values(
        [
            "Page Display Name",
            "Y",
            "X"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
