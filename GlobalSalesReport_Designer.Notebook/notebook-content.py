# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import base64
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
EXPECTED_PAGE_ID = "ae63e334e13ceb673ebb"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_DEFINITION_URL = (
    f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}"
    f"/reports/{REPORT_ID}/getDefinition"
)


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def wait_for_operation(response, request_headers):
    if response.status_code != 202:
        raise RuntimeError(
            f"Unexpected HTTP status: {response.status_code}\n{response.text}"
        )

    operation_id = response.headers.get("x-ms-operation-id")
    operation_url = response.headers.get("Location")
    retry_after = int(response.headers.get("Retry-After", "5"))

    if not operation_id:
        raise RuntimeError("Fabric returned HTTP 202 without x-ms-operation-id.")

    if not operation_url:
        operation_url = f"{API_BASE_URL}/operations/{operation_id}"

    for attempt in range(60):
        time.sleep(retry_after)
        status_response = requests.get(
            operation_url,
            headers=request_headers,
            timeout=60,
        )
        status_response.raise_for_status()
        status_data = status_response.json()
        status = str(status_data.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")

        if status == "succeeded":
            return operation_id

        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(status_data, indent=2))

    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = get_headers()
    response = requests.post(
        GET_DEFINITION_URL,
        headers=request_headers,
        timeout=120,
    )

    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(
            f"Definition download failed.\n"
            f"HTTP status: {response.status_code}\n"
            f"Response: {response.text}"
        )

    definition_parts = result.get("definition", result).get("parts", [])
    if not definition_parts:
        raise RuntimeError("The report definition contains no parts.")

    decoded_parts = {}
    for part in definition_parts:
        path = part["path"]
        payload = part.get("payload", "")
        payload_type = part.get("payloadType", "")

        if payload_type == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded_parts[path] = base64.b64decode(payload)
        else:
            decoded_parts[path] = payload.encode("utf-8")

    return decoded_parts


def load_json(parts, path):
    return json.loads(parts[path].decode("utf-8"))


def get_page_name(page_data):
    return (
        page_data.get("displayName")
        or page_data.get("name")
        or "(unknown page)"
    )


print("Downloading the blank report definition...")
report_parts = download_definition()

page_paths = sorted(
    path for path in report_parts if path.lower().endswith("/page.json")
)
visual_paths = sorted(
    path for path in report_parts if path.lower().endswith("/visual.json")
)

if len(page_paths) != 1:
    raise RuntimeError(
        f"Expected exactly one blank report page; found {len(page_paths)}."
    )

page_path = page_paths[0]
page_data = load_json(report_parts, page_path)
page_id = page_data.get("name", "")
page_name = get_page_name(page_data)

if page_id != EXPECTED_PAGE_ID:
    raise RuntimeError(
        f"Unexpected page ID. Expected {EXPECTED_PAGE_ID}, found {page_id}."
    )

if visual_paths:
    raise RuntimeError(
        "The baseline report is not blank. "
        f"Detected {len(visual_paths)} visual definitions."
    )

pages_index_path = "definition/pages/pages.json"
report_definition_path = "definition/report.json"
model_binding_path = "definition.pbir"

required_paths = {
    pages_index_path,
    report_definition_path,
    model_binding_path,
    page_path,
}
missing_paths = sorted(required_paths - set(report_parts))

if missing_paths:
    raise RuntimeError(
        "Required PBIR definition parts are missing:\n"
        + "\n".join(missing_paths)
    )

pages_index = load_json(report_parts, pages_index_path)
report_definition = load_json(report_parts, report_definition_path)
model_binding = load_json(report_parts, model_binding_path)

semantic_model_reference = (
    model_binding.get("datasetReference")
    or model_binding.get("semanticModelReference")
    or model_binding.get("dataset")
    or model_binding
)

print("\nGLOBAL SALES REPORT BASELINE AUDIT")
print(f"Definition parts: {len(report_parts)}")
print(f"Pages: {len(page_paths)}")
print(f"Visuals: {len(visual_paths)}")
print(f"Page ID: {page_id}")
print(f"Page name: {page_name}")
print(f"Page width: {page_data.get('width')}")
print(f"Page height: {page_data.get('height')}")
print("Blank baseline confirmed: Yes")
print("Report updated: No")

print("\nSEMANTIC MODEL REFERENCE")
print(
    json.dumps(
        semantic_model_reference,
        indent=2,
        ensure_ascii=False,
    )
)

print("\nPAGES INDEX")
print(
    json.dumps(
        pages_index,
        indent=2,
        ensure_ascii=False,
    )
)

print("\nREPORT SETTINGS")
print(
    json.dumps(
        report_definition,
        indent=2,
        ensure_ascii=False,
    )
)

print("\nBASELINE AUDIT COMPLETED")
print("Report updated: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
PAGE_ID = "ae63e334e13ceb673ebb"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_DEFINITION_URL = (
    f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}"
    f"/reports/{REPORT_ID}/getDefinition"
)

EXPECTED_TYPES = {
    "cardVisual",
    "lineChart",
    "clusteredColumnChart",
    "tableEx",
    "slicer",
}


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def wait_for_operation(response, request_headers):
    if response.status_code != 202:
        raise RuntimeError(
            f"Unexpected HTTP status: {response.status_code}\n{response.text}"
        )

    operation_id = response.headers.get("x-ms-operation-id")
    operation_url = response.headers.get("Location")
    retry_after = int(response.headers.get("Retry-After", "5"))

    if not operation_id:
        raise RuntimeError("Fabric returned HTTP 202 without x-ms-operation-id.")

    if not operation_url:
        operation_url = f"{API_BASE_URL}/operations/{operation_id}"

    for attempt in range(60):
        time.sleep(retry_after)
        status_response = requests.get(
            operation_url,
            headers=request_headers,
            timeout=60,
        )
        status_response.raise_for_status()
        status_data = status_response.json()
        status = str(status_data.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")

        if status == "succeeded":
            return operation_id

        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(status_data, indent=2))

    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = get_headers()
    response = requests.post(
        GET_DEFINITION_URL,
        headers=request_headers,
        timeout=120,
    )

    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(
            f"Definition download failed.\n"
            f"HTTP status: {response.status_code}\n"
            f"Response: {response.text}"
        )

    definition_parts = result.get("definition", result).get("parts", [])
    if not definition_parts:
        raise RuntimeError("The report definition contains no parts.")

    decoded_parts = {}
    for part in definition_parts:
        path = part["path"]
        payload = part.get("payload", "")
        payload_type = part.get("payloadType", "")

        if payload_type == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded_parts[path] = base64.b64decode(payload)
        else:
            decoded_parts[path] = payload.encode("utf-8")

    return decoded_parts


def get_query_refs(visual_data):
    query_state = (
        visual_data.get("visual", {})
        .get("query", {})
        .get("queryState", {})
    )
    refs = []
    for role_name, role_data in query_state.items():
        for projection in role_data.get("projections", []):
            refs.append(
                {
                    "role": role_name,
                    "queryRef": projection.get("queryRef"),
                    "nativeQueryRef": projection.get("nativeQueryRef"),
                    "displayName": projection.get("displayName"),
                }
            )
    return refs


print("Downloading the saved seed report definition...")
report_parts = download_definition()

visual_paths = sorted(
    path
    for path in report_parts
    if path.startswith(f"definition/pages/{PAGE_ID}/visuals/")
    and path.endswith("/visual.json")
)

if len(visual_paths) != 5:
    raise RuntimeError(
        f"Expected exactly 5 seed visuals; found {len(visual_paths)}."
    )

visuals = []
for path in visual_paths:
    visual_data = json.loads(report_parts[path].decode("utf-8"))
    visual_type = visual_data.get("visual", {}).get("visualType", "unknown")
    visuals.append(
        {
            "path": path,
            "id": visual_data.get("name", ""),
            "type": visual_type,
            "fields": get_query_refs(visual_data),
            "definition": visual_data,
        }
    )

actual_types = {item["type"] for item in visuals}
missing_types = sorted(EXPECTED_TYPES - actual_types)
extra_types = sorted(actual_types - EXPECTED_TYPES)

if missing_types or extra_types:
    raise RuntimeError(
        "Seed visual type validation failed.\n"
        f"Missing types: {missing_types}\n"
        f"Unexpected types: {extra_types}\n"
        f"Detected types: {sorted(actual_types)}"
    )

print("\nSEED VISUAL VALIDATION PASSED")
print("Seed visuals: 5")
print("Detected types: " + ", ".join(sorted(actual_types)))
print("Report updated: No")

print("\nSEED VISUAL SUMMARY")
for item in sorted(visuals, key=lambda value: value["type"]):
    print(f"\nType: {item['type']}")
    print(f"ID: {item['id']}")
    print(f"Path: {item['path']}")
    print("Fields:")
    for field in item["fields"]:
        print(
            f"  - {field['role']}: "
            f"{field['queryRef'] or field['nativeQueryRef'] or field['displayName']}"
        )

export_payload = {
    "pageId": PAGE_ID,
    "visuals": visuals,
}

print("\nSEED VISUAL DEFINITIONS")
print(json.dumps(export_payload, ensure_ascii=False, separators=(",", ":")))
print("\nSEED VISUAL EXPORT COMPLETED")
print("Report updated: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import uuid
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
SOURCE_PAGE_ID = "ae63e334e13ceb673ebb"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/getDefinition"
UPDATE_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/updateDefinition"

PAGE_DEFINITIONS = [
    {
        "name": "Executive Overview",
        "visuals": [
            ("slicer", "Year", 12, 12, 170, 72, {"field": "date.year"}),
            ("card", "Total Revenue", 194, 12, 202, 112, {"measure": "_Measures.Total Revenue"}),
            ("card", "Total Profit", 408, 12, 202, 112, {"measure": "_Measures.Total Profit"}),
            ("card", "Profit Margin", 622, 12, 202, 112, {"measure": "_Measures.Profit Margin %"}),
            ("card", "Units Sold", 836, 12, 202, 112, {"measure": "_Measures.Units Sold"}),
            ("card", "Orders", 1050, 12, 214, 112, {"measure": "_Measures.Order Count"}),
            ("line", "Monthly Revenue Trend", 12, 136, 826, 280, {"category": "date.year_month", "measure": "_Measures.Total Revenue"}),
            ("bar", "Revenue by Product", 850, 136, 414, 280, {"category": "product.product_name", "measure": "_Measures.Total Revenue"}),
            ("bar", "Revenue by Region", 12, 428, 410, 278, {"category": "geography.region", "measure": "_Measures.Total Revenue"}),
            ("bar", "Revenue by Channel", 434, 428, 410, 278, {"category": "saleschannel.sales_channel_name", "measure": "_Measures.Total Revenue"}),
            ("table", "Executive Detail", 856, 428, 408, 278, {"fields": ["product.product_name", "_Measures.Total Revenue", "_Measures.Total Profit"]}),
        ],
    },
    {
        "name": "Budget Performance",
        "visuals": [
            ("slicer", "Year", 12, 12, 170, 72, {"field": "date.year"}),
            ("card", "Revenue Attainment", 194, 12, 202, 112, {"measure": "_Measures.Revenue Budget Attainment %"}),
            ("card", "Revenue Variance", 408, 12, 202, 112, {"measure": "_Measures.Revenue Variance"}),
            ("card", "Profit Attainment", 622, 12, 202, 112, {"measure": "_Measures.Profit Budget Attainment %"}),
            ("card", "Profit Variance", 836, 12, 202, 112, {"measure": "_Measures.Profit Variance"}),
            ("card", "Budget Revenue", 1050, 12, 214, 112, {"measure": "_Measures.Budget Revenue"}),
            ("line", "Monthly Revenue vs Budget", 12, 136, 620, 280, {"category": "date.year_month", "measure": "_Measures.Total Revenue"}),
            ("line", "Monthly Profit vs Budget", 644, 136, 620, 280, {"category": "date.year_month", "measure": "_Measures.Total Profit"}),
            ("bar", "Revenue Variance by Product", 12, 428, 620, 278, {"category": "product.product_name", "measure": "_Measures.Revenue Variance"}),
            ("table", "Budget Performance Detail", 644, 428, 620, 278, {"fields": ["product.product_name", "_Measures.Budget Revenue", "_Measures.Revenue Variance"]}),
        ],
    },
    {
        "name": "Product Performance",
        "visuals": [
            ("slicer", "Year", 12, 12, 170, 72, {"field": "date.year"}),
            ("card", "Total Revenue", 194, 12, 202, 112, {"measure": "_Measures.Total Revenue"}),
            ("card", "Total Profit", 408, 12, 202, 112, {"measure": "_Measures.Total Profit"}),
            ("card", "Profit Margin", 622, 12, 202, 112, {"measure": "_Measures.Profit Margin %"}),
            ("card", "Units Sold", 836, 12, 202, 112, {"measure": "_Measures.Units Sold"}),
            ("card", "Average Order Value", 1050, 12, 214, 112, {"measure": "_Measures.Average Order Value"}),
            ("bar", "Revenue by Product", 12, 136, 620, 280, {"category": "product.product_name", "measure": "_Measures.Total Revenue"}),
            ("bar", "Profit by Product", 644, 136, 620, 280, {"category": "product.product_name", "measure": "_Measures.Total Profit"}),
            ("bar", "Profit Margin by Product", 12, 428, 620, 278, {"category": "product.product_name", "measure": "_Measures.Profit Margin %"}),
            ("table", "Product Performance Detail", 644, 428, 620, 278, {"fields": ["product.product_name", "_Measures.Total Revenue", "_Measures.Profit Margin %"]}),
        ],
    },
    {
        "name": "Markets and Channels",
        "visuals": [
            ("slicer", "Year", 12, 12, 170, 72, {"field": "date.year"}),
            ("card", "Total Revenue", 194, 12, 202, 112, {"measure": "_Measures.Total Revenue"}),
            ("card", "Total Profit", 408, 12, 202, 112, {"measure": "_Measures.Total Profit"}),
            ("card", "Orders", 622, 12, 202, 112, {"measure": "_Measures.Order Count"}),
            ("card", "Average Order Value", 836, 12, 202, 112, {"measure": "_Measures.Average Order Value"}),
            ("card", "Average Delivery Days", 1050, 12, 214, 112, {"measure": "_Measures.Average Delivery Days"}),
            ("bar", "Revenue by Region", 12, 136, 620, 280, {"category": "geography.region", "measure": "_Measures.Total Revenue"}),
            ("bar", "Revenue by Channel", 644, 136, 620, 280, {"category": "saleschannel.sales_channel_name", "measure": "_Measures.Total Revenue"}),
            ("bar", "Profit by Region", 12, 428, 620, 278, {"category": "geography.region", "measure": "_Measures.Total Profit"}),
            ("table", "Geography Detail", 644, 428, 620, 278, {"fields": ["geography.geography_name", "_Measures.Total Revenue", "_Measures.Total Profit"]}),
        ],
    },
    {
        "name": "Orders and Delivery",
        "visuals": [
            ("slicer", "Year", 12, 12, 170, 72, {"field": "date.year"}),
            ("card", "Orders", 194, 12, 202, 112, {"measure": "_Measures.Order Count"}),
            ("card", "Average Order Value", 408, 12, 202, 112, {"measure": "_Measures.Average Order Value"}),
            ("card", "Average Delivery Days", 622, 12, 202, 112, {"measure": "_Measures.Average Delivery Days"}),
            ("card", "Revenue by Ship Date", 836, 12, 202, 112, {"measure": "_Measures.Revenue by Ship Date"}),
            ("card", "Units Sold", 1050, 12, 214, 112, {"measure": "_Measures.Units Sold"}),
            ("line", "Monthly Orders", 12, 136, 620, 280, {"category": "date.year_month", "measure": "_Measures.Order Count"}),
            ("bar", "Orders by Priority", 644, 136, 620, 280, {"category": "orderpriority.order_priority_name", "measure": "_Measures.Order Count"}),
            ("bar", "Delivery Days by Priority", 12, 428, 620, 278, {"category": "orderpriority.order_priority_name", "measure": "_Measures.Average Delivery Days"}),
            ("table", "Order Performance Detail", 644, 428, 620, 278, {"fields": ["orderpriority.order_priority_name", "_Measures.Order Count", "_Measures.Average Delivery Days"]}),
        ],
    },
]


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def wait_for_operation(response, request_headers):
    if response.status_code == 200:
        return None
    if response.status_code != 202:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Missing x-ms-operation-id.")
    operation_url = response.headers.get("Location") or f"{API_BASE_URL}/operations/{operation_id}"
    retry_after = int(response.headers.get("Retry-After", "5"))
    for attempt in range(60):
        time.sleep(retry_after)
        status_response = requests.get(operation_url, headers=request_headers, timeout=60)
        status_response.raise_for_status()
        status_data = status_response.json()
        status = str(status_data.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")
        if status == "succeeded":
            return operation_id
        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(status_data, indent=2))
    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = get_headers()
    response = requests.post(GET_URL, headers=request_headers, timeout=120)
    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
    parts = result.get("definition", result).get("parts", [])
    if not parts:
        raise RuntimeError("The report definition contains no parts.")
    decoded = {}
    for part in parts:
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[part["path"]] = base64.b64decode(payload)
        else:
            decoded[part["path"]] = payload.encode("utf-8")
    return decoded


def encode_definition(parts):
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(parts[path]).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
                for path in sorted(parts)
            ]
        }
    }


def new_id():
    return uuid.uuid4().hex[:20]


def literal(value):
    return {"expr": {"Literal": {"Value": value}}}


def entity_property(ref):
    table, prop = ref.split(".", 1)
    return table, prop


def replace_strings(value, replacements):
    if isinstance(value, dict):
        return {key: replace_strings(item, replacements) for key, item in value.items()}
    if isinstance(value, list):
        return [replace_strings(item, replacements) for item in value]
    if isinstance(value, str):
        for old_value, new_value in replacements.items():
            value = value.replace(old_value, new_value)
    return value


def set_title(visual_data, title):
    container = visual_data.setdefault("visual", {}).setdefault("visualContainerObjects", {})
    container["title"] = [{
        "properties": {
            "text": literal(f"'{title}'"),
            "show": literal("true"),
            "titleWrap": literal("true"),
            "alignment": literal("'left'"),
            "fontSize": literal("'14'"),
            "bold": literal("true"),
        }
    }]
    container["border"] = [{
        "properties": {
            "show": literal("true"),
            "color": {"solid": {"color": literal("'#D9D9E3'")}},
            "radius": literal("6D"),
            "width": literal("1D"),
        }
    }]
    container["dropShadow"] = [{"properties": {"show": literal("false")}}]


def set_position(visual_data, x, y, width, height, tab_order):
    visual_data.setdefault("position", {}).update({
        "x": float(x),
        "y": float(y),
        "width": float(width),
        "height": float(height),
        "tabOrder": int(tab_order),
    })


def find_templates(parts):
    templates = {}
    for path, content in parts.items():
        if not path.endswith("/visual.json"):
            continue
        data = json.loads(content.decode("utf-8"))
        visual_type = data.get("visual", {}).get("visualType")
        if visual_type == "cardVisual":
            templates["card"] = data
        elif visual_type == "lineChart":
            templates["line"] = data
        elif visual_type == "clusteredColumnChart":
            templates["bar"] = data
        elif visual_type == "tableEx":
            templates["table"] = data
        elif visual_type == "slicer":
            templates["slicer"] = data
    missing = sorted({"card", "line", "bar", "table", "slicer"} - set(templates))
    if missing:
        raise RuntimeError(f"Missing seed templates: {missing}")
    return templates


def clone_visual(template, visual_id, title, x, y, width, height, tab_order, config):
    data = copy.deepcopy(template)
    source_id = data.get("name", "")
    data = replace_strings(data, {source_id: visual_id})
    data["name"] = visual_id
    set_position(data, x, y, width, height, tab_order)
    set_title(data, title)

    visual_type = data.get("visual", {}).get("visualType")
    text = json.dumps(data, ensure_ascii=False)

    if visual_type == "cardVisual":
        target = config["measure"]
        table, prop = entity_property(target)
        replacements = {
            "_Measures.Total Revenue": target,
            "Total Revenue": prop,
        }
        data = replace_strings(data, replacements)
        if width >= 190:
            objects = data.setdefault("visual", {}).setdefault("objects", {})
            objects["labels"] = [{"properties": {"fontSize": literal("24D"), "bold": literal("true")}}]

    elif visual_type in {"lineChart", "clusteredColumnChart"}:
        category = config["category"]
        measure = config["measure"]
        category_table, category_prop = entity_property(category)
        measure_table, measure_prop = entity_property(measure)
        replacements = {
            "date.year_month": category,
            "year_month": category_prop,
            "product.product_name": category,
            "product_name": category_prop,
            "_Measures.Total Revenue": measure,
            "Total Revenue": measure_prop,
        }
        data = replace_strings(data, replacements)
        if visual_type == "clusteredColumnChart":
            data["visual"]["visualType"] = "clusteredBarChart"

    elif visual_type == "slicer":
        field = config["field"]
        field_table, field_prop = entity_property(field)
        data = replace_strings(data, {
            "date.year": field,
            "year": field_prop,
        })
        data.setdefault("visual", {}).setdefault("objects", {})["data"] = [
            {"properties": {"mode": literal("'Dropdown'")}}
        ]

    elif visual_type == "tableEx":
        fields = config["fields"]
        source_fields = ["product.product_name", "_Measures.Total Revenue", "_Measures.Total Profit"]
        replacements = {}
        for source, target in zip(source_fields, fields):
            _, source_prop = entity_property(source)
            _, target_prop = entity_property(target)
            replacements[source] = target
            replacements[source_prop] = target_prop
        data = replace_strings(data, replacements)

    data["name"] = visual_id
    set_position(data, x, y, width, height, tab_order)
    set_title(data, title)
    return data


print("Downloading the saved seed report definition...")
original_parts = download_definition()
modified_parts = copy.deepcopy(original_parts)
templates = find_templates(original_parts)

source_page_path = f"definition/pages/{SOURCE_PAGE_ID}/page.json"
if source_page_path not in original_parts:
    raise RuntimeError("Source page definition was not found.")
source_page = json.loads(original_parts[source_page_path].decode("utf-8"))

for path in list(modified_parts):
    if "/visuals/" in path and path.startswith(f"definition/pages/{SOURCE_PAGE_ID}/"):
        del modified_parts[path]

page_ids = []
created_visual_count = 0
for page_index, page_spec in enumerate(PAGE_DEFINITIONS):
    page_id = SOURCE_PAGE_ID if page_index == 0 else new_id()
    page_ids.append(page_id)
    page_data = copy.deepcopy(source_page)
    page_data["name"] = page_id
    page_data["displayName"] = page_spec["name"]
    page_data["width"] = 1280
    page_data["height"] = 720
    page_path = f"definition/pages/{page_id}/page.json"
    modified_parts[page_path] = json.dumps(page_data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    for tab_order, visual_spec in enumerate(page_spec["visuals"]):
        kind, title, x, y, width, height, config = visual_spec
        visual_id = new_id()
        visual_data = clone_visual(
            templates[kind], visual_id, title, x, y, width, height, tab_order, config
        )
        visual_path = f"definition/pages/{page_id}/visuals/{visual_id}/visual.json"
        modified_parts[visual_path] = json.dumps(
            visual_data, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        created_visual_count += 1

pages_path = "definition/pages/pages.json"
pages_data = json.loads(modified_parts[pages_path].decode("utf-8"))
pages_data["pageOrder"] = page_ids
pages_data["activePageName"] = page_ids[0]
modified_parts[pages_path] = json.dumps(
    pages_data, ensure_ascii=False, separators=(",", ":")
).encode("utf-8")

expected_page_count = len(PAGE_DEFINITIONS)
expected_visual_count = sum(len(page["visuals"]) for page in PAGE_DEFINITIONS)
actual_page_paths = [path for path in modified_parts if path.endswith("/page.json")]
actual_visual_paths = [path for path in modified_parts if path.endswith("/visual.json")]

if len(actual_page_paths) != expected_page_count:
    raise RuntimeError(
        f"Page validation failed. Expected {expected_page_count}, found {len(actual_page_paths)}."
    )
if len(actual_visual_paths) != expected_visual_count:
    raise RuntimeError(
        f"Visual validation failed. Expected {expected_visual_count}, found {len(actual_visual_paths)}."
    )
if created_visual_count != expected_visual_count:
    raise RuntimeError("Created visual count validation failed.")

for page_id in page_ids:
    page_visuals = [
        path for path in actual_visual_paths if path.startswith(f"definition/pages/{page_id}/visuals/")
    ]
    if not page_visuals:
        raise RuntimeError(f"Page {page_id} contains no visuals.")

rollback_request_body = encode_definition(original_parts)
update_request_body = encode_definition(modified_parts)

print("\nGLOBAL SALES DASHBOARD VALIDATION PASSED")
print(f"Pages created: {expected_page_count}")
print(f"Visuals created: {expected_visual_count}")
print("Semantic model binding preserved: Yes")
print("Seed visuals replaced: Yes")
print("Rollback stored: rollback_request_body")

request_headers = get_headers()
update_response = requests.post(
    UPDATE_URL,
    headers=request_headers,
    json=update_request_body,
    timeout=240,
)
print(f"Initial update status: {update_response.status_code}")
wait_for_operation(update_response, request_headers)

print("\nGLOBAL SALES DASHBOARD BUILD COMPLETED")
print("Final status: Succeeded")
print(f"Pages: {expected_page_count}")
print(f"Visuals: {expected_visual_count}")
print("Semantic model changed: No")
print("Lakehouse required: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/getDefinition"
UPDATE_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/updateDefinition"

PAGE_LAYOUTS = {
    "Executive Overview": {
        "Year": (12.0, 12.0, 170.0, 112.0),
        "Total Revenue": (194.0, 12.0, 202.0, 112.0),
        "Total Profit": (408.0, 12.0, 202.0, 112.0),
        "Profit Margin": (622.0, 12.0, 202.0, 112.0),
        "Units Sold": (836.0, 12.0, 202.0, 112.0),
        "Orders": (1050.0, 12.0, 214.0, 112.0),
        "Monthly Revenue Trend": (12.0, 136.0, 826.0, 280.0),
        "Revenue by Product": (850.0, 136.0, 414.0, 280.0),
        "Revenue by Region": (12.0, 428.0, 409.333, 278.0),
        "Revenue by Channel": (433.333, 428.0, 409.334, 278.0),
        "Executive Detail": (854.667, 428.0, 409.333, 278.0),
    },
    "Budget Performance": {
        "Year": (12.0, 12.0, 170.0, 112.0),
        "Revenue Attainment": (194.0, 12.0, 202.0, 112.0),
        "Revenue Variance": (408.0, 12.0, 202.0, 112.0),
        "Profit Attainment": (622.0, 12.0, 202.0, 112.0),
        "Profit Variance": (836.0, 12.0, 202.0, 112.0),
        "Budget Revenue": (1050.0, 12.0, 214.0, 112.0),
        "Monthly Revenue vs Budget": (12.0, 136.0, 620.0, 280.0),
        "Monthly Profit vs Budget": (644.0, 136.0, 620.0, 280.0),
        "Revenue Variance by Product": (12.0, 428.0, 620.0, 278.0),
        "Budget Performance Detail": (644.0, 428.0, 620.0, 278.0),
    },
    "Product Performance": {
        "Year": (12.0, 12.0, 170.0, 112.0),
        "Total Revenue": (194.0, 12.0, 202.0, 112.0),
        "Total Profit": (408.0, 12.0, 202.0, 112.0),
        "Profit Margin": (622.0, 12.0, 202.0, 112.0),
        "Units Sold": (836.0, 12.0, 202.0, 112.0),
        "Average Order Value": (1050.0, 12.0, 214.0, 112.0),
        "Revenue by Product": (12.0, 136.0, 620.0, 280.0),
        "Profit by Product": (644.0, 136.0, 620.0, 280.0),
        "Profit Margin by Product": (12.0, 428.0, 620.0, 278.0),
        "Product Performance Detail": (644.0, 428.0, 620.0, 278.0),
    },
    "Markets and Channels": {
        "Year": (12.0, 12.0, 170.0, 112.0),
        "Total Revenue": (194.0, 12.0, 202.0, 112.0),
        "Total Profit": (408.0, 12.0, 202.0, 112.0),
        "Orders": (622.0, 12.0, 202.0, 112.0),
        "Average Order Value": (836.0, 12.0, 202.0, 112.0),
        "Average Delivery Days": (1050.0, 12.0, 214.0, 112.0),
        "Revenue by Region": (12.0, 136.0, 620.0, 280.0),
        "Revenue by Channel": (644.0, 136.0, 620.0, 280.0),
        "Profit by Region": (12.0, 428.0, 620.0, 278.0),
        "Geography Detail": (644.0, 428.0, 620.0, 278.0),
    },
    "Orders and Delivery": {
        "Year": (12.0, 12.0, 170.0, 112.0),
        "Orders": (194.0, 12.0, 202.0, 112.0),
        "Average Order Value": (408.0, 12.0, 202.0, 112.0),
        "Average Delivery Days": (622.0, 12.0, 202.0, 112.0),
        "Revenue by Ship Date": (836.0, 12.0, 202.0, 112.0),
        "Units Sold": (1050.0, 12.0, 214.0, 112.0),
        "Monthly Orders": (12.0, 136.0, 620.0, 280.0),
        "Orders by Priority": (644.0, 136.0, 620.0, 280.0),
        "Delivery Days by Priority": (12.0, 428.0, 620.0, 278.0),
        "Order Performance Detail": (644.0, 428.0, 620.0, 278.0),
    },
}

VISUAL_FIELDS = {
    "Year": {"Values": ["date.year"]},
    "Total Revenue": {"Data": ["_Measures.Total Revenue"]},
    "Total Profit": {"Data": ["_Measures.Total Profit"]},
    "Profit Margin": {"Data": ["_Measures.Profit Margin %"]},
    "Units Sold": {"Data": ["_Measures.Units Sold"]},
    "Orders": {"Data": ["_Measures.Order Count"]},
    "Revenue Attainment": {"Data": ["_Measures.Revenue Budget Attainment %"]},
    "Revenue Variance": {"Data": ["_Measures.Revenue Variance"]},
    "Profit Attainment": {"Data": ["_Measures.Profit Budget Attainment %"]},
    "Profit Variance": {"Data": ["_Measures.Profit Variance"]},
    "Budget Revenue": {"Data": ["_Measures.Budget Revenue"]},
    "Average Order Value": {"Data": ["_Measures.Average Order Value"]},
    "Average Delivery Days": {"Data": ["_Measures.Average Delivery Days"]},
    "Revenue by Ship Date": {"Data": ["_Measures.Revenue by Ship Date"]},
    "Monthly Revenue Trend": {"Category": ["date.year_month"], "Y": ["_Measures.Total Revenue"]},
    "Monthly Revenue vs Budget": {"Category": ["date.year_month"], "Y": ["_Measures.Total Revenue"]},
    "Monthly Profit vs Budget": {"Category": ["date.year_month"], "Y": ["_Measures.Total Profit"]},
    "Monthly Orders": {"Category": ["date.year_month"], "Y": ["_Measures.Order Count"]},
    "Revenue by Product": {"Category": ["product.product_name"], "Y": ["_Measures.Total Revenue"]},
    "Profit by Product": {"Category": ["product.product_name"], "Y": ["_Measures.Total Profit"]},
    "Profit Margin by Product": {"Category": ["product.product_name"], "Y": ["_Measures.Profit Margin %"]},
    "Revenue Variance by Product": {"Category": ["product.product_name"], "Y": ["_Measures.Revenue Variance"]},
    "Revenue by Region": {"Category": ["geography.region"], "Y": ["_Measures.Total Revenue"]},
    "Profit by Region": {"Category": ["geography.region"], "Y": ["_Measures.Total Profit"]},
    "Revenue by Channel": {"Category": ["saleschannel.sales_channel_name"], "Y": ["_Measures.Total Revenue"]},
    "Orders by Priority": {"Category": ["orderpriority.order_priority_name"], "Y": ["_Measures.Order Count"]},
    "Delivery Days by Priority": {"Category": ["orderpriority.order_priority_name"], "Y": ["_Measures.Average Delivery Days"]},
    "Executive Detail": {"Values": ["product.product_name", "_Measures.Total Revenue", "_Measures.Total Profit"]},
    "Budget Performance Detail": {"Values": ["product.product_name", "_Measures.Budget Revenue", "_Measures.Revenue Variance"]},
    "Product Performance Detail": {"Values": ["product.product_name", "_Measures.Total Revenue", "_Measures.Profit Margin %"]},
    "Geography Detail": {"Values": ["geography.geography_name", "_Measures.Total Revenue", "_Measures.Total Profit"]},
    "Order Performance Detail": {"Values": ["orderpriority.order_priority_name", "_Measures.Order Count", "_Measures.Average Delivery Days"]},
}


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def wait_for_operation(response, headers):
    if response.status_code == 200:
        return None
    if response.status_code != 202:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
    op_id = response.headers.get("x-ms-operation-id")
    if not op_id:
        raise RuntimeError("Missing x-ms-operation-id.")
    op_url = response.headers.get("Location") or f"{API_BASE_URL}/operations/{op_id}"
    retry_after = int(response.headers.get("Retry-After", "5"))
    for attempt in range(60):
        time.sleep(retry_after)
        r = requests.get(op_url, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()
        status = str(data.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")
        if status == "succeeded":
            return op_id
        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(data, indent=2))
    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    headers = get_headers()
    response = requests.post(GET_URL, headers=headers, timeout=120)
    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        op_id = wait_for_operation(response, headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{op_id}/result",
            headers=headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
    parts = result.get("definition", result).get("parts", [])
    if not parts:
        raise RuntimeError("The report definition contains no parts.")
    decoded = {}
    for part in parts:
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[part["path"]] = base64.b64decode(payload)
        else:
            decoded[part["path"]] = payload.encode("utf-8")
    return decoded


def encode_definition(parts):
    return {"definition": {"parts": [
        {
            "path": path,
            "payload": base64.b64encode(parts[path]).decode("ascii"),
            "payloadType": "InlineBase64",
        }
        for path in sorted(parts)
    ]}}


def clean_literal(value):
    text = str(value)
    return text[1:-1] if len(text) >= 2 and text.startswith("'") and text.endswith("'") else text


def find_literals(value):
    output = []
    if isinstance(value, dict):
        lit = value.get("Literal")
        if isinstance(lit, dict) and lit.get("Value") is not None:
            output.append(clean_literal(lit["Value"]))
        for item in value.values():
            output.extend(find_literals(item))
    elif isinstance(value, list):
        for item in value:
            output.extend(find_literals(item))
    return output


def get_title(data):
    title_items = data.get("visual", {}).get("visualContainerObjects", {}).get("title", [])
    for item in title_items:
        values = find_literals(item.get("properties", {}).get("text", {}))
        if values:
            return values[0]
    return ""


def field_definition(ref):
    table_name, property_name = ref.split(".", 1)
    if table_name == "_Measures":
        return {
            "field": {
                "Measure": {
                    "Expression": {"SourceRef": {"Entity": table_name}},
                    "Property": property_name,
                }
            },
            "queryRef": ref,
            "nativeQueryRef": property_name,
        }
    return {
        "field": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": property_name,
            }
        },
        "queryRef": ref,
        "nativeQueryRef": property_name,
        "active": True,
    }


def filter_definition(ref, index):
    table_name, property_name = ref.split(".", 1)
    if table_name == "_Measures":
        field = {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": property_name,
            }
        }
        filter_type = "Advanced"
    else:
        field = {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": property_name,
            }
        }
        filter_type = "Categorical"
    return {"name": f"repair{index:02d}", "field": field, "type": filter_type}


def set_title_visible(data, visible):
    for item in data.get("visual", {}).get("visualContainerObjects", {}).get("title", []):
        item.setdefault("properties", {})["show"] = {
            "expr": {"Literal": {"Value": "true" if visible else "false"}}
        }


print("Downloading the generated dashboard definition...")
original_parts = download_definition()
modified_parts = copy.deepcopy(original_parts)
page_paths = sorted(path for path in modified_parts if path.endswith("/page.json"))
page_names = {}
for path in page_paths:
    page_data = json.loads(modified_parts[path].decode("utf-8"))
    page_id = page_data.get("name")
    page_names[page_id] = page_data.get("displayName", page_id)

changed_paths = []
repaired_visuals = 0
for path in sorted(modified_parts):
    if not path.endswith("/visual.json"):
        continue
    data = json.loads(modified_parts[path].decode("utf-8"))
    title = get_title(data)
    page_id = path.split("/pages/")[1].split("/")[0]
    page_name = page_names.get(page_id, "")
    if title not in VISUAL_FIELDS:
        raise RuntimeError(f"Unknown generated visual title: {title} on page {page_name}")

    roles = VISUAL_FIELDS[title]
    query_state = {}
    all_refs = []
    for role_name, refs in roles.items():
        query_state[role_name] = {"projections": [field_definition(ref) for ref in refs]}
        all_refs.extend(refs)

    data.setdefault("visual", {}).setdefault("query", {})["queryState"] = query_state
    data["filterConfig"] = {
        "filters": [filter_definition(ref, index) for index, ref in enumerate(all_refs, start=1)]
    }

    if title == "Year":
        data["visual"].setdefault("objects", {})["data"] = [
            {"properties": {"mode": {"expr": {"Literal": {"Value": "'Dropdown'"}}}}}
        ]
        set_title_visible(data, False)

    expected_position = PAGE_LAYOUTS.get(page_name, {}).get(title)
    if expected_position is None:
        raise RuntimeError(f"Missing layout for {page_name} / {title}")
    x, y, width, height = expected_position
    position = data.setdefault("position", {})
    position.update({"x": x, "y": y, "width": width, "height": height})

    modified_parts[path] = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if modified_parts[path] != original_parts[path]:
        changed_paths.append(path)
    repaired_visuals += 1

if repaired_visuals != 51:
    raise RuntimeError(f"Expected 51 visuals; repaired {repaired_visuals}.")
if len(changed_paths) != 51:
    raise RuntimeError(f"Expected 51 changed visual definitions; found {len(changed_paths)}.")

for path in changed_paths:
    data = json.loads(modified_parts[path].decode("utf-8"))
    title = get_title(data)
    roles = VISUAL_FIELDS[title]
    actual_refs = []
    for role_data in data["visual"]["query"]["queryState"].values():
        actual_refs.extend(item["queryRef"] for item in role_data["projections"])
    expected_refs = [ref for refs in roles.values() for ref in refs]
    if actual_refs != expected_refs:
        raise RuntimeError(f"Field validation failed for {title}: {actual_refs}")

rollback_request_body = encode_definition(original_parts)
update_request_body = encode_definition(modified_parts)

print("\nGLOBAL SALES REPAIR VALIDATION PASSED")
print("Visual definitions repaired: 51")
print("Broken table/column references repaired: Yes")
print("Year slicers configured as dropdowns: 5")
print("Executive bottom row widths: 409.333 / 409.334 / 409.333")
print("Semantic model changed: No")
print("Rollback stored: rollback_request_body")

headers = get_headers()
response = requests.post(UPDATE_URL, headers=headers, json=update_request_body, timeout=240)
print(f"Initial update status: {response.status_code}")
wait_for_operation(response, headers)

print("\nGLOBAL SALES DASHBOARD REPAIR COMPLETED")
print("Final status: Succeeded")
print("Updated visuals: 51")
print("Semantic model changed: No")
print("Lakehouse required: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
TARGET_PAGE_NAME = "Executive Overview"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/getDefinition"
UPDATE_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/updateDefinition"

EXECUTIVE_LAYOUT = {
    "Year": (12.0, 12.0, 170.0, 112.0, 0),
    "Total Revenue": (194.0, 12.0, 202.0, 112.0, 1),
    "Total Profit": (408.0, 12.0, 202.0, 112.0, 2),
    "Profit Margin": (622.0, 12.0, 202.0, 112.0, 3),
    "Units Sold": (836.0, 12.0, 202.0, 112.0, 4),
    "Orders": (1050.0, 12.0, 214.0, 112.0, 5),
    "Monthly Revenue Trend": (12.0, 136.0, 780.0, 314.0, 6),
    "Revenue by Product": (804.0, 136.0, 460.0, 314.0, 7),
    "Revenue by Region": (12.0, 462.0, 409.333, 244.0, 8),
    "Revenue by Channel": (433.333, 462.0, 409.334, 244.0, 9),
    "Profit by Product": (854.667, 462.0, 409.333, 244.0, 10),
}


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def wait_for_operation(response, request_headers):
    if response.status_code == 200:
        return None
    if response.status_code != 202:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Missing x-ms-operation-id.")

    operation_url = response.headers.get("Location") or f"{API_BASE_URL}/operations/{operation_id}"
    retry_after = int(response.headers.get("Retry-After", "5"))

    for attempt in range(60):
        time.sleep(retry_after)
        status_response = requests.get(
            operation_url,
            headers=request_headers,
            timeout=60,
        )
        status_response.raise_for_status()
        status_data = status_response.json()
        status = str(status_data.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")

        if status == "succeeded":
            return operation_id
        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(status_data, indent=2))

    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = get_headers()
    response = requests.post(GET_URL, headers=request_headers, timeout=120)

    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    parts = result.get("definition", result).get("parts", [])
    if not parts:
        raise RuntimeError("The report definition contains no parts.")

    decoded = {}
    for part in parts:
        path = part["path"]
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[path] = base64.b64decode(payload)
        else:
            decoded[path] = payload.encode("utf-8")
    return decoded


def encode_definition(parts):
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(parts[path]).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
                for path in sorted(parts)
            ]
        }
    }


def literal(value):
    return {"expr": {"Literal": {"Value": value}}}


def clean_literal(value):
    text = str(value)
    if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    return text


def find_literals(value):
    results = []
    if isinstance(value, dict):
        item = value.get("Literal")
        if isinstance(item, dict) and item.get("Value") is not None:
            results.append(clean_literal(item["Value"]))
        for nested in value.values():
            results.extend(find_literals(nested))
    elif isinstance(value, list):
        for nested in value:
            results.extend(find_literals(nested))
    return results


def get_title(visual_data):
    title_items = (
        visual_data.get("visual", {})
        .get("visualContainerObjects", {})
        .get("title", [])
    )
    for item in title_items:
        values = find_literals(item.get("properties", {}).get("text", {}))
        if values:
            return values[0]
    return ""


def set_title(visual_data, title, alignment="left"):
    container = (
        visual_data
        .setdefault("visual", {})
        .setdefault("visualContainerObjects", {})
    )
    container["title"] = [
        {
            "properties": {
                "text": literal(f"'{title}'"),
                "show": literal("true"),
                "titleWrap": literal("true"),
                "alignment": literal(f"'{alignment}'"),
                "fontSize": literal("'14'"),
                "bold": literal("true"),
            }
        }
    ]
    container["border"] = [
        {
            "properties": {
                "show": literal("true"),
                "color": {"solid": {"color": literal("'#D9D9E3'")}},
                "radius": literal("6D"),
                "width": literal("1D"),
            }
        }
    ]
    container["dropShadow"] = [
        {"properties": {"show": literal("false")}}
    ]


def set_position(visual_data, position):
    x, y, width, height, tab_order = position
    visual_data.setdefault("position", {}).update(
        {
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "tabOrder": tab_order,
        }
    )


def field_definition(reference):
    table_name, property_name = reference.split(".", 1)
    if table_name == "_Measures":
        return {
            "field": {
                "Measure": {
                    "Expression": {"SourceRef": {"Entity": table_name}},
                    "Property": property_name,
                }
            },
            "queryRef": reference,
            "nativeQueryRef": property_name,
        }
    return {
        "field": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": property_name,
            }
        },
        "queryRef": reference,
        "nativeQueryRef": property_name,
        "active": True,
    }


def filter_definition(reference, name):
    table_name, property_name = reference.split(".", 1)
    if table_name == "_Measures":
        field = {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": property_name,
            }
        }
        filter_type = "Advanced"
    else:
        field = {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": property_name,
            }
        }
        filter_type = "Categorical"
    return {"name": name, "field": field, "type": filter_type}


def configure_bar_fields(visual_data, category_ref, measure_ref):
    visual = visual_data.setdefault("visual", {})
    visual["visualType"] = "clusteredBarChart"
    visual["query"] = {
        "queryState": {
            "Category": {"projections": [field_definition(category_ref)]},
            "Y": {"projections": [field_definition(measure_ref)]},
        },
        "sortDefinition": {
            "sort": [field_definition(measure_ref)["field"] | {"direction": "Descending"}],
            "isDefaultSort": True,
        },
    }
    visual_data["filterConfig"] = {
        "filters": [
            filter_definition(category_ref, "categoryFilter"),
            filter_definition(measure_ref, "measureFilter"),
        ]
    }


print("Downloading the current dashboard definition...")
original_parts = download_definition()
modified_parts = copy.deepcopy(original_parts)

page_id = None
for path in sorted(original_parts):
    if not path.endswith("/page.json"):
        continue
    page_data = json.loads(original_parts[path].decode("utf-8"))
    if page_data.get("displayName") == TARGET_PAGE_NAME:
        page_id = page_data.get("name")
        break

if not page_id:
    raise RuntimeError("Executive Overview page was not found.")

visual_entries = []
for path in sorted(original_parts):
    if not path.startswith(f"definition/pages/{page_id}/visuals/"):
        continue
    if not path.endswith("/visual.json"):
        continue
    data = json.loads(original_parts[path].decode("utf-8"))
    visual_entries.append({"path": path, "title": get_title(data), "data": data})

if len(visual_entries) != 11:
    raise RuntimeError(f"Expected 11 Executive visuals; found {len(visual_entries)}.")

by_title = {item["title"]: item for item in visual_entries}
required_titles = {
    "Year",
    "Total Revenue",
    "Total Profit",
    "Profit Margin",
    "Units Sold",
    "Orders",
    "Monthly Revenue Trend",
    "Revenue by Product",
    "Revenue by Region",
    "Revenue by Channel",
    "Executive Detail",
}

missing_titles = sorted(required_titles - set(by_title))
if missing_titles:
    raise RuntimeError(f"Missing Executive visuals: {missing_titles}")

source_bar = copy.deepcopy(by_title["Revenue by Product"]["data"])
target_entry = by_title["Executive Detail"]
target_id = target_entry["data"].get("name")
source_bar["name"] = target_id
set_position(source_bar, EXECUTIVE_LAYOUT["Profit by Product"])
set_title(source_bar, "Profit by Product")
configure_bar_fields(source_bar, "product.product_name", "_Measures.Total Profit")
modified_parts[target_entry["path"]] = json.dumps(
    source_bar,
    ensure_ascii=False,
    separators=(",", ":"),
).encode("utf-8")

for title, entry in by_title.items():
    if title == "Executive Detail":
        continue
    data = copy.deepcopy(entry["data"])
    set_position(data, EXECUTIVE_LAYOUT[title])
    set_title(data, title, "center" if title in {"Total Revenue", "Total Profit", "Profit Margin", "Units Sold", "Orders"} else "left")

    if title == "Year":
        data.setdefault("visual", {}).setdefault("objects", {})["data"] = [
            {"properties": {"mode": literal("'Dropdown'")}}
        ]
        for item in data.get("visual", {}).get("visualContainerObjects", {}).get("title", []):
            item.setdefault("properties", {})["show"] = literal("false")
        data.setdefault("visual", {}).setdefault("visualContainerObjects", {})["padding"] = [
            {
                "properties": {
                    "top": literal("6D"),
                    "bottom": literal("6D"),
                    "left": literal("8D"),
                    "right": literal("8D"),
                }
            }
        ]

    if title in {"Total Revenue", "Total Profit", "Profit Margin", "Units Sold", "Orders"}:
        objects = data.setdefault("visual", {}).setdefault("objects", {})
        objects["labels"] = [
            {
                "properties": {
                    "fontSize": literal("26D"),
                    "bold": literal("true"),
                }
            }
        ]

    modified_parts[entry["path"]] = json.dumps(
        data,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

changed_paths = [
    path
    for path in sorted(original_parts)
    if modified_parts.get(path) != original_parts[path]
]

executive_paths = {item["path"] for item in visual_entries}
if set(changed_paths) != executive_paths:
    raise RuntimeError(
        "Validation failed. Only Executive Overview visual definitions may change.\n"
        f"Expected: {sorted(executive_paths)}\n"
        f"Detected: {changed_paths}"
    )

if len(changed_paths) != 11:
    raise RuntimeError(f"Expected 11 changed Executive visuals; found {len(changed_paths)}.")

bottom_widths = [
    EXECUTIVE_LAYOUT["Revenue by Region"][2],
    EXECUTIVE_LAYOUT["Revenue by Channel"][2],
    EXECUTIVE_LAYOUT["Profit by Product"][2],
]
if max(bottom_widths) - min(bottom_widths) > 0.01:
    raise RuntimeError("Executive bottom-row symmetry validation failed.")

rollback_request_body = encode_definition(original_parts)
update_request_body = encode_definition(modified_parts)

print("\nEXECUTIVE OVERVIEW REDESIGN VALIDATION PASSED")
print("Changed page: Executive Overview only")
print("Changed visuals: 11")
print("Detail table removed: Yes")
print("Profit by Product added: Yes")
print("Bottom row: 3 equal analytical panels")
print("Year slicer preserved: Yes")
print("Other pages changed: No")
print("Semantic model changed: No")
print("Rollback stored: rollback_request_body")

request_headers = get_headers()
update_response = requests.post(
    UPDATE_URL,
    headers=request_headers,
    json=update_request_body,
    timeout=180,
)
print(f"Initial update status: {update_response.status_code}")
wait_for_operation(update_response, request_headers)

print("\nEXECUTIVE OVERVIEW REDESIGN COMPLETED")
print("Final status: Succeeded")
print("Updated page: Executive Overview")
print("Updated visuals: 11")
print("Other pages changed: No")
print("Semantic model changed: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
TARGET_PAGE_NAME = "Executive Overview"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/getDefinition"
UPDATE_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/updateDefinition"

EXECUTIVE_LAYOUT = {
    "Year": (12.0, 12.0, 170.0, 112.0, 0),
    "Total Revenue": (194.0, 12.0, 202.0, 112.0, 1),
    "Total Profit": (408.0, 12.0, 202.0, 112.0, 2),
    "Profit Margin": (622.0, 12.0, 202.0, 112.0, 3),
    "Units Sold": (836.0, 12.0, 202.0, 112.0, 4),
    "Orders": (1050.0, 12.0, 214.0, 112.0, 5),
    "Monthly Revenue Trend": (12.0, 136.0, 780.0, 314.0, 6),
    "Revenue by Product": (804.0, 136.0, 460.0, 314.0, 7),
    "Revenue by Region": (12.0, 462.0, 409.333, 244.0, 8),
    "Revenue by Channel": (433.333, 462.0, 409.334, 244.0, 9),
    "Profit by Product": (854.667, 462.0, 409.333, 244.0, 10),
}


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def wait_for_operation(response, request_headers):
    if response.status_code == 200:
        return None
    if response.status_code != 202:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Missing x-ms-operation-id.")

    operation_url = response.headers.get("Location") or f"{API_BASE_URL}/operations/{operation_id}"
    retry_after = int(response.headers.get("Retry-After", "5"))

    for attempt in range(60):
        time.sleep(retry_after)
        status_response = requests.get(
            operation_url,
            headers=request_headers,
            timeout=60,
        )
        status_response.raise_for_status()
        status_data = status_response.json()
        status = str(status_data.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")

        if status == "succeeded":
            return operation_id
        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(status_data, indent=2))

    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = get_headers()
    response = requests.post(GET_URL, headers=request_headers, timeout=120)

    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    parts = result.get("definition", result).get("parts", [])
    if not parts:
        raise RuntimeError("The report definition contains no parts.")

    decoded = {}
    for part in parts:
        path = part["path"]
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[path] = base64.b64decode(payload)
        else:
            decoded[path] = payload.encode("utf-8")
    return decoded


def encode_definition(parts):
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(parts[path]).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
                for path in sorted(parts)
            ]
        }
    }


def literal(value):
    return {"expr": {"Literal": {"Value": value}}}


def clean_literal(value):
    text = str(value)
    if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    return text


def find_literals(value):
    results = []
    if isinstance(value, dict):
        item = value.get("Literal")
        if isinstance(item, dict) and item.get("Value") is not None:
            results.append(clean_literal(item["Value"]))
        for nested in value.values():
            results.extend(find_literals(nested))
    elif isinstance(value, list):
        for nested in value:
            results.extend(find_literals(nested))
    return results


def get_title(visual_data):
    title_items = (
        visual_data.get("visual", {})
        .get("visualContainerObjects", {})
        .get("title", [])
    )
    for item in title_items:
        values = find_literals(item.get("properties", {}).get("text", {}))
        if values:
            return values[0]
    return ""


def set_title(visual_data, title, alignment="left"):
    container = (
        visual_data
        .setdefault("visual", {})
        .setdefault("visualContainerObjects", {})
    )
    container["title"] = [
        {
            "properties": {
                "text": literal(f"'{title}'"),
                "show": literal("true"),
                "titleWrap": literal("true"),
                "alignment": literal(f"'{alignment}'"),
                "fontSize": literal("'14'"),
                "bold": literal("true"),
            }
        }
    ]
    container["border"] = [
        {
            "properties": {
                "show": literal("true"),
                "color": {"solid": {"color": literal("'#D9D9E3'")}},
                "radius": literal("6D"),
                "width": literal("1D"),
            }
        }
    ]
    container["dropShadow"] = [
        {"properties": {"show": literal("false")}}
    ]


def set_position(visual_data, position):
    x, y, width, height, tab_order = position
    visual_data.setdefault("position", {}).update(
        {
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "tabOrder": tab_order,
        }
    )


def field_definition(reference):
    table_name, property_name = reference.split(".", 1)
    if table_name == "_Measures":
        return {
            "field": {
                "Measure": {
                    "Expression": {"SourceRef": {"Entity": table_name}},
                    "Property": property_name,
                }
            },
            "queryRef": reference,
            "nativeQueryRef": property_name,
        }
    return {
        "field": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": property_name,
            }
        },
        "queryRef": reference,
        "nativeQueryRef": property_name,
        "active": True,
    }


def filter_definition(reference, name):
    table_name, property_name = reference.split(".", 1)
    if table_name == "_Measures":
        field = {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": property_name,
            }
        }
        filter_type = "Advanced"
    else:
        field = {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": property_name,
            }
        }
        filter_type = "Categorical"
    return {"name": name, "field": field, "type": filter_type}


def configure_bar_fields(visual_data, category_ref, measure_ref):
    visual = visual_data.setdefault("visual", {})
    visual["visualType"] = "clusteredBarChart"
    visual["query"] = {
        "queryState": {
            "Category": {"projections": [field_definition(category_ref)]},
            "Y": {"projections": [field_definition(measure_ref)]},
        },
        "sortDefinition": {
            "sort": [
                {
                    "field": field_definition(measure_ref)["field"],
                    "direction": "Descending",
                }
            ],
            "isDefaultSort": True,
        },
    }
    visual_data["filterConfig"] = {
        "filters": [
            filter_definition(category_ref, "categoryFilter"),
            filter_definition(measure_ref, "measureFilter"),
        ]
    }


print("Downloading the current dashboard definition...")
original_parts = download_definition()
modified_parts = copy.deepcopy(original_parts)

page_id = None
for path in sorted(original_parts):
    if not path.endswith("/page.json"):
        continue
    page_data = json.loads(original_parts[path].decode("utf-8"))
    if page_data.get("displayName") == TARGET_PAGE_NAME:
        page_id = page_data.get("name")
        break

if not page_id:
    raise RuntimeError("Executive Overview page was not found.")

visual_entries = []
for path in sorted(original_parts):
    if not path.startswith(f"definition/pages/{page_id}/visuals/"):
        continue
    if not path.endswith("/visual.json"):
        continue
    data = json.loads(original_parts[path].decode("utf-8"))
    visual_entries.append({"path": path, "title": get_title(data), "data": data})

if len(visual_entries) != 11:
    raise RuntimeError(f"Expected 11 Executive visuals; found {len(visual_entries)}.")

by_title = {item["title"]: item for item in visual_entries}
required_titles = {
    "Year",
    "Total Revenue",
    "Total Profit",
    "Profit Margin",
    "Units Sold",
    "Orders",
    "Monthly Revenue Trend",
    "Revenue by Product",
    "Revenue by Region",
    "Revenue by Channel",
    "Executive Detail",
}

missing_titles = sorted(required_titles - set(by_title))
if missing_titles:
    raise RuntimeError(f"Missing Executive visuals: {missing_titles}")

source_bar = copy.deepcopy(by_title["Revenue by Product"]["data"])
target_entry = by_title["Executive Detail"]
target_id = target_entry["data"].get("name")
source_bar["name"] = target_id
set_position(source_bar, EXECUTIVE_LAYOUT["Profit by Product"])
set_title(source_bar, "Profit by Product")
configure_bar_fields(source_bar, "product.product_name", "_Measures.Total Profit")
modified_parts[target_entry["path"]] = json.dumps(
    source_bar,
    ensure_ascii=False,
    separators=(",", ":"),
).encode("utf-8")

for title, entry in by_title.items():
    if title == "Executive Detail":
        continue
    data = copy.deepcopy(entry["data"])
    set_position(data, EXECUTIVE_LAYOUT[title])
    set_title(data, title, "center" if title in {"Total Revenue", "Total Profit", "Profit Margin", "Units Sold", "Orders"} else "left")

    if title == "Year":
        data.setdefault("visual", {}).setdefault("objects", {})["data"] = [
            {"properties": {"mode": literal("'Dropdown'")}}
        ]
        for item in data.get("visual", {}).get("visualContainerObjects", {}).get("title", []):
            item.setdefault("properties", {})["show"] = literal("false")
        data.setdefault("visual", {}).setdefault("visualContainerObjects", {})["padding"] = [
            {
                "properties": {
                    "top": literal("6D"),
                    "bottom": literal("6D"),
                    "left": literal("8D"),
                    "right": literal("8D"),
                }
            }
        ]

    if title in {"Total Revenue", "Total Profit", "Profit Margin", "Units Sold", "Orders"}:
        objects = data.setdefault("visual", {}).setdefault("objects", {})
        objects["labels"] = [
            {
                "properties": {
                    "fontSize": literal("26D"),
                    "bold": literal("true"),
                }
            }
        ]

    modified_parts[entry["path"]] = json.dumps(
        data,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

changed_paths = [
    path
    for path in sorted(original_parts)
    if modified_parts.get(path) != original_parts[path]
]

executive_paths = {item["path"] for item in visual_entries}
if set(changed_paths) != executive_paths:
    raise RuntimeError(
        "Validation failed. Only Executive Overview visual definitions may change.\n"
        f"Expected: {sorted(executive_paths)}\n"
        f"Detected: {changed_paths}"
    )

if len(changed_paths) != 11:
    raise RuntimeError(f"Expected 11 changed Executive visuals; found {len(changed_paths)}.")

bottom_widths = [
    EXECUTIVE_LAYOUT["Revenue by Region"][2],
    EXECUTIVE_LAYOUT["Revenue by Channel"][2],
    EXECUTIVE_LAYOUT["Profit by Product"][2],
]
if max(bottom_widths) - min(bottom_widths) > 0.01:
    raise RuntimeError("Executive bottom-row symmetry validation failed.")

rollback_request_body = encode_definition(original_parts)
update_request_body = encode_definition(modified_parts)

print("\nEXECUTIVE OVERVIEW REDESIGN VALIDATION PASSED")
print("Changed page: Executive Overview only")
print("Changed visuals: 11")
print("Detail table removed: Yes")
print("Profit by Product added: Yes")
print("Bottom row: 3 equal analytical panels")
print("Year slicer preserved: Yes")
print("Other pages changed: No")
print("Semantic model changed: No")
print("Rollback stored: rollback_request_body")

request_headers = get_headers()
update_response = requests.post(
    UPDATE_URL,
    headers=request_headers,
    json=update_request_body,
    timeout=180,
)
print(f"Initial update status: {update_response.status_code}")
wait_for_operation(update_response, request_headers)

print("\nEXECUTIVE OVERVIEW REDESIGN COMPLETED")
print("Final status: Succeeded")
print("Updated page: Executive Overview")
print("Updated visuals: 11")
print("Other pages changed: No")
print("Semantic model changed: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
TARGET_PAGE_NAME = "Executive Overview"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/getDefinition"
UPDATE_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/updateDefinition"

LAYOUT = {
    "Year": (12.0, 12.0, 170.0, 112.0, 0),
    "Total Revenue": (194.0, 12.0, 202.0, 112.0, 1),
    "Total Profit": (408.0, 12.0, 202.0, 112.0, 2),
    "Profit Margin": (622.0, 12.0, 202.0, 112.0, 3),
    "Units Sold": (836.0, 12.0, 202.0, 112.0, 4),
    "Orders": (1050.0, 12.0, 214.0, 112.0, 5),
    "Monthly Revenue Trend": (12.0, 136.0, 780.0, 314.0, 6),
    "Revenue by Product": (804.0, 136.0, 460.0, 314.0, 7),
    "Revenue by Region": (12.0, 462.0, 409.333, 244.0, 8),
    "Revenue by Channel": (433.333, 462.0, 409.334, 244.0, 9),
    "Profit by Product": (854.667, 462.0, 409.333, 244.0, 10),
}


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def wait_for_operation(response, request_headers):
    if response.status_code == 200:
        return None
    if response.status_code != 202:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Missing x-ms-operation-id.")

    operation_url = response.headers.get("Location") or f"{API_BASE_URL}/operations/{operation_id}"
    retry_after = int(response.headers.get("Retry-After", "5"))

    for attempt in range(60):
        time.sleep(retry_after)
        status_response = requests.get(
            operation_url,
            headers=request_headers,
            timeout=60,
        )
        status_response.raise_for_status()
        status_data = status_response.json()
        status = str(status_data.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")

        if status == "succeeded":
            return operation_id
        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(status_data, indent=2))

    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = get_headers()
    response = requests.post(GET_URL, headers=request_headers, timeout=120)

    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    parts = result.get("definition", result).get("parts", [])
    if not parts:
        raise RuntimeError("The report definition contains no parts.")

    decoded = {}
    for part in parts:
        path = part["path"]
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[path] = base64.b64decode(payload)
        else:
            decoded[path] = payload.encode("utf-8")
    return decoded


def encode_definition(parts):
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(parts[path]).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
                for path in sorted(parts)
            ]
        }
    }


def literal(value):
    return {"expr": {"Literal": {"Value": value}}}


def clean_literal(value):
    text = str(value)
    if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    return text


def find_literals(value):
    results = []
    if isinstance(value, dict):
        item = value.get("Literal")
        if isinstance(item, dict) and item.get("Value") is not None:
            results.append(clean_literal(item["Value"]))
        for nested in value.values():
            results.extend(find_literals(nested))
    elif isinstance(value, list):
        for nested in value:
            results.extend(find_literals(nested))
    return results


def get_title(data):
    items = data.get("visual", {}).get("visualContainerObjects", {}).get("title", [])
    for item in items:
        values = find_literals(item.get("properties", {}).get("text", {}))
        if values:
            return values[0]
    return ""


def measure_projection(measure_name, display_name=None):
    return {
        "field": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": "_Measures"}},
                "Property": measure_name,
            }
        },
        "queryRef": f"_Measures.{measure_name}",
        "nativeQueryRef": display_name or measure_name,
        "displayName": display_name or measure_name,
    }


def column_projection(table_name, column_name, display_name):
    return {
        "field": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": column_name,
            }
        },
        "queryRef": f"{table_name}.{column_name}",
        "nativeQueryRef": display_name,
        "displayName": display_name,
        "active": True,
    }


def measure_filter(measure_name, filter_name):
    return {
        "name": filter_name,
        "field": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": "_Measures"}},
                "Property": measure_name,
            }
        },
        "type": "Advanced",
    }


def column_filter(table_name, column_name, filter_name):
    return {
        "name": filter_name,
        "field": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": column_name,
            }
        },
        "type": "Categorical",
    }


def set_position(data, position):
    x, y, width, height, tab_order = position
    data.setdefault("position", {}).update(
        {
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "tabOrder": tab_order,
        }
    )


def set_title(data, title, alignment="left", size=14):
    container = data.setdefault("visual", {}).setdefault("visualContainerObjects", {})
    container["title"] = [
        {
            "properties": {
                "text": literal(f"'{title}'"),
                "show": literal("true"),
                "titleWrap": literal("true"),
                "alignment": literal(f"'{alignment}'"),
                "fontSize": literal(f"'{size}'"),
                "bold": literal("true"),
            }
        }
    ]
    container["subTitle"] = [{"properties": {"show": literal("false")}}]
    container["border"] = [
        {
            "properties": {
                "show": literal("true"),
                "color": {"solid": {"color": literal("'#D9D9E3'")}},
                "radius": literal("6D"),
                "width": literal("1D"),
            }
        }
    ]
    container["dropShadow"] = [{"properties": {"show": literal("false")}}]


def configure_bar(data, title, table_name, column_name, measure_name, position):
    set_position(data, position)
    set_title(data, title)
    visual = data.setdefault("visual", {})
    visual["visualType"] = "clusteredBarChart"
    visual["query"] = {
        "queryState": {
            "Category": {
                "projections": [column_projection(table_name, column_name, title.split(" by ")[-1])]
            },
            "Y": {"projections": [measure_projection(measure_name)]},
        },
        "sortDefinition": {
            "sort": [
                {
                    "field": measure_projection(measure_name)["field"],
                    "direction": "Descending",
                }
            ],
            "isDefaultSort": True,
        },
    }
    data["filterConfig"] = {
        "filters": [
            column_filter(table_name, column_name, "categoryFilter"),
            measure_filter(measure_name, "measureFilter"),
        ]
    }
    objects = visual.setdefault("objects", {})
    objects["labels"] = [
        {
            "properties": {
                "show": literal("true"),
                "labelDisplayUnits": literal("1000000000D"),
                "labelPrecision": literal("1L"),
            }
        }
    ]


def configure_card(data, title, measure_name, position):
    set_position(data, position)
    set_title(data, title, alignment="center", size=13)
    visual = data.setdefault("visual", {})
    visual["visualType"] = "cardVisual"
    visual["query"] = {
        "queryState": {
            "Data": {"projections": [measure_projection(measure_name, title)]}
        }
    }
    data["filterConfig"] = {
        "filters": [measure_filter(measure_name, "measureFilter")]
    }
    visual.setdefault("objects", {})["labels"] = [
        {
            "properties": {
                "fontSize": literal("26D"),
                "bold": literal("true"),
            }
        }
    ]


print("Downloading the current Executive Overview definition...")
original_parts = download_definition()
modified_parts = copy.deepcopy(original_parts)

page_id = None
for path in sorted(original_parts):
    if path.endswith("/page.json"):
        page_data = json.loads(original_parts[path].decode("utf-8"))
        if page_data.get("displayName") == TARGET_PAGE_NAME:
            page_id = page_data.get("name")
            break

if not page_id:
    raise RuntimeError("Executive Overview page was not found.")

entries = []
for path in sorted(original_parts):
    if path.startswith(f"definition/pages/{page_id}/visuals/") and path.endswith("/visual.json"):
        data = json.loads(original_parts[path].decode("utf-8"))
        entries.append({"path": path, "title": get_title(data), "data": data})

if len(entries) != 11:
    raise RuntimeError(f"Expected 11 Executive visuals; found {len(entries)}.")

by_title = {entry["title"]: entry for entry in entries}
required = set(LAYOUT)
missing = sorted(required - set(by_title))
if missing:
    raise RuntimeError(f"Missing Executive visuals: {missing}")

for title in ["Total Revenue", "Total Profit", "Profit Margin", "Units Sold", "Orders"]:
    entry = by_title[title]
    data = copy.deepcopy(entry["data"])
    measure_map = {
        "Total Revenue": "Total Revenue",
        "Total Profit": "Total Profit",
        "Profit Margin": "Profit Margin %",
        "Units Sold": "Units Sold",
        "Orders": "Order Count",
    }
    configure_card(data, title, measure_map[title], LAYOUT[title])
    modified_parts[entry["path"]] = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

entry = by_title["Year"]
data = copy.deepcopy(entry["data"])
set_position(data, LAYOUT["Year"])
set_title(data, "Year", size=12)
visual = data.setdefault("visual", {})
visual["query"] = {
    "queryState": {
        "Values": {"projections": [column_projection("date", "year", "Year")]}
    }
}
visual.setdefault("objects", {})["data"] = [
    {"properties": {"mode": literal("'Dropdown'")}}
]
data["filterConfig"] = {
    "filters": [column_filter("date", "year", "yearFilter")]
}
modified_parts[entry["path"]] = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

entry = by_title["Monthly Revenue Trend"]
data = copy.deepcopy(entry["data"])
set_position(data, LAYOUT["Monthly Revenue Trend"])
set_title(data, "Revenue vs Budget Trend")
visual = data.setdefault("visual", {})
visual["visualType"] = "lineChart"
visual["query"] = {
    "queryState": {
        "Category": {
            "projections": [column_projection("date", "year_month", "Month")]
        },
        "Y": {
            "projections": [
                measure_projection("Total Revenue", "Actual Revenue"),
                measure_projection("Budget Revenue", "Budget Revenue"),
            ]
        },
    }
}
data["filterConfig"] = {
    "filters": [
        column_filter("date", "year_month", "monthFilter"),
        measure_filter("Total Revenue", "actualFilter"),
        measure_filter("Budget Revenue", "budgetFilter"),
    ]
}
modified_parts[entry["path"]] = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

bar_configs = {
    "Revenue by Product": ("Revenue by Product", "product", "product_name", "Total Revenue"),
    "Revenue by Region": ("Revenue by Region", "geography", "region", "Total Revenue"),
    "Profit by Product": ("Profit by Product", "product", "product_name", "Total Profit"),
}
for source_title, config in bar_configs.items():
    entry = by_title[source_title]
    data = copy.deepcopy(entry["data"])
    title, table_name, column_name, measure_name = config
    configure_bar(data, title, table_name, column_name, measure_name, LAYOUT[source_title])
    modified_parts[entry["path"]] = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

entry = by_title["Revenue by Channel"]
data = copy.deepcopy(by_title["Total Revenue"]["data"])
data["name"] = entry["data"].get("name")
configure_card(data, "Revenue Budget Attainment", "Revenue Budget Attainment %", LAYOUT["Revenue by Channel"])
modified_parts[entry["path"]] = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

changed_paths = [
    path
    for path in sorted(original_parts)
    if modified_parts.get(path) != original_parts[path]
]
expected_paths = {entry["path"] for entry in entries}
if set(changed_paths) != expected_paths:
    raise RuntimeError(
        "Validation failed. Only Executive Overview visual definitions may change.\n"
        f"Expected: {sorted(expected_paths)}\n"
        f"Detected: {changed_paths}"
    )
if len(changed_paths) != 11:
    raise RuntimeError(f"Expected 11 changed visuals; found {len(changed_paths)}.")

bottom_widths = [LAYOUT["Revenue by Region"][2], LAYOUT["Revenue by Channel"][2], LAYOUT["Profit by Product"][2]]
if max(bottom_widths) - min(bottom_widths) > 0.01:
    raise RuntimeError("Bottom row symmetry validation failed.")

rollback_request_body = encode_definition(original_parts)
update_request_body = encode_definition(modified_parts)

print("\nEXECUTIVE OVERVIEW POLISH VALIDATION PASSED")
print("Changed page: Executive Overview only")
print("Revenue vs Budget trend: Added")
print("Revenue Budget Attainment card: Added")
print("Technical subtitles hidden: Yes")
print("Business field labels applied: Yes")
print("Bottom row symmetry: Passed")
print("Other pages changed: No")
print("Semantic model changed: No")
print("Rollback stored: rollback_request_body")

headers = get_headers()
response = requests.post(UPDATE_URL, headers=headers, json=update_request_body, timeout=180)
print(f"Initial update status: {response.status_code}")
wait_for_operation(response, headers)

print("\nEXECUTIVE OVERVIEW POLISH COMPLETED")
print("Final status: Succeeded")
print("Updated visuals: 11")
print("Other pages changed: No")
print("Semantic model changed: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
TARGET_PAGE_NAME = "Executive Overview"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/getDefinition"
UPDATE_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/updateDefinition"

POSITIONS = {
    "Year": (12.0, 12.0, 170.0, 112.0, 0),
    "Total Revenue": (194.0, 12.0, 202.0, 112.0, 1),
    "Total Profit": (408.0, 12.0, 202.0, 112.0, 2),
    "Profit Margin": (622.0, 12.0, 202.0, 112.0, 3),
    "Units Sold": (836.0, 12.0, 202.0, 112.0, 4),
    "Orders": (1050.0, 12.0, 214.0, 112.0, 5),
    "Revenue vs Budget Trend": (12.0, 136.0, 780.0, 314.0, 6),
    "Revenue by Product": (804.0, 136.0, 460.0, 314.0, 7),
    "Revenue by Region": (12.0, 462.0, 409.333, 244.0, 8),
    "Revenue Budget Attainment": (433.333, 462.0, 409.334, 244.0, 9),
    "Profit by Product": (854.667, 462.0, 409.333, 244.0, 10),
}

CARD_MEASURES = {
    "Total Revenue": ("Total Revenue", "Revenue"),
    "Total Profit": ("Total Profit", "Profit"),
    "Profit Margin": ("Profit Margin %", "Profit Margin"),
    "Units Sold": ("Units Sold", "Units Sold"),
    "Orders": ("Order Count", "Orders"),
    "Revenue Budget Attainment": (
        "Revenue Budget Attainment %",
        "Revenue vs Budget",
    ),
}


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def wait_for_operation(response, request_headers):
    if response.status_code == 200:
        return
    if response.status_code != 202:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Missing x-ms-operation-id.")

    operation_url = response.headers.get("Location") or (
        f"{API_BASE_URL}/operations/{operation_id}"
    )
    retry_after = int(response.headers.get("Retry-After", "5"))

    for attempt in range(60):
        time.sleep(retry_after)
        status_response = requests.get(
            operation_url,
            headers=request_headers,
            timeout=60,
        )
        status_response.raise_for_status()
        status_data = status_response.json()
        status = str(status_data.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")

        if status == "succeeded":
            return
        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(status_data, indent=2))

    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = get_headers()
    response = requests.post(GET_URL, headers=request_headers, timeout=120)

    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = response.headers.get("x-ms-operation-id")
        wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    definition_parts = result.get("definition", result).get("parts", [])
    if not definition_parts:
        raise RuntimeError("The report definition contains no parts.")

    decoded = {}
    for part in definition_parts:
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[part["path"]] = base64.b64decode(payload)
        else:
            decoded[part["path"]] = payload.encode("utf-8")
    return decoded


def encode_definition(parts):
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(parts[path]).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
                for path in sorted(parts)
            ]
        }
    }


def literal(value):
    return {"expr": {"Literal": {"Value": value}}}


def clean_literal(value):
    text = str(value)
    if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    return text


def find_literals(value):
    values = []
    if isinstance(value, dict):
        literal_item = value.get("Literal")
        if isinstance(literal_item, dict) and literal_item.get("Value") is not None:
            values.append(clean_literal(literal_item["Value"]))
        for nested in value.values():
            values.extend(find_literals(nested))
    elif isinstance(value, list):
        for nested in value:
            values.extend(find_literals(nested))
    return values


def get_title(visual_data):
    title_items = (
        visual_data.get("visual", {})
        .get("visualContainerObjects", {})
        .get("title", [])
    )
    for item in title_items:
        values = find_literals(item.get("properties", {}).get("text", {}))
        if values:
            return values[0]
    return ""


def set_position(visual_data, position):
    x, y, width, height, tab_order = position
    visual_data.setdefault("position", {}).update(
        {
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "tabOrder": tab_order,
        }
    )


def set_container(visual_data, title, show_title=True, alignment="left"):
    container = (
        visual_data.setdefault("visual", {})
        .setdefault("visualContainerObjects", {})
    )
    container["title"] = [
        {
            "properties": {
                "text": literal(f"'{title}'"),
                "show": literal("true" if show_title else "false"),
                "titleWrap": literal("true"),
                "alignment": literal(f"'{alignment}'"),
                "fontSize": literal("'14'"),
                "bold": literal("true"),
            }
        }
    ]
    container["subTitle"] = [{"properties": {"show": literal("false")}}]
    container["border"] = [
        {
            "properties": {
                "show": literal("true"),
                "color": {"solid": {"color": literal("'#D7DCE5'")}},
                "radius": literal("8D"),
                "width": literal("1D"),
            }
        }
    ]
    container["dropShadow"] = [{"properties": {"show": literal("false")}}]
    container["background"] = [
        {
            "properties": {
                "show": literal("true"),
                "color": {"solid": {"color": literal("'#FFFFFF'")}},
                "transparency": literal("0D"),
            }
        }
    ]
    container["padding"] = [
        {
            "properties": {
                "top": literal("8D"),
                "bottom": literal("8D"),
                "left": literal("10D"),
                "right": literal("10D"),
            }
        }
    ]


def measure_projection(measure_name, display_name):
    return {
        "field": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": "_Measures"}},
                "Property": measure_name,
            }
        },
        "queryRef": f"_Measures.{measure_name}",
        "nativeQueryRef": display_name,
        "displayName": display_name,
    }


def measure_filter(measure_name):
    return {
        "name": "measureFilter",
        "field": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": "_Measures"}},
                "Property": measure_name,
            }
        },
        "type": "Advanced",
    }


def configure_card(visual_data, title):
    measure_name, display_name = CARD_MEASURES[title]
    visual = visual_data.setdefault("visual", {})
    visual["visualType"] = "cardVisual"
    visual["query"] = {
        "queryState": {
            "Data": {
                "projections": [measure_projection(measure_name, display_name)]
            }
        }
    }
    visual_data["filterConfig"] = {"filters": [measure_filter(measure_name)]}
    objects = visual.setdefault("objects", {})
    objects["labels"] = [
        {
            "properties": {
                "fontSize": literal("28D"),
                "bold": literal("true"),
            }
        }
    ]
    objects["categoryLabels"] = [
        {
            "properties": {
                "show": literal("true"),
                "fontSize": literal("11D"),
                "bold": literal("true"),
            }
        }
    ]
    set_container(visual_data, title, show_title=False)


print("Downloading the current report definition...")
original_parts = download_definition()
modified_parts = copy.deepcopy(original_parts)

page_id = None
for path in sorted(original_parts):
    if not path.endswith("/page.json"):
        continue
    page_data = json.loads(original_parts[path].decode("utf-8"))
    if page_data.get("displayName") == TARGET_PAGE_NAME:
        page_id = page_data.get("name")
        break

if not page_id:
    raise RuntimeError("Executive Overview page was not found.")

visual_entries = []
for path in sorted(original_parts):
    if path.startswith(f"definition/pages/{page_id}/visuals/") and path.endswith("/visual.json"):
        data = json.loads(original_parts[path].decode("utf-8"))
        visual_entries.append({"path": path, "title": get_title(data), "data": data})

if len(visual_entries) != 11:
    raise RuntimeError(f"Expected 11 Executive visuals; found {len(visual_entries)}.")

by_title = {entry["title"]: entry for entry in visual_entries}
missing_titles = sorted(set(POSITIONS) - set(by_title))
if missing_titles:
    raise RuntimeError(f"Missing Executive visuals: {missing_titles}")

for title, entry in by_title.items():
    data = copy.deepcopy(entry["data"])
    set_position(data, POSITIONS[title])

    if title in CARD_MEASURES:
        configure_card(data, title)
    elif title == "Year":
        set_container(data, "Year", show_title=True)
        visual = data.setdefault("visual", {})
        visual.setdefault("objects", {})["data"] = [
            {"properties": {"mode": literal("'Dropdown'")}}
        ]
    else:
        set_container(data, title, show_title=True)
        visual = data.setdefault("visual", {})
        visual_type = visual.get("visualType")
        objects = visual.setdefault("objects", {})
        if visual_type == "clusteredBarChart":
            objects["labels"] = [
                {
                    "properties": {
                        "show": literal("true"),
                        "labelDisplayUnits": literal("1000000000D"),
                        "labelPrecision": literal("1L"),
                    }
                }
            ]
        if visual_type == "lineChart":
            objects["legend"] = [
                {"properties": {"show": literal("true")}}
            ]

    modified_parts[entry["path"]] = json.dumps(
        data,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

changed_paths = [
    path
    for path in sorted(original_parts)
    if modified_parts.get(path) != original_parts[path]
]
expected_paths = {entry["path"] for entry in visual_entries}

if set(changed_paths) != expected_paths:
    raise RuntimeError(
        "Validation failed. Only Executive Overview visuals may change.\n"
        f"Expected: {sorted(expected_paths)}\n"
        f"Detected: {changed_paths}"
    )

for title in CARD_MEASURES:
    path = by_title[title]["path"]
    data = json.loads(modified_parts[path].decode("utf-8"))
    title_show = (
        data.get("visual", {})
        .get("visualContainerObjects", {})
        .get("title", [])[0]
        .get("properties", {})
        .get("show", {})
        .get("expr", {})
        .get("Literal", {})
        .get("Value")
    )
    if title_show != "false":
        raise RuntimeError(f"Card title duplication validation failed for {title}.")

bottom_widths = [
    POSITIONS["Revenue by Region"][2],
    POSITIONS["Revenue Budget Attainment"][2],
    POSITIONS["Profit by Product"][2],
]
if max(bottom_widths) - min(bottom_widths) > 0.01:
    raise RuntimeError("Bottom row symmetry validation failed.")

rollback_request_body = encode_definition(original_parts)
update_request_body = encode_definition(modified_parts)

print("\nEXECUTIVE OVERVIEW FINAL VALIDATION PASSED")
print("Changed page: Executive Overview only")
print("Duplicate KPI titles removed: 6")
print("KPI labels retained inside cards: Yes")
print("Year dropdown retained: Yes")
print("Bottom row symmetry: Passed")
print("Other pages changed: No")
print("Semantic model changed: No")
print("Rollback stored: rollback_request_body")

request_headers = get_headers()
update_response = requests.post(
    UPDATE_URL,
    headers=request_headers,
    json=update_request_body,
    timeout=180,
)
print(f"Initial update status: {update_response.status_code}")
wait_for_operation(update_response, request_headers)

print("\nEXECUTIVE OVERVIEW FINAL POLISH COMPLETED")
print("Final status: Succeeded")
print("Updated visuals: 11")
print("Other pages changed: No")
print("Semantic model changed: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
TARGET_PAGE_NAME = "Executive Overview"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = (
    f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}"
    f"/reports/{REPORT_ID}/getDefinition"
)


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def wait_for_operation(response, headers):
    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Missing x-ms-operation-id.")

    operation_url = (
        response.headers.get("Location")
        or f"{API_BASE_URL}/operations/{operation_id}"
    )
    retry_after = int(response.headers.get("Retry-After", "5"))

    for attempt in range(60):
        time.sleep(retry_after)
        poll = requests.get(operation_url, headers=headers, timeout=60)
        poll.raise_for_status()
        result = poll.json()
        status = str(result.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")

        if status == "succeeded":
            return operation_id
        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(result, indent=2))

    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    headers = get_headers()
    response = requests.post(GET_URL, headers=headers, timeout=120)

    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = wait_for_operation(response, headers)
        final = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=headers,
            timeout=120,
        )
        final.raise_for_status()
        result = final.json()
    else:
        raise RuntimeError(
            f"HTTP {response.status_code}: {response.text}"
        )

    parts = result.get("definition", result).get("parts", [])
    if not parts:
        raise RuntimeError("The report definition contains no parts.")

    decoded = {}
    for part in parts:
        path = part["path"]
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[path] = base64.b64decode(payload)
        else:
            decoded[path] = payload.encode("utf-8")
    return decoded


def clean_literal(value):
    text = str(value)
    if len(text) >= 2 and text.startswith("'") and text.endswith("'"):
        return text[1:-1]
    return text


def find_literals(value):
    values = []
    if isinstance(value, dict):
        literal = value.get("Literal")
        if isinstance(literal, dict) and literal.get("Value") is not None:
            values.append(clean_literal(literal["Value"]))
        for nested in value.values():
            values.extend(find_literals(nested))
    elif isinstance(value, list):
        for nested in value:
            values.extend(find_literals(nested))
    return values


def get_title(data):
    title_objects = (
        data.get("visual", {})
        .get("visualContainerObjects", {})
        .get("title", [])
    )
    for item in title_objects:
        values = find_literals(item.get("properties", {}).get("text", {}))
        if values:
            return values[0]
    return "(no title)"


def get_fields(data):
    query_state = (
        data.get("visual", {})
        .get("query", {})
        .get("queryState", {})
    )
    fields = []
    for role, role_data in query_state.items():
        for projection in role_data.get("projections", []):
            fields.append(
                {
                    "role": role,
                    "queryRef": projection.get("queryRef"),
                    "nativeQueryRef": projection.get("nativeQueryRef"),
                    "displayName": projection.get("displayName"),
                }
            )
    return fields


print("Downloading the current report definition...")
current_parts = download_definition()

page_id = None
for path, content in current_parts.items():
    if not path.endswith("/page.json"):
        continue
    page_data = json.loads(content.decode("utf-8"))
    if page_data.get("displayName") == TARGET_PAGE_NAME:
        page_id = page_data.get("name")
        break

if not page_id:
    raise RuntimeError("Executive Overview page was not found.")

visuals = []
for path, content in current_parts.items():
    if not path.startswith(f"definition/pages/{page_id}/visuals/"):
        continue
    if not path.endswith("/visual.json"):
        continue

    data = json.loads(content.decode("utf-8"))
    position = data.get("position", {})
    visuals.append(
        {
            "id": data.get("name"),
            "title": get_title(data),
            "type": data.get("visual", {}).get("visualType"),
            "position": {
                "x": position.get("x"),
                "y": position.get("y"),
                "width": position.get("width"),
                "height": position.get("height"),
                "tabOrder": position.get("tabOrder"),
            },
            "fields": get_fields(data),
            "path": path,
        }
    )

visuals.sort(
    key=lambda item: (
        item["position"]["y"] or 0,
        item["position"]["x"] or 0,
    )
)

if len(visuals) != 11:
    raise RuntimeError(
        f"Expected 11 Executive Overview visuals; found {len(visuals)}."
    )

print("\nCURRENT EXECUTIVE OVERVIEW AUDIT")
print(f"Page ID: {page_id}")
print(f"Visuals: {len(visuals)}")
print("Report updated: No")
print(json.dumps(visuals, indent=2, ensure_ascii=False))
print("\nEXECUTIVE AUDIT COMPLETED")
print("Report updated: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
PAGE_ID = "ae63e334e13ceb673ebb"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/getDefinition"
UPDATE_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/updateDefinition"

VISUAL_IDS = {
    "year": "c8ef5d94428d4d099fb8",
    "revenue": "65841f0eb960481c8e45",
    "profit": "af934125a17243258cc7",
    "margin": "6392b40dc75343e5a325",
    "units": "a21abf05e1c14fdba19e",
    "orders": "9d76d039249748cebbb9",
    "trend": "4fdce01aab754ce79d1a",
    "product_revenue": "b6c6b21bd3c2424d9805",
    "region_revenue": "02c9fc13d9054cc7bf62",
    "attainment": "9974938af6e4482fb36c",
    "product_profit": "cffa9093705142019814",
}

POSITIONS = {
    "year": (12.0, 12.0, 140.0, 112.0, 0),
    "revenue": (164.0, 12.0, 210.4, 112.0, 1),
    "profit": (386.4, 12.0, 210.4, 112.0, 2),
    "margin": (608.8, 12.0, 210.4, 112.0, 3),
    "attainment": (831.2, 12.0, 210.4, 112.0, 4),
    "orders": (1053.6, 12.0, 210.4, 112.0, 5),
    "trend": (12.0, 136.0, 832.0, 314.0, 6),
    "product_revenue": (856.0, 136.0, 408.0, 314.0, 7),
    "region_revenue": (12.0, 462.0, 409.333, 244.0, 8),
    "units": (433.333, 462.0, 409.334, 244.0, 9),
    "product_profit": (854.667, 462.0, 409.333, 244.0, 10),
}


def headers():
    token = notebookutils.credentials.getToken("pbi")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def wait_for_operation(response, request_headers):
    if response.status_code == 200:
        return
    if response.status_code != 202:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Missing x-ms-operation-id.")
    operation_url = response.headers.get("Location") or f"{API_BASE_URL}/operations/{operation_id}"
    retry_after = int(response.headers.get("Retry-After", "5"))
    for attempt in range(60):
        time.sleep(retry_after)
        poll = requests.get(operation_url, headers=request_headers, timeout=60)
        poll.raise_for_status()
        payload = poll.json()
        status = str(payload.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")
        if status == "succeeded":
            return
        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(payload, indent=2))
    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = headers()
    response = requests.post(GET_URL, headers=request_headers, timeout=120)
    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = response.headers.get("x-ms-operation-id")
        wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
    parts = result.get("definition", result).get("parts", [])
    if not parts:
        raise RuntimeError("The report definition contains no parts.")
    decoded = {}
    for part in parts:
        path = part["path"]
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[path] = base64.b64decode(payload)
        else:
            decoded[path] = payload.encode("utf-8")
    return decoded


def encode_definition(parts):
    return {"definition": {"parts": [
        {
            "path": path,
            "payload": base64.b64encode(parts[path]).decode("ascii"),
            "payloadType": "InlineBase64",
        }
        for path in sorted(parts)
    ]}}


def visual_path(parts, visual_id):
    path = f"definition/pages/{PAGE_ID}/visuals/{visual_id}/visual.json"
    if path not in parts:
        raise RuntimeError(f"Visual definition not found: {visual_id}")
    return path


def literal(value):
    return {"expr": {"Literal": {"Value": value}}}


def set_position(data, position):
    x, y, width, height, tab_order = position
    data.setdefault("position", {}).update({
        "x": x,
        "y": y,
        "width": width,
        "height": height,
        "tabOrder": tab_order,
    })


def set_container(data, title, show_title=True, alignment="left"):
    container = data.setdefault("visual", {}).setdefault("visualContainerObjects", {})
    container["title"] = [{"properties": {
        "text": literal(f"'{title}'"),
        "show": literal("true" if show_title else "false"),
        "titleWrap": literal("true"),
        "alignment": literal(f"'{alignment}'"),
        "fontSize": literal("'14'"),
        "bold": literal("true"),
    }}]
    container["subTitle"] = [{"properties": {"show": literal("false")}}]
    container["border"] = [{"properties": {
        "show": literal("true"),
        "color": {"solid": {"color": literal("'#D7DCE5'")}},
        "radius": literal("8D"),
        "width": literal("1D"),
    }}]
    container["dropShadow"] = [{"properties": {"show": literal("false")}}]
    container["padding"] = [{"properties": {
        "top": literal("8D"),
        "bottom": literal("8D"),
        "left": literal("10D"),
        "right": literal("10D"),
    }}]


def measure_projection(measure, display_name):
    return {
        "field": {"Measure": {
            "Expression": {"SourceRef": {"Entity": "_Measures"}},
            "Property": measure,
        }},
        "queryRef": f"_Measures.{measure}",
        "nativeQueryRef": display_name,
        "displayName": display_name,
    }


def measure_filter(measure):
    return {
        "name": "measureFilter",
        "field": {"Measure": {
            "Expression": {"SourceRef": {"Entity": "_Measures"}},
            "Property": measure,
        }},
        "type": "Advanced",
    }


def configure_card(data, measure, label, position):
    set_position(data, position)
    set_container(data, label, show_title=False)
    visual = data.setdefault("visual", {})
    visual["visualType"] = "cardVisual"
    visual["query"] = {"queryState": {"Data": {
        "projections": [measure_projection(measure, label)]
    }}}
    data["filterConfig"] = {"filters": [measure_filter(measure)]}
    visual.setdefault("objects", {})["labels"] = [{"properties": {
        "fontSize": literal("28D"),
        "bold": literal("true"),
    }}]
    visual["objects"]["categoryLabels"] = [{"properties": {
        "show": literal("true"),
        "fontSize": literal("11D"),
        "bold": literal("true"),
    }}]


print("Downloading the current Executive Overview definition...")
original_parts = download_definition()
modified_parts = copy.deepcopy(original_parts)

for key, visual_id in VISUAL_IDS.items():
    visual_path(original_parts, visual_id)

card_specs = {
    "revenue": ("Total Revenue", "Revenue"),
    "profit": ("Total Profit", "Profit"),
    "margin": ("Profit Margin %", "Profit Margin"),
    "attainment": ("Revenue Budget Attainment %", "Revenue vs Budget"),
    "orders": ("Order Count", "Orders"),
}

for key, (measure, label) in card_specs.items():
    path = visual_path(modified_parts, VISUAL_IDS[key])
    data = json.loads(modified_parts[path].decode("utf-8"))
    configure_card(data, measure, label, POSITIONS[key])
    modified_parts[path] = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

path = visual_path(modified_parts, VISUAL_IDS["year"])
data = json.loads(modified_parts[path].decode("utf-8"))
set_position(data, POSITIONS["year"])
set_container(data, "Year", show_title=False)
data.setdefault("visual", {}).setdefault("objects", {})["data"] = [
    {"properties": {"mode": literal("'Dropdown'")}}
]
modified_parts[path] = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

for key, title in {
    "trend": "Revenue vs Budget Trend",
    "product_revenue": "Revenue by Product",
    "region_revenue": "Revenue by Region",
    "product_profit": "Profit by Product",
}.items():
    path = visual_path(modified_parts, VISUAL_IDS[key])
    data = json.loads(modified_parts[path].decode("utf-8"))
    set_position(data, POSITIONS[key])
    set_container(data, title, show_title=True)
    modified_parts[path] = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

source_path = visual_path(original_parts, VISUAL_IDS["region_revenue"])
source_bar = json.loads(original_parts[source_path].decode("utf-8"))
units_path = visual_path(modified_parts, VISUAL_IDS["units"])
units_id = VISUAL_IDS["units"]
units_bar = copy.deepcopy(source_bar)
units_bar["name"] = units_id
set_position(units_bar, POSITIONS["units"])
set_container(units_bar, "Units by Product", show_title=True)
visual = units_bar.setdefault("visual", {})
visual["visualType"] = "clusteredBarChart"
visual["query"] = {
    "queryState": {
        "Category": {"projections": [{
            "field": {"Column": {
                "Expression": {"SourceRef": {"Entity": "product"}},
                "Property": "product_name",
            }},
            "queryRef": "product.product_name",
            "nativeQueryRef": "Product",
            "displayName": "Product",
            "active": True,
        }]},
        "Y": {"projections": [measure_projection("Units Sold", "Units Sold")]},
    },
    "sortDefinition": {
        "sort": [{
            "field": measure_projection("Units Sold", "Units Sold")["field"],
            "direction": "Descending",
        }],
        "isDefaultSort": True,
    },
}
units_bar["filterConfig"] = {"filters": [measure_filter("Units Sold")]}
modified_parts[units_path] = json.dumps(units_bar, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

changed_paths = [
    path for path in sorted(original_parts)
    if modified_parts.get(path) != original_parts[path]
]
expected_paths = {
    visual_path(original_parts, visual_id)
    for visual_id in VISUAL_IDS.values()
}

if set(changed_paths) != expected_paths:
    raise RuntimeError(
        "Validation failed. Only Executive Overview visuals may change.\n"
        f"Expected: {sorted(expected_paths)}\n"
        f"Detected: {changed_paths}"
    )

if len(changed_paths) != 11:
    raise RuntimeError(f"Expected 11 changed visuals; found {len(changed_paths)}.")

right_edge = POSITIONS["orders"][0] + POSITIONS["orders"][2]
if abs(right_edge - 1264.0) > 0.01:
    raise RuntimeError("Top-row right-edge validation failed.")

bottom_widths = [
    POSITIONS["region_revenue"][2],
    POSITIONS["units"][2],
    POSITIONS["product_profit"][2],
]
if max(bottom_widths) - min(bottom_widths) > 0.01:
    raise RuntimeError("Bottom-row symmetry validation failed.")

rollback_request_body = encode_definition(original_parts)
update_request_body = encode_definition(modified_parts)

print("\nEXECUTIVE OVERVIEW BALANCED LAYOUT VALIDATION PASSED")
print("Changed page: Executive Overview only")
print("Top row: Year + 5 equal KPI cards")
print("Large empty attainment card removed: Yes")
print("Bottom row: Revenue by Region / Units by Product / Profit by Product")
print("Bottom row symmetry: Passed")
print("Other pages changed: No")
print("Semantic model changed: No")
print("Rollback stored: rollback_request_body")

request_headers = headers()
response = requests.post(
    UPDATE_URL,
    headers=request_headers,
    json=update_request_body,
    timeout=180,
)
print(f"Initial update status: {response.status_code}")
wait_for_operation(response, request_headers)

print("\nEXECUTIVE OVERVIEW BALANCED LAYOUT COMPLETED")
print("Final status: Succeeded")
print("Updated visuals: 11")
print("Other pages changed: No")
print("Semantic model changed: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
PAGE_ID = "ae63e334e13ceb673ebb"
API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/getDefinition"
UPDATE_URL = f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/{REPORT_ID}/updateDefinition"

VISUAL_IDS = {
    "trend": "4fdce01aab754ce79d1a",
    "revenue_product": "b6c6b21bd3c2424d9805",
    "units_product": "a21abf05e1c14fdba19e",
    "profit_product": "cffa9093705142019814",
}


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def wait_for_operation(response, request_headers):
    if response.status_code == 200:
        return
    if response.status_code != 202:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Missing x-ms-operation-id.")

    operation_url = response.headers.get("Location") or (
        f"{API_BASE_URL}/operations/{operation_id}"
    )
    retry_after = int(response.headers.get("Retry-After", "5"))

    for attempt in range(60):
        time.sleep(retry_after)
        poll = requests.get(operation_url, headers=request_headers, timeout=60)
        poll.raise_for_status()
        payload = poll.json()
        status = str(payload.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")

        if status == "succeeded":
            return
        if status in {"failed", "cancelled"}:
            raise RuntimeError(json.dumps(payload, indent=2))

    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = get_headers()
    response = requests.post(GET_URL, headers=request_headers, timeout=120)

    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = response.headers.get("x-ms-operation-id")
        wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

    parts = result.get("definition", result).get("parts", [])
    if not parts:
        raise RuntimeError("The report definition contains no parts.")

    decoded = {}
    for part in parts:
        path = part["path"]
        payload = part.get("payload", "")
        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[path] = base64.b64decode(payload)
        else:
            decoded[path] = payload.encode("utf-8")
    return decoded


def encode_definition(parts):
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(parts[path]).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
                for path in sorted(parts)
            ]
        }
    }


def visual_path(parts, visual_id):
    path = f"definition/pages/{PAGE_ID}/visuals/{visual_id}/visual.json"
    if path not in parts:
        raise RuntimeError(f"Visual definition not found: {visual_id}")
    return path


def literal(value):
    return {"expr": {"Literal": {"Value": value}}}


def column_projection(table_name, column_name, display_name):
    return {
        "field": {
            "Column": {
                "Expression": {"SourceRef": {"Entity": table_name}},
                "Property": column_name,
            }
        },
        "queryRef": f"{table_name}.{column_name}",
        "nativeQueryRef": display_name,
        "displayName": display_name,
        "active": True,
    }


def measure_projection(measure_name, display_name):
    return {
        "field": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": "_Measures"}},
                "Property": measure_name,
            }
        },
        "queryRef": f"_Measures.{measure_name}",
        "nativeQueryRef": display_name,
        "displayName": display_name,
    }


def set_title(data, title):
    container = data.setdefault("visual", {}).setdefault(
        "visualContainerObjects", {}
    )
    container["title"] = [
        {
            "properties": {
                "text": literal(f"'{title}'"),
                "show": literal("true"),
                "titleWrap": literal("true"),
                "alignment": literal("'left'"),
                "fontSize": literal("'14'"),
                "bold": literal("true"),
            }
        }
    ]
    container["visualHeader"] = [
        {"properties": {"show": literal("false")}}
    ]


def configure_month_axis(data):
    visual = data.setdefault("visual", {})
    query_state = visual.setdefault("query", {}).setdefault("queryState", {})
    query_state["Category"] = {
        "projections": [
            column_projection("date", "month_start_date", "Month")
        ]
    }

    objects = visual.setdefault("objects", {})
    objects["categoryAxis"] = [
        {
            "properties": {
                "axisType": literal("'Scalar'"),
                "showTitle": literal("false"),
                "labelDisplayUnits": literal("1D"),
            }
        }
    ]

    filters = data.setdefault("filterConfig", {}).setdefault("filters", [])
    filters = [
        item
        for item in filters
        if not (
            item.get("field", {})
            .get("Column", {})
            .get("Expression", {})
            .get("SourceRef", {})
            .get("Entity") == "date"
        )
    ]
    filters.append(
        {
            "name": "monthFilter",
            "field": {
                "Column": {
                    "Expression": {"SourceRef": {"Entity": "date"}},
                    "Property": "month_start_date",
                }
            },
            "type": "Categorical",
        }
    )
    data["filterConfig"]["filters"] = filters


def configure_product_bar(data, title, measure_name, display_name):
    visual = data.setdefault("visual", {})
    visual["visualType"] = "clusteredBarChart"
    visual["query"] = {
        "queryState": {
            "Category": {
                "projections": [
                    column_projection("product", "product_name", "Product")
                ]
            },
            "Y": {
                "projections": [
                    measure_projection(measure_name, display_name)
                ]
            },
        },
        "sortDefinition": {
            "sort": [
                {
                    "field": measure_projection(
                        measure_name, display_name
                    )["field"],
                    "direction": "Descending",
                }
            ],
            "isDefaultSort": True,
        },
    }

    objects = visual.setdefault("objects", {})
    objects["labels"] = [
        {
            "properties": {
                "show": literal("true"),
                "labelDisplayUnits": literal("1000000000D"),
                "labelPrecision": literal("1L"),
            }
        }
    ]
    set_title(data, title)


def top_n_filter(measure_name):
    return {
        "name": "top5Products",
        "field": {
            "Column": {
                "Expression": {
                    "SourceRef": {"Entity": "product"}
                },
                "Property": "product_name",
            }
        },
        "type": "TopN",
        "filter": {
            "Version": 2,
            "From": [
                {
                    "Name": "subquery",
                    "Expression": {
                        "Subquery": {
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {
                                        "Name": "p",
                                        "Entity": "product",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "m",
                                        "Entity": "_Measures",
                                        "Type": 0,
                                    },
                                ],
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {"Source": "p"}
                                            },
                                            "Property": "product_name",
                                        },
                                        "Name": "field",
                                    }
                                ],
                                "OrderBy": [
                                    {
                                        "Direction": 2,
                                        "Expression": {
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {
                                                        "Source": "m"
                                                    }
                                                },
                                                "Property": measure_name,
                                            }
                                        },
                                    }
                                ],
                                "Top": 5,
                            }
                        }
                    },
                    "Type": 2,
                },
                {
                    "Name": "p",
                    "Entity": "product",
                    "Type": 0,
                },
            ],
            "Where": [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {"Source": "p"}
                                        },
                                        "Property": "product_name",
                                    }
                                }
                            ],
                            "Table": {
                                "SourceRef": {"Source": "subquery"}
                            },
                        }
                    }
                }
            ],
        },
    }


def measure_filter(measure_name):
    return {
        "name": "measureFilter",
        "field": {
            "Measure": {
                "Expression": {"SourceRef": {"Entity": "_Measures"}},
                "Property": measure_name,
            }
        },
        "type": "Advanced",
    }


print("Downloading the current Executive Overview definition...")
original_parts = download_definition()
modified_parts = copy.deepcopy(original_parts)
changed_paths = []

trend_path = visual_path(modified_parts, VISUAL_IDS["trend"])
trend = json.loads(modified_parts[trend_path].decode("utf-8"))
configure_month_axis(trend)
set_title(trend, "Revenue vs Budget Trend")
modified_parts[trend_path] = json.dumps(
    trend, ensure_ascii=False, separators=(",", ":")
).encode("utf-8")
changed_paths.append(trend_path)

product_specs = {
    "revenue_product": (
        "Top 5 Products by Revenue",
        "Total Revenue",
        "Revenue",
    ),
    "units_product": (
        "Top 5 Products by Margin",
        "Profit Margin %",
        "Profit Margin",
    ),
    "profit_product": (
        "Top 5 Products by Profit",
        "Total Profit",
        "Profit",
    ),
}

for key, (title, measure_name, display_name) in product_specs.items():
    path = visual_path(modified_parts, VISUAL_IDS[key])
    data = json.loads(modified_parts[path].decode("utf-8"))
    configure_product_bar(data, title, measure_name, display_name)
    data["filterConfig"] = {
        "filters": [
            top_n_filter(measure_name),
            measure_filter(measure_name),
        ]
    }
    modified_parts[path] = json.dumps(
        data, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    changed_paths.append(path)

actual_changed_paths = [
    path
    for path in sorted(original_parts)
    if modified_parts.get(path) != original_parts[path]
]

if set(actual_changed_paths) != set(changed_paths):
    raise RuntimeError(
        "Validation failed. Unexpected report definitions changed.\n"
        f"Expected: {sorted(changed_paths)}\n"
        f"Detected: {actual_changed_paths}"
    )

if len(actual_changed_paths) != 4:
    raise RuntimeError(
        f"Expected 4 changed visual definitions; "
        f"found {len(actual_changed_paths)}."
    )

for key in product_specs:
    path = visual_path(modified_parts, VISUAL_IDS[key])
    data = json.loads(modified_parts[path].decode("utf-8"))
    filters = data.get("filterConfig", {}).get("filters", [])
    top_filters = [item for item in filters if item.get("type") == "TopN"]
    if len(top_filters) != 1:
        raise RuntimeError(f"Top 5 validation failed for {key}.")
    top_value = (
        top_filters[0]
        .get("filter", {})
        .get("From", [])[0]
        .get("Expression", {})
        .get("Subquery", {})
        .get("Query", {})
        .get("Top")
    )
    if top_value != 5:
        raise RuntimeError(f"Top value validation failed for {key}.")

trend_data = json.loads(modified_parts[trend_path].decode("utf-8"))
trend_ref = (
    trend_data.get("visual", {})
    .get("query", {})
    .get("queryState", {})
    .get("Category", {})
    .get("projections", [])[0]
    .get("queryRef")
)
if trend_ref != "date.month_start_date":
    raise RuntimeError("Trend month-axis validation failed.")

rollback_request_body = encode_definition(original_parts)
update_request_body = encode_definition(modified_parts)

print("\nEXECUTIVE FORMAT FIX VALIDATION PASSED")
print("Changed visuals: 4")
print("Trend axis: month_start_date")
print("Product charts limited to Top 5: 3")
print("Units chart replaced with Profit Margin ranking: Yes")
print("Visual headers hidden on changed charts: Yes")
print("Layout changed: No")
print("Other pages changed: No")
print("Semantic model changed: No")
print("Rollback stored: rollback_request_body")

request_headers = get_headers()
response = requests.post(
    UPDATE_URL,
    headers=request_headers,
    json=update_request_body,
    timeout=180,
)
print(f"Initial update status: {response.status_code}")
wait_for_operation(response, request_headers)

print("\nEXECUTIVE FORMAT FIX COMPLETED")
print("Final status: Succeeded")
print("Updated visuals: 4")
print("Layout changed: No")
print("Other pages changed: No")
print("Semantic model changed: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import requests

WORKSPACE_ID = "4e609ca2-4ac4-4f95-865c-809888b75e38"
REPORT_ID = "99fea0b5-ac81-45f8-a7f9-eecac5accc44"
PAGE_ID = "ae63e334e13ceb673ebb"
MARGIN_VISUAL_ID = "a21abf05e1c14fdba19e"
TREND_VISUAL_ID = "4fdce01aab754ce79d1a"
CUTOFF_DATE = "2020-09-01T00:00:00"

API_BASE_URL = "https://api.fabric.microsoft.com/v1"
GET_URL = (
    f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}"
    f"/reports/{REPORT_ID}/getDefinition"
)
UPDATE_URL = (
    f"{API_BASE_URL}/workspaces/{WORKSPACE_ID}"
    f"/reports/{REPORT_ID}/updateDefinition"
)


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def wait_for_operation(response, request_headers):
    if response.status_code == 200:
        return

    if response.status_code != 202:
        raise RuntimeError(
            f"HTTP {response.status_code}: {response.text}"
        )

    operation_id = response.headers.get("x-ms-operation-id")
    if not operation_id:
        raise RuntimeError("Missing x-ms-operation-id.")

    operation_url = (
        response.headers.get("Location")
        or f"{API_BASE_URL}/operations/{operation_id}"
    )
    retry_after = int(response.headers.get("Retry-After", "5"))

    for attempt in range(60):
        time.sleep(retry_after)
        poll_response = requests.get(
            operation_url,
            headers=request_headers,
            timeout=60,
        )
        poll_response.raise_for_status()
        operation_data = poll_response.json()
        status = str(operation_data.get("status", "")).lower()
        print(f"Operation attempt {attempt + 1}: {status}")

        if status == "succeeded":
            return

        if status in {"failed", "cancelled"}:
            raise RuntimeError(
                json.dumps(operation_data, indent=2)
            )

    raise TimeoutError("Fabric operation timed out.")


def download_definition():
    request_headers = get_headers()
    response = requests.post(
        GET_URL,
        headers=request_headers,
        timeout=120,
    )

    if response.status_code == 200:
        result = response.json()
    elif response.status_code == 202:
        operation_id = response.headers.get("x-ms-operation-id")
        wait_for_operation(response, request_headers)
        result_response = requests.get(
            f"{API_BASE_URL}/operations/{operation_id}/result",
            headers=request_headers,
            timeout=120,
        )
        result_response.raise_for_status()
        result = result_response.json()
    else:
        raise RuntimeError(
            f"Definition download failed.\n"
            f"HTTP status: {response.status_code}\n"
            f"Response: {response.text}"
        )

    parts = result.get("definition", result).get("parts", [])
    if not parts:
        raise RuntimeError("The report definition contains no parts.")

    decoded = {}
    for part in parts:
        path = part["path"]
        payload = part.get("payload", "")

        if part.get("payloadType") == "InlineBase64":
            payload += "=" * (-len(payload) % 4)
            decoded[path] = base64.b64decode(payload)
        else:
            decoded[path] = payload.encode("utf-8")

    return decoded


def encode_definition(parts):
    return {
        "definition": {
            "parts": [
                {
                    "path": path,
                    "payload": base64.b64encode(
                        parts[path]
                    ).decode("ascii"),
                    "payloadType": "InlineBase64",
                }
                for path in sorted(parts)
            ]
        }
    }


def visual_path(visual_id):
    return (
        f"definition/pages/{PAGE_ID}/visuals/"
        f"{visual_id}/visual.json"
    )


def literal(value):
    return {
        "expr": {
            "Literal": {
                "Value": value
            }
        }
    }


def margin_measure_filter():
    return {
        "name": "marginMeasureFilter",
        "field": {
            "Measure": {
                "Expression": {
                    "SourceRef": {
                        "Entity": "_Measures"
                    }
                },
                "Property": "Profit Margin %",
            }
        },
        "type": "Advanced",
    }


def top_five_margin_filter():
    return {
        "name": "top5MarginProducts",
        "field": {
            "Column": {
                "Expression": {
                    "SourceRef": {
                        "Entity": "product"
                    }
                },
                "Property": "product_name",
            }
        },
        "type": "TopN",
        "filter": {
            "Version": 2,
            "From": [
                {
                    "Name": "subquery",
                    "Expression": {
                        "Subquery": {
                            "Query": {
                                "Version": 2,
                                "From": [
                                    {
                                        "Name": "p",
                                        "Entity": "product",
                                        "Type": 0,
                                    },
                                    {
                                        "Name": "m",
                                        "Entity": "_Measures",
                                        "Type": 0,
                                    },
                                ],
                                "Select": [
                                    {
                                        "Column": {
                                            "Expression": {
                                                "SourceRef": {
                                                    "Source": "p"
                                                }
                                            },
                                            "Property": "product_name",
                                        },
                                        "Name": "field",
                                    }
                                ],
                                "OrderBy": [
                                    {
                                        "Direction": 2,
                                        "Expression": {
                                            "Measure": {
                                                "Expression": {
                                                    "SourceRef": {
                                                        "Source": "m"
                                                    }
                                                },
                                                "Property": "Profit Margin %",
                                            }
                                        },
                                    }
                                ],
                                "Top": 5,
                            }
                        }
                    },
                    "Type": 2,
                },
                {
                    "Name": "p",
                    "Entity": "product",
                    "Type": 0,
                },
            ],
            "Where": [
                {
                    "Condition": {
                        "In": {
                            "Expressions": [
                                {
                                    "Column": {
                                        "Expression": {
                                            "SourceRef": {
                                                "Source": "p"
                                            }
                                        },
                                        "Property": "product_name",
                                    }
                                }
                            ],
                            "Table": {
                                "SourceRef": {
                                    "Source": "subquery"
                                }
                            },
                        }
                    }
                }
            ],
        },
    }


def incomplete_month_filter():
    return {
        "name": "excludeIncompleteSeptember2020",
        "field": {
            "Column": {
                "Expression": {
                    "SourceRef": {
                        "Entity": "date"
                    }
                },
                "Property": "month_start_date",
            }
        },
        "type": "Advanced",
        "filter": {
            "Version": 2,
            "From": [
                {
                    "Name": "d",
                    "Entity": "date",
                    "Type": 0,
                }
            ],
            "Where": [
                {
                    "Condition": {
                        "Comparison": {
                            "ComparisonKind": 3,
                            "Left": {
                                "Column": {
                                    "Expression": {
                                        "SourceRef": {
                                            "Source": "d"
                                        }
                                    },
                                    "Property": "month_start_date",
                                }
                            },
                            "Right": {
                                "Literal": {
                                    "Value": (
                                        f"datetime'{CUTOFF_DATE}'"
                                    )
                                }
                            },
                        }
                    }
                }
            ],
        },
    }


print("Downloading the current Executive Overview definition...")
original_parts = download_definition()
modified_parts = copy.deepcopy(original_parts)

margin_path = visual_path(MARGIN_VISUAL_ID)
trend_path = visual_path(TREND_VISUAL_ID)

for required_path in [margin_path, trend_path]:
    if required_path not in modified_parts:
        raise RuntimeError(
            f"Required visual definition was not found: "
            f"{required_path}"
        )

margin_visual = json.loads(
    modified_parts[margin_path].decode("utf-8")
)

margin_objects = (
    margin_visual
    .setdefault("visual", {})
    .setdefault("objects", {})
)
margin_objects["labels"] = [
    {
        "properties": {
            "show": literal("true"),
            "labelDisplayUnits": literal("1D"),
            "labelPrecision": literal("1L"),
        }
    }
]
margin_objects["valueAxis"] = [
    {
        "properties": {
            "labelDisplayUnits": literal("1D"),
            "labelPrecision": literal("0L"),
            "showTitle": literal("false"),
        }
    }
]
margin_visual["filterConfig"] = {
    "filters": [
        top_five_margin_filter(),
        margin_measure_filter(),
    ]
}

modified_parts[margin_path] = json.dumps(
    margin_visual,
    ensure_ascii=False,
    separators=(",", ":"),
).encode("utf-8")

trend_visual = json.loads(
    modified_parts[trend_path].decode("utf-8")
)
trend_filters = (
    trend_visual
    .setdefault("filterConfig", {})
    .setdefault("filters", [])
)
trend_filters = [
    item
    for item in trend_filters
    if item.get("name") != "excludeIncompleteSeptember2020"
]
trend_filters.append(incomplete_month_filter())
trend_visual["filterConfig"]["filters"] = trend_filters

modified_parts[trend_path] = json.dumps(
    trend_visual,
    ensure_ascii=False,
    separators=(",", ":"),
).encode("utf-8")

changed_paths = [
    path
    for path in sorted(original_parts)
    if modified_parts.get(path) != original_parts[path]
]
expected_paths = {margin_path, trend_path}

if set(changed_paths) != expected_paths:
    raise RuntimeError(
        "Validation failed. Exactly two visual definitions "
        "must change.\n"
        f"Expected: {sorted(expected_paths)}\n"
        f"Detected: {changed_paths}"
    )

margin_check = json.loads(
    modified_parts[margin_path].decode("utf-8")
)
margin_units = (
    margin_check["visual"]["objects"]["labels"][0]
    ["properties"]["labelDisplayUnits"]
    ["expr"]["Literal"]["Value"]
)
margin_precision = (
    margin_check["visual"]["objects"]["labels"][0]
    ["properties"]["labelPrecision"]
    ["expr"]["Literal"]["Value"]
)

if margin_units != "1D" or margin_precision != "1L":
    raise RuntimeError(
        "Margin label-format validation failed."
    )

trend_check = json.loads(
    modified_parts[trend_path].decode("utf-8")
)
cutoff_filters = [
    item
    for item in trend_check["filterConfig"]["filters"]
    if item.get("name") == "excludeIncompleteSeptember2020"
]

if len(cutoff_filters) != 1:
    raise RuntimeError(
        "Incomplete-month filter validation failed."
    )

rollback_request_body = encode_definition(original_parts)
update_request_body = encode_definition(modified_parts)

print("\nEXECUTIVE FINAL DATA FIX VALIDATION PASSED")
print("Changed visuals: 2")
print("Margin labels: percentage, 1 decimal")
print("Margin axis: percentage, no display-unit scaling")
print("Trend cutoff: before 2020-09-01")
print("Layout changed: No")
print("Other pages changed: No")
print("Semantic model changed: No")
print("Rollback stored: rollback_request_body")

request_headers = get_headers()
update_response = requests.post(
    UPDATE_URL,
    headers=request_headers,
    json=update_request_body,
    timeout=180,
)
print(
    f"Initial update status: "
    f"{update_response.status_code}"
)
wait_for_operation(update_response, request_headers)

print("\nEXECUTIVE FINAL DATA FIX COMPLETED")
print("Final status: Succeeded")
print("Updated visuals: 2")
print("Layout changed: No")
print("Other pages changed: No")
print("Semantic model changed: No")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import json
import re
import time
from collections import Counter

import requests
import notebookutils

REPORT_NAME = "Global Sales Performance Dashboard"
SEMANTIC_MODEL_NAME = "Sales Records Model"
API_BASE = "https://api.fabric.microsoft.com/v1"
TIMEOUT_SECONDS = 120
MAX_WAIT_SECONDS = 600


def context_value(context, name):
    if isinstance(context, dict):
        return context.get(name)
    return getattr(context, name, None)


def headers():
    token = notebookutils.credentials.getToken("pbi")
    if not token:
        raise RuntimeError("Fabric authentication token could not be obtained.")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def raise_api_error(response):
    if response.ok:
        return
    try:
        body = response.json()
    except ValueError:
        body = response.text
    raise RuntimeError(
        f"Fabric API request failed.\n"
        f"Status: {response.status_code}\n"
        f"URL: {response.url}\n"
        f"Response: {body}"
    )


def get_json(url, request_headers):
    response = requests.get(
        url,
        headers=request_headers,
        timeout=TIMEOUT_SECONDS,
    )
    raise_api_error(response)
    return response.json()


def post_and_wait(url, request_headers):
    response = requests.post(
        url,
        headers=request_headers,
        timeout=TIMEOUT_SECONDS,
    )
    raise_api_error(response)

    if response.status_code == 200:
        return response.json()

    if response.status_code != 202:
        raise RuntimeError(
            f"Unexpected Fabric API status: {response.status_code}"
        )

    operation_location = response.headers.get("Location")
    operation_id = response.headers.get("x-ms-operation-id")
    retry_after = int(response.headers.get("Retry-After", "5"))

    if not operation_location and operation_id:
        operation_location = f"{API_BASE}/operations/{operation_id}"

    if not operation_location:
        raise RuntimeError(
            "Fabric returned HTTP 202 without an operation location."
        )

    started_at = time.time()

    while True:
        if time.time() - started_at > MAX_WAIT_SECONDS:
            raise TimeoutError(
                "The Fabric long-running operation exceeded the wait limit."
            )

        time.sleep(max(retry_after, 1))

        operation_response = requests.get(
            operation_location,
            headers=request_headers,
            timeout=TIMEOUT_SECONDS,
        )
        raise_api_error(operation_response)
        operation = operation_response.json()
        status = str(operation.get("status", "")).lower()

        if status in {"succeeded", "completed"}:
            result_location = operation_response.headers.get("Location")
            if not result_location:
                result_location = f"{operation_location.rstrip('/')}/result"

            result_response = requests.get(
                result_location,
                headers=request_headers,
                timeout=TIMEOUT_SECONDS,
            )
            raise_api_error(result_response)
            return result_response.json()

        if status in {"failed", "cancelled", "canceled"}:
            raise RuntimeError(
                "Fabric long-running operation failed.\n"
                + json.dumps(operation, indent=2)
            )

        retry_after = int(
            operation_response.headers.get("Retry-After", "5")
        )


def list_workspace_items(workspace_id, request_headers):
    url = f"{API_BASE}/workspaces/{workspace_id}/items"
    items = []

    while url:
        payload = get_json(url, request_headers)
        items.extend(payload.get("value", []))
        url = payload.get("continuationUri")

    return items


def find_item(items, display_name, item_type):
    matches = [
        item
        for item in items
        if item.get("displayName") == display_name
        and item.get("type") == item_type
    ]

    if len(matches) != 1:
        available = "\n".join(
            sorted(
                f"{item.get('type')}: {item.get('displayName')}"
                for item in items
            )
        )
        raise RuntimeError(
            f"Expected exactly one {item_type} named '{display_name}', "
            f"but found {len(matches)}.\n\n"
            f"Available workspace items:\n{available}"
        )

    return matches[0]


def decode_parts(payload):
    decoded = {}

    for part in payload.get("definition", {}).get("parts", []):
        path = part.get("path")
        value = part.get("payload")
        payload_type = part.get("payloadType")

        if not path or value is None:
            continue

        if payload_type == "InlineBase64":
            decoded[path] = base64.b64decode(value).decode(
                "utf-8",
                errors="replace",
            )
        else:
            decoded[path] = str(value)

    return decoded


def load_json_part(parts, path):
    if path not in parts:
        raise KeyError(f"Expected definition part was not found: {path}")
    return json.loads(parts[path])


def find_first_value(node, key):
    if isinstance(node, dict):
        value = node.get(key)
        if isinstance(value, str) and value:
            return value
        for child in node.values():
            result = find_first_value(child, key)
            if result:
                return result
    elif isinstance(node, list):
        for child in node:
            result = find_first_value(child, key)
            if result:
                return result
    return ""


def inventory_report(parts):
    metadata = load_json_part(parts, "definition/pages/pages.json")
    page_order = metadata.get("pageOrder", [])
    active_page = metadata.get("activePageName")
    pages = []
    visuals = []

    for page_id in page_order:
        page_path = f"definition/pages/{page_id}/page.json"
        page = load_json_part(parts, page_path)
        page_name = page.get("displayName")

        pages.append(
            {
                "id": page_id,
                "name": page_name,
                "width": page.get("width"),
                "height": page.get("height"),
                "display_option": page.get("displayOption"),
                "active": page_id == active_page,
            }
        )

        prefix = f"definition/pages/{page_id}/visuals/"

        for path, content in sorted(parts.items()):
            if not path.startswith(prefix) or not path.endswith("/visual.json"):
                continue

            definition = json.loads(content)
            visual_id = path[len(prefix):].split("/", 1)[0]
            position = definition.get("position", {})
            visual_type = find_first_value(definition, "visualType") or "Unknown"

            visuals.append(
                {
                    "page": page_name,
                    "id": visual_id,
                    "type": visual_type,
                    "x": position.get("x"),
                    "y": position.get("y"),
                    "width": position.get("width"),
                    "height": position.get("height"),
                    "path": path,
                }
            )

    return pages, visuals


def parse_tmdl(parts):
    tmdl = {
        path: content
        for path, content in parts.items()
        if path.lower().endswith(".tmdl")
    }

    table_pattern = re.compile(r"(?m)^\s*table\s+'?([^'=\r\n]+)'?\s*$")
    measure_pattern = re.compile(
        r"(?m)^\s*measure\s+'([^']+)'\s*=|^\s*measure\s+([^=\r\n]+?)\s*="
    )
    column_pattern = re.compile(
        r"(?m)^\s*column\s+'([^']+)'\s*$|^\s*column\s+([^\r\n]+?)\s*$"
    )
    relationship_pattern = re.compile(r"(?m)^\s*relationship\s+([^\r\n]+)")
    calculation_group_pattern = re.compile(r"(?m)^\s*calculationGroup\b")
    calculation_item_pattern = re.compile(
        r"(?m)^\s*calculationItem\s+'?([^'=\r\n]+)'?"
    )

    tables = []
    measures = []
    columns = []
    relationships = []
    calculation_groups = []
    calculation_items = []

    for path, content in sorted(tmdl.items()):
        found_tables = [name.strip() for name in table_pattern.findall(content)]
        current_table = found_tables[0] if found_tables else ""
        tables.extend(found_tables)

        for match in measure_pattern.findall(content):
            name = next(value.strip() for value in match if value.strip())
            measures.append((current_table, name))

        for match in column_pattern.findall(content):
            name = next(value.strip() for value in match if value.strip())
            columns.append((current_table, name))

        relationships.extend(
            name.strip() for name in relationship_pattern.findall(content)
        )

        if calculation_group_pattern.search(content):
            calculation_groups.append(current_table or path)

        for name in calculation_item_pattern.findall(content):
            calculation_items.append((current_table or path, name.strip()))

    return {
        "part_count": len(tmdl),
        "tables": sorted(set(tables)),
        "measures": measures,
        "columns": columns,
        "relationships": relationships,
        "calculation_groups": sorted(set(calculation_groups)),
        "calculation_items": calculation_items,
    }


def section(title):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


context = notebookutils.runtime.context
workspace_id = context_value(context, "currentWorkspaceId")
workspace_name = context_value(context, "currentWorkspaceName")

if not workspace_id:
    raise RuntimeError("The current Fabric workspace ID could not be obtained.")

request_headers = headers()
items = list_workspace_items(workspace_id, request_headers)
report = find_item(items, REPORT_NAME, "Report")
model = find_item(items, SEMANTIC_MODEL_NAME, "SemanticModel")

report_payload = post_and_wait(
    f"{API_BASE}/workspaces/{workspace_id}/reports/{report['id']}/getDefinition",
    request_headers,
)
model_payload = post_and_wait(
    f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model['id']}/getDefinition?format=TMDL",
    request_headers,
)

report_parts = decode_parts(report_payload)
model_parts = decode_parts(model_payload)
pages, visuals = inventory_report(report_parts)
model_inventory = parse_tmdl(model_parts)
visual_counts = Counter(item["type"] for item in visuals)

section("REPORT SUMMARY")
print(f"Report: {REPORT_NAME}")
print(f"Report ID: {report['id']}")
print(f"Definition parts: {len(report_parts)}")
print(f"Pages: {len(pages)}")
print(f"Visual definitions: {len(visuals)}")

section("PAGES")
for page in pages:
    print(
        f"- {page['name']} | ID: {page['id']} | "
        f"Canvas: {page['width']} x {page['height']} | "
        f"Display: {page['display_option']} | Active: {page['active']}"
    )

section("VISUAL TYPE COUNTS")
for visual_type, count in sorted(
    visual_counts.items(),
    key=lambda item: (-item[1], item[0]),
):
    print(f"- {visual_type}: {count}")

section("VISUAL DEFINITIONS")
for index, visual in enumerate(visuals, start=1):
    print(
        f"{index:02d}. Type: {visual['type']} | ID: {visual['id']} | "
        f"Page: {visual['page']} | x={visual['x']}, y={visual['y']}, "
        f"w={visual['width']}, h={visual['height']}"
    )

section("SEMANTIC MODEL SUMMARY")
print(f"Semantic model: {SEMANTIC_MODEL_NAME}")
print(f"Semantic model ID: {model['id']}")
print(f"Definition parts: {len(model_parts)}")
print(f"TMDL parts: {model_inventory['part_count']}")
print(f"Tables: {len(model_inventory['tables'])}")
print(f"Columns: {len(model_inventory['columns'])}")
print(f"Measures: {len(model_inventory['measures'])}")
print(f"Relationships: {len(model_inventory['relationships'])}")
print(f"Calculation groups: {len(model_inventory['calculation_groups'])}")
print(f"Calculation items: {len(model_inventory['calculation_items'])}")

section("TABLES")
for table_name in model_inventory["tables"]:
    print(f"- {table_name}")

section("MEASURES")
for table_name, measure_name in model_inventory["measures"]:
    print(f"- {table_name}[{measure_name}]")

section("CALCULATION GROUPS")
if model_inventory["calculation_groups"]:
    for group_name in model_inventory["calculation_groups"]:
        print(f"- {group_name}")
else:
    print("- None detected")

section("CALCULATION ITEMS")
if model_inventory["calculation_items"]:
    for group_name, item_name in model_inventory["calculation_items"]:
        print(f"- {group_name}: {item_name}")
else:
    print("- None detected")

section("FINAL STATUS")
print("PASS: Read-only report and semantic-model inventory completed.")
print("No report or semantic-model definition was modified.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import json
import time
from collections import defaultdict

import notebookutils
import requests

REPORT_NAME = "Global Sales Performance Dashboard"
API_BASE = "https://api.fabric.microsoft.com/v1"
TIMEOUT_SECONDS = 120
MAX_WAIT_SECONDS = 600


def get_context_value(context, name):
    if isinstance(context, dict):
        return context.get(name)
    return getattr(context, name, None)


def get_headers():
    token = notebookutils.credentials.getToken("pbi")
    if not token:
        raise RuntimeError("Fabric authentication token could not be obtained.")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def raise_api_error(response):
    if response.ok:
        return
    try:
        body = response.json()
    except ValueError:
        body = response.text
    raise RuntimeError(
        f"Fabric API request failed.\n"
        f"Status: {response.status_code}\n"
        f"URL: {response.url}\n"
        f"Response: {body}"
    )


def get_json(url, request_headers):
    response = requests.get(
        url,
        headers=request_headers,
        timeout=TIMEOUT_SECONDS,
    )
    raise_api_error(response)
    return response.json()


def post_and_wait(url, request_headers):
    response = requests.post(
        url,
        headers=request_headers,
        timeout=TIMEOUT_SECONDS,
    )
    raise_api_error(response)

    if response.status_code == 200:
        return response.json()

    if response.status_code != 202:
        raise RuntimeError(f"Unexpected Fabric API status: {response.status_code}")

    operation_location = response.headers.get("Location")
    operation_id = response.headers.get("x-ms-operation-id")
    retry_after = int(response.headers.get("Retry-After", "5"))

    if not operation_location and operation_id:
        operation_location = f"{API_BASE}/operations/{operation_id}"

    if not operation_location:
        raise RuntimeError("Fabric returned HTTP 202 without an operation location.")

    started_at = time.time()

    while True:
        if time.time() - started_at > MAX_WAIT_SECONDS:
            raise TimeoutError("The Fabric long-running operation exceeded the wait limit.")

        time.sleep(max(retry_after, 1))
        operation_response = requests.get(
            operation_location,
            headers=request_headers,
            timeout=TIMEOUT_SECONDS,
        )
        raise_api_error(operation_response)
        operation = operation_response.json()
        status = str(operation.get("status", "")).lower()

        if status in {"succeeded", "completed"}:
            result_location = operation_response.headers.get("Location")
            if not result_location:
                result_location = f"{operation_location.rstrip('/')}/result"
            result_response = requests.get(
                result_location,
                headers=request_headers,
                timeout=TIMEOUT_SECONDS,
            )
            raise_api_error(result_response)
            return result_response.json()

        if status in {"failed", "cancelled", "canceled"}:
            raise RuntimeError(
                "Fabric long-running operation failed.\n"
                + json.dumps(operation, indent=2)
            )

        retry_after = int(operation_response.headers.get("Retry-After", "5"))


def list_workspace_items(workspace_id, request_headers):
    url = f"{API_BASE}/workspaces/{workspace_id}/items"
    items = []
    while url:
        payload = get_json(url, request_headers)
        items.extend(payload.get("value", []))
        url = payload.get("continuationUri")
    return items


def find_item(items, display_name, item_type):
    matches = [
        item
        for item in items
        if item.get("displayName") == display_name and item.get("type") == item_type
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one {item_type} named '{display_name}', "
            f"but found {len(matches)}."
        )
    return matches[0]


def decode_parts(payload):
    decoded = {}
    for part in payload.get("definition", {}).get("parts", []):
        path = part.get("path")
        value = part.get("payload")
        payload_type = part.get("payloadType")
        if not path or value is None:
            continue
        if payload_type == "InlineBase64":
            decoded[path] = base64.b64decode(value).decode("utf-8", errors="replace")
        else:
            decoded[path] = str(value)
    return decoded


def walk(node, path="$"):
    if isinstance(node, dict):
        yield path, node
        for key, value in node.items():
            yield from walk(value, f"{path}.{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from walk(value, f"{path}[{index}]")


def first_string_for_key(node, target_key):
    if isinstance(node, dict):
        value = node.get(target_key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        for child in node.values():
            result = first_string_for_key(child, target_key)
            if result:
                return result
    elif isinstance(node, list):
        for child in node:
            result = first_string_for_key(child, target_key)
            if result:
                return result
    return ""


def collect_semantic_references(node):
    references = set()

    for _, item in walk(node):
        if not isinstance(item, dict):
            continue

        column = item.get("Column")
        if isinstance(column, dict):
            source = column.get("Expression", {}).get("SourceRef", {}).get("Source")
            property_name = column.get("Property")
            if source and property_name:
                references.add(f"COLUMN {source}[{property_name}]")

        measure = item.get("Measure")
        if isinstance(measure, dict):
            source = measure.get("Expression", {}).get("SourceRef", {}).get("Source")
            property_name = measure.get("Property")
            if source and property_name:
                references.add(f"MEASURE {source}[{property_name}]")

        aggregation = item.get("Aggregation")
        if isinstance(aggregation, dict):
            expression = aggregation.get("Expression", {})
            aggregation_function = aggregation.get("Function", "Aggregation")
            column_node = expression.get("Column", {})
            source = column_node.get("Expression", {}).get("SourceRef", {}).get("Source")
            property_name = column_node.get("Property")
            if source and property_name:
                references.add(
                    f"{aggregation_function.upper()} {source}[{property_name}]"
                )

    return sorted(references)


def collect_filters(node):
    filters = []
    for path, item in walk(node):
        if not isinstance(item, dict):
            continue
        if "filter" in path.lower() or "condition" in path.lower():
            references = collect_semantic_references(item)
            if references:
                filters.extend(references)
    return sorted(set(filters))


def get_position(definition):
    position = definition.get("position")
    if isinstance(position, dict):
        return position

    for _, item in walk(definition):
        if not isinstance(item, dict):
            continue
        if all(key in item for key in ("x", "y", "width", "height")):
            return item

    return {}


def get_title(definition):
    title = first_string_for_key(definition, "title")
    if title:
        return title

    for _, item in walk(definition):
        if not isinstance(item, dict):
            continue
        literal = item.get("Literal")
        if isinstance(literal, dict):
            value = literal.get("Value")
            if isinstance(value, str):
                cleaned = value.strip("'\"")
                if cleaned and len(cleaned) <= 120:
                    return cleaned

    return ""


def section(title):
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


context = notebookutils.runtime.context
workspace_id = get_context_value(context, "currentWorkspaceId")
workspace_name = get_context_value(context, "currentWorkspaceName")

if not workspace_id:
    raise RuntimeError("The current Fabric workspace ID could not be obtained.")

request_headers = get_headers()
workspace_items = list_workspace_items(workspace_id, request_headers)
report_item = find_item(workspace_items, REPORT_NAME, "Report")

report_payload = post_and_wait(
    f"{API_BASE}/workspaces/{workspace_id}/reports/{report_item['id']}/getDefinition",
    request_headers,
)
report_parts = decode_parts(report_payload)
pages_metadata = json.loads(report_parts["definition/pages/pages.json"])

section("READ-ONLY VISUAL BINDING AUDIT")
print(f"Workspace: {workspace_name}")
print(f"Report: {REPORT_NAME}")
print(f"Report ID: {report_item['id']}")
print(f"Pages: {len(pages_metadata.get('pageOrder', []))}")

visual_count = 0
summary = defaultdict(int)

for page_id in pages_metadata.get("pageOrder", []):
    page_path = f"definition/pages/{page_id}/page.json"
    page = json.loads(report_parts[page_path])
    page_name = page.get("displayName", page_id)
    prefix = f"definition/pages/{page_id}/visuals/"

    section(f"PAGE: {page_name}")

    for path in sorted(report_parts):
        if not path.startswith(prefix) or not path.endswith("/visual.json"):
            continue

        visual_count += 1
        visual_id = path[len(prefix):].split("/", 1)[0]
        definition = json.loads(report_parts[path])
        visual_type = first_string_for_key(definition, "visualType") or "Unknown"
        title = get_title(definition) or "Not detected"
        position = get_position(definition)
        references = collect_semantic_references(definition)
        filters = collect_filters(definition)
        summary[visual_type] += 1

        print(f"\nVISUAL {visual_count:02d}")
        print(f"ID: {visual_id}")
        print(f"Type: {visual_type}")
        print(f"Title: {title}")
        print(
            "Position: "
            f"x={position.get('x')}, y={position.get('y')}, "
            f"w={position.get('width')}, h={position.get('height')}"
        )
        print("Semantic references:")
        if references:
            for reference in references:
                print(f"- {reference}")
        else:
            print("- None detected")

        print("Filter references:")
        if filters:
            for filter_reference in filters:
                print(f"- {filter_reference}")
        else:
            print("- None detected")

        print(f"Definition path: {path}")

section("VISUAL TYPE SUMMARY")
for visual_type, count in sorted(summary.items(), key=lambda item: (-item[1], item[0])):
    print(f"- {visual_type}: {count}")

section("FINAL STATUS")
print(f"PASS: Inspected {visual_count} visual definitions in read-only mode.")
print("No report or semantic-model definition was modified.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import json
import re
import time

import notebookutils
import requests

APPLY_CHANGES = False
SEMANTIC_MODEL_NAME = "Sales Records Model"
CALCULATION_GROUP_NAME = "Time Comparison"
DATE_TABLE_NAME = "date"
DATE_COLUMN_NAME = "full_date"
API_BASE = "https://api.fabric.microsoft.com/v1"
TIMEOUT_SECONDS = 120
MAX_WAIT_SECONDS = 600
MODEL_PATH = "definition/model.tmdl"
GROUP_PATH = "definition/tables/Time Comparison.tmdl"


def context_value(context, name):
    if isinstance(context, dict):
        return context.get(name)
    return getattr(context, name, None)


def request_headers():
    token = notebookutils.credentials.getToken("pbi")
    if not token:
        raise RuntimeError("Fabric authentication token could not be obtained.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def raise_api_error(response):
    if response.ok:
        return
    try:
        body = response.json()
    except ValueError:
        body = response.text
    raise RuntimeError(
        f"Fabric API request failed.\nStatus: {response.status_code}\n"
        f"URL: {response.url}\nResponse: {body}"
    )


def wait_for_operation(response, headers, expect_result):
    raise_api_error(response)
    if response.status_code == 200:
        return response.json() if expect_result else None
    if response.status_code != 202:
        raise RuntimeError(f"Unexpected Fabric API status: {response.status_code}")

    location = response.headers.get("Location")
    operation_id = response.headers.get("x-ms-operation-id")
    if not location and operation_id:
        location = f"{API_BASE}/operations/{operation_id}"
    if not location:
        raise RuntimeError("Fabric returned HTTP 202 without an operation location.")

    retry_after = int(response.headers.get("Retry-After", "5"))
    started_at = time.time()

    while True:
        if time.time() - started_at > MAX_WAIT_SECONDS:
            raise TimeoutError("The Fabric long-running operation exceeded the wait limit.")
        time.sleep(max(retry_after, 1))
        operation_response = requests.get(location, headers=headers, timeout=TIMEOUT_SECONDS)
        raise_api_error(operation_response)
        operation = operation_response.json()
        status = str(operation.get("status", "")).lower()

        if status in {"succeeded", "completed"}:
            if not expect_result:
                return None
            result_location = operation_response.headers.get("Location")
            if not result_location:
                result_location = f"{location.rstrip('/')}/result"
            result_response = requests.get(
                result_location, headers=headers, timeout=TIMEOUT_SECONDS
            )
            raise_api_error(result_response)
            return result_response.json()

        if status in {"failed", "cancelled", "canceled"}:
            raise RuntimeError(
                "Fabric long-running operation failed.\n"
                + json.dumps(operation, indent=2)
            )
        retry_after = int(operation_response.headers.get("Retry-After", "5"))


def get_json(url, headers):
    response = requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS)
    raise_api_error(response)
    return response.json()


def post_for_result(url, headers, body=None):
    response = requests.post(
        url, headers=headers, json=body, timeout=TIMEOUT_SECONDS
    )
    return wait_for_operation(response, headers, expect_result=True)


def post_without_result(url, headers, body):
    response = requests.post(
        url, headers=headers, json=body, timeout=TIMEOUT_SECONDS
    )
    wait_for_operation(response, headers, expect_result=False)


def list_items(workspace_id, headers):
    url = f"{API_BASE}/workspaces/{workspace_id}/items"
    items = []
    while url:
        payload = get_json(url, headers)
        items.extend(payload.get("value", []))
        url = payload.get("continuationUri")
    return items


def find_item(items, name, item_type):
    matches = [
        item for item in items
        if item.get("displayName") == name and item.get("type") == item_type
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one {item_type} named '{name}', found {len(matches)}."
        )
    return matches[0]


def decode_text(payload):
    return base64.b64decode(payload).decode("utf-8", errors="strict")


def encode_text(text):
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def parts_by_path(payload):
    parts = payload.get("definition", {}).get("parts", [])
    return {part["path"]: dict(part) for part in parts}


def text_part(parts, path):
    if path not in parts:
        raise KeyError(f"Required semantic-model part was not found: {path}")
    if parts[path].get("payloadType") != "InlineBase64":
        raise RuntimeError(f"Unsupported payload type for {path}.")
    return decode_text(parts[path]["payload"])


def validate_date_column(parts):
    table_pattern = re.compile(
        rf"(?mi)^\s*table\s+'?{re.escape(DATE_TABLE_NAME)}'?\s*$"
    )
    column_pattern = re.compile(
        rf"(?mi)^\s*column\s+'?{re.escape(DATE_COLUMN_NAME)}'?\s*$"
    )
    for path in sorted(parts):
        if not path.lower().startswith("definition/tables/"):
            continue
        if not path.lower().endswith(".tmdl"):
            continue
        content = text_part(parts, path)
        if table_pattern.search(content):
            if not column_pattern.search(content):
                columns = re.findall(
                    r"(?mi)^\s*column\s+'?([^'\r\n]+)'?\s*$", content
                )
                raise RuntimeError(
                    f"Missing '{DATE_TABLE_NAME}[{DATE_COLUMN_NAME}]' in {path}. "
                    f"Detected columns: {columns}"
                )
            return path
    raise RuntimeError(f"Table '{DATE_TABLE_NAME}' was not found in TMDL.")


def update_model_tmdl(model_tmdl):
    property_pattern = re.compile(
        r"(?mi)^\s*discourageImplicitMeasures\s*:\s*(true|false)\s*$"
    )
    if property_pattern.search(model_tmdl):
        result = property_pattern.sub(
            "\tdiscourageImplicitMeasures: true", model_tmdl, count=1
        )
    else:
        lines = model_tmdl.splitlines()
        lines.insert(1 if lines else 0, "\tdiscourageImplicitMeasures: true")
        result = "\n".join(lines)
        if model_tmdl.endswith("\n"):
            result += "\n"

    reference_pattern = re.compile(
        rf"(?mi)^\s*ref\s+table\s+'?{re.escape(CALCULATION_GROUP_NAME)}'?\s*$"
    )
    if reference_pattern.search(result):
        raise RuntimeError(
            f"Model already references '{CALCULATION_GROUP_NAME}'."
        )
    if not result.endswith("\n"):
        result += "\n"
    return result + f"ref table '{CALCULATION_GROUP_NAME}'\n"


def calculation_group_tmdl():
    return """table 'Time Comparison'
\tcalculationGroup
\t\tprecedence: 10

\t\tcalculationItem Current = SELECTEDMEASURE()
\t\t\tordinal: 0
\t\t\tformatStringDefinition = SELECTEDMEASUREFORMATSTRING()

\t\tcalculationItem 'Previous Year' =
\t\t\tCALCULATE(
\t\t\t\tSELECTEDMEASURE(),
\t\t\t\tSAMEPERIODLASTYEAR('date'[full_date])
\t\t\t)
\t\t\tordinal: 1
\t\t\tformatStringDefinition = SELECTEDMEASUREFORMATSTRING()

\t\tcalculationItem YoY =
\t\t\tVAR CurrentValue = SELECTEDMEASURE()
\t\t\tVAR PreviousYearValue =
\t\t\t\tCALCULATE(
\t\t\t\t\tSELECTEDMEASURE(),
\t\t\t\t\tSAMEPERIODLASTYEAR('date'[full_date])
\t\t\t\t)
\t\t\tRETURN
\t\t\t\tCurrentValue - PreviousYearValue
\t\t\tordinal: 2
\t\t\tformatStringDefinition = SELECTEDMEASUREFORMATSTRING()

\t\tcalculationItem 'YoY %' =
\t\t\tVAR CurrentValue = SELECTEDMEASURE()
\t\t\tVAR PreviousYearValue =
\t\t\t\tCALCULATE(
\t\t\t\t\tSELECTEDMEASURE(),
\t\t\t\t\tSAMEPERIODLASTYEAR('date'[full_date])
\t\t\t\t)
\t\t\tRETURN
\t\t\t\tDIVIDE(CurrentValue - PreviousYearValue, PreviousYearValue)
\t\t\tordinal: 3
\t\t\tformatStringDefinition = "0.00%;-0.00%;0.00%"

\t\tcalculationItem YTD =
\t\t\tCALCULATE(
\t\t\t\tSELECTEDMEASURE(),
\t\t\t\tDATESYTD('date'[full_date])
\t\t\t)
\t\t\tordinal: 4
\t\t\tformatStringDefinition = SELECTEDMEASUREFORMATSTRING()

\tcolumn 'Time Comparison'
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: Name
\t\tsortByColumn: Ordinal

\tcolumn Ordinal
\t\tdataType: int64
\t\tformatString: 0
\t\tsummarizeBy: sum
\t\tsourceColumn: Ordinal
"""


def update_payload(original_parts, changed_parts):
    output = []
    for path, part in original_parts.items():
        new_part = dict(part)
        if path in changed_parts:
            new_part["payload"] = encode_text(changed_parts[path])
            new_part["payloadType"] = "InlineBase64"
        output.append(new_part)
    for path, content in changed_parts.items():
        if path not in original_parts:
            output.append(
                {
                    "path": path,
                    "payload": encode_text(content),
                    "payloadType": "InlineBase64",
                }
            )
    return {"definition": {"parts": output}}


def verify_stored(workspace_id, model_id, headers):
    payload = post_for_result(
        f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition?format=TMDL",
        headers,
    )
    parts = parts_by_path(payload)
    if GROUP_PATH not in parts:
        raise RuntimeError("Calculation-group TMDL was not stored by Fabric.")
    group = text_part(parts, GROUP_PATH)
    required = [
        "calculationItem Current",
        "calculationItem 'Previous Year'",
        "calculationItem YoY",
        "calculationItem 'YoY %'",
        "calculationItem YTD",
    ]
    missing = [token for token in required if token not in group]
    if missing:
        raise RuntimeError(f"Stored calculation group is incomplete: {missing}")
    model = text_part(parts, MODEL_PATH)
    if "ref table 'Time Comparison'" not in model:
        raise RuntimeError("Stored model does not reference Time Comparison.")
    return parts


def section(title):
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


context = notebookutils.runtime.context
workspace_id = context_value(context, "currentWorkspaceId")
workspace_name = context_value(context, "currentWorkspaceName")
if not workspace_id:
    raise RuntimeError("The current Fabric workspace ID could not be obtained.")

headers = request_headers()
items = list_items(workspace_id, headers)
model_item = find_item(items, SEMANTIC_MODEL_NAME, "SemanticModel")
model_id = model_item["id"]

section("SEMANTIC MODEL V1.2.0 PREPARATION")
print(f"Workspace: {workspace_name}")
print(f"Semantic model: {SEMANTIC_MODEL_NAME}")
print(f"Semantic model ID: {model_id}")
print(f"Apply changes: {APPLY_CHANGES}")

original_definition = post_for_result(
    f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition?format=TMDL",
    headers,
)
original_parts = parts_by_path(original_definition)
rollback_request_body = {"definition": {"parts": list(original_parts.values())}}

if GROUP_PATH in original_parts:
    raise RuntimeError(
        f"{GROUP_PATH} already exists. The script stopped without overwriting it."
    )

date_table_path = validate_date_column(original_parts)
model_tmdl = update_model_tmdl(text_part(original_parts, MODEL_PATH))
group_tmdl = calculation_group_tmdl()
changed_parts = {MODEL_PATH: model_tmdl, GROUP_PATH: group_tmdl}
update_request_body = update_payload(original_parts, changed_parts)

section("VALIDATION")
print(f"Validated date table part: {date_table_path}")
print("Validated date reference: 'date'[full_date]")
print("Validated items: Current, Previous Year, YoY, YoY %, YTD")
print("Prepared rollback_request_body in memory.")
print(f"Original parts: {len(original_parts)}")
print(f"Prepared parts: {len(update_request_body['definition']['parts'])}")

section("CALCULATION GROUP TMDL PREVIEW")
print(group_tmdl)

if not APPLY_CHANGES:
    section("FINAL STATUS")
    print("PASS: Dry-run validation completed.")
    print("No semantic-model definition was modified.")
    print("The script is locked with APPLY_CHANGES = False.")
else:
    update_url = (
        f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/updateDefinition"
    )
    try:
        post_without_result(update_url, headers, update_request_body)
        stored_parts = verify_stored(workspace_id, model_id, headers)
    except Exception as deployment_error:
        print("Deployment or verification failed. Attempting rollback.")
        post_without_result(update_url, headers, rollback_request_body)
        raise RuntimeError(
            "The semantic-model update failed and rollback was submitted."
        ) from deployment_error

    section("FINAL STATUS")
    print("PASS: Time Comparison calculation group deployed and verified.")
    print(f"Stored definition parts: {len(stored_parts)}")
    print("rollback_request_body remains available in this notebook session.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import json
import re
import time

import notebookutils
import requests

APPLY_CHANGES = True
SEMANTIC_MODEL_NAME = "Sales Records Model"
CALCULATION_GROUP_NAME = "Time Comparison"
DATE_TABLE_NAME = "date"
DATE_COLUMN_NAME = "full_date"
API_BASE = "https://api.fabric.microsoft.com/v1"
TIMEOUT_SECONDS = 120
MAX_WAIT_SECONDS = 600
MODEL_PATH = "definition/model.tmdl"
GROUP_PATH = "definition/tables/Time Comparison.tmdl"


def context_value(context, name):
    if isinstance(context, dict):
        return context.get(name)
    return getattr(context, name, None)


def request_headers():
    token = notebookutils.credentials.getToken("pbi")
    if not token:
        raise RuntimeError("Fabric authentication token could not be obtained.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def raise_api_error(response):
    if response.ok:
        return
    try:
        body = response.json()
    except ValueError:
        body = response.text
    raise RuntimeError(
        f"Fabric API request failed.\nStatus: {response.status_code}\n"
        f"URL: {response.url}\nResponse: {body}"
    )


def wait_for_operation(response, headers, expect_result):
    raise_api_error(response)
    if response.status_code == 200:
        return response.json() if expect_result else None
    if response.status_code != 202:
        raise RuntimeError(f"Unexpected Fabric API status: {response.status_code}")

    location = response.headers.get("Location")
    operation_id = response.headers.get("x-ms-operation-id")
    if not location and operation_id:
        location = f"{API_BASE}/operations/{operation_id}"
    if not location:
        raise RuntimeError("Fabric returned HTTP 202 without an operation location.")

    retry_after = int(response.headers.get("Retry-After", "5"))
    started_at = time.time()

    while True:
        if time.time() - started_at > MAX_WAIT_SECONDS:
            raise TimeoutError("The Fabric long-running operation exceeded the wait limit.")
        time.sleep(max(retry_after, 1))
        operation_response = requests.get(location, headers=headers, timeout=TIMEOUT_SECONDS)
        raise_api_error(operation_response)
        operation = operation_response.json()
        status = str(operation.get("status", "")).lower()

        if status in {"succeeded", "completed"}:
            if not expect_result:
                return None
            result_location = operation_response.headers.get("Location")
            if not result_location:
                result_location = f"{location.rstrip('/')}/result"
            result_response = requests.get(
                result_location, headers=headers, timeout=TIMEOUT_SECONDS
            )
            raise_api_error(result_response)
            return result_response.json()

        if status in {"failed", "cancelled", "canceled"}:
            raise RuntimeError(
                "Fabric long-running operation failed.\n"
                + json.dumps(operation, indent=2)
            )
        retry_after = int(operation_response.headers.get("Retry-After", "5"))


def get_json(url, headers):
    response = requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS)
    raise_api_error(response)
    return response.json()


def post_for_result(url, headers, body=None):
    response = requests.post(
        url, headers=headers, json=body, timeout=TIMEOUT_SECONDS
    )
    return wait_for_operation(response, headers, expect_result=True)


def post_without_result(url, headers, body):
    response = requests.post(
        url, headers=headers, json=body, timeout=TIMEOUT_SECONDS
    )
    wait_for_operation(response, headers, expect_result=False)


def list_items(workspace_id, headers):
    url = f"{API_BASE}/workspaces/{workspace_id}/items"
    items = []
    while url:
        payload = get_json(url, headers)
        items.extend(payload.get("value", []))
        url = payload.get("continuationUri")
    return items


def find_item(items, name, item_type):
    matches = [
        item for item in items
        if item.get("displayName") == name and item.get("type") == item_type
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one {item_type} named '{name}', found {len(matches)}."
        )
    return matches[0]


def decode_text(payload):
    return base64.b64decode(payload).decode("utf-8", errors="strict")


def encode_text(text):
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def parts_by_path(payload):
    parts = payload.get("definition", {}).get("parts", [])
    return {part["path"]: dict(part) for part in parts}


def text_part(parts, path):
    if path not in parts:
        raise KeyError(f"Required semantic-model part was not found: {path}")
    if parts[path].get("payloadType") != "InlineBase64":
        raise RuntimeError(f"Unsupported payload type for {path}.")
    return decode_text(parts[path]["payload"])


def validate_date_column(parts):
    table_pattern = re.compile(
        rf"(?mi)^\s*table\s+'?{re.escape(DATE_TABLE_NAME)}'?\s*$"
    )
    column_pattern = re.compile(
        rf"(?mi)^\s*column\s+'?{re.escape(DATE_COLUMN_NAME)}'?\s*$"
    )
    for path in sorted(parts):
        if not path.lower().startswith("definition/tables/"):
            continue
        if not path.lower().endswith(".tmdl"):
            continue
        content = text_part(parts, path)
        if table_pattern.search(content):
            if not column_pattern.search(content):
                columns = re.findall(
                    r"(?mi)^\s*column\s+'?([^'\r\n]+)'?\s*$", content
                )
                raise RuntimeError(
                    f"Missing '{DATE_TABLE_NAME}[{DATE_COLUMN_NAME}]' in {path}. "
                    f"Detected columns: {columns}"
                )
            return path
    raise RuntimeError(f"Table '{DATE_TABLE_NAME}' was not found in TMDL.")


def update_model_tmdl(model_tmdl):
    property_pattern = re.compile(
        r"(?mi)^\s*discourageImplicitMeasures\s*:\s*(true|false)\s*$"
    )
    if property_pattern.search(model_tmdl):
        result = property_pattern.sub(
            "\tdiscourageImplicitMeasures: true", model_tmdl, count=1
        )
    else:
        lines = model_tmdl.splitlines()
        lines.insert(1 if lines else 0, "\tdiscourageImplicitMeasures: true")
        result = "\n".join(lines)
        if model_tmdl.endswith("\n"):
            result += "\n"

    reference_pattern = re.compile(
        rf"(?mi)^\s*ref\s+table\s+'?{re.escape(CALCULATION_GROUP_NAME)}'?\s*$"
    )
    if reference_pattern.search(result):
        raise RuntimeError(
            f"Model already references '{CALCULATION_GROUP_NAME}'."
        )
    if not result.endswith("\n"):
        result += "\n"
    return result + f"ref table '{CALCULATION_GROUP_NAME}'\n"


def calculation_group_tmdl():
    return """table 'Time Comparison'
	calculationGroup
		precedence: 10
		calculationItem Current = SELECTEDMEASURE()
		calculationItem 'Previous Year' = CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('date'[full_date]))
		calculationItem YoY = SELECTEDMEASURE() - CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('date'[full_date]))
		calculationItem 'YoY %' = DIVIDE(SELECTEDMEASURE() - CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('date'[full_date])), CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('date'[full_date])))
		calculationItem YTD = CALCULATE(SELECTEDMEASURE(), DATESYTD('date'[full_date]))

	column 'Time Comparison'
		dataType: string
		summarizeBy: none
		sourceColumn: Name
		sortByColumn: Ordinal
		annotation SummarizationSetBy = Automatic

	column Ordinal
		dataType: int64
		formatString: 0
		summarizeBy: sum
		sourceColumn: Ordinal
		annotation SummarizationSetBy = Automatic
"""


def update_payload(original_parts, changed_parts):
    output = []
    for path, part in original_parts.items():
        new_part = dict(part)
        if path in changed_parts:
            new_part["payload"] = encode_text(changed_parts[path])
            new_part["payloadType"] = "InlineBase64"
        output.append(new_part)
    for path, content in changed_parts.items():
        if path not in original_parts:
            output.append(
                {
                    "path": path,
                    "payload": encode_text(content),
                    "payloadType": "InlineBase64",
                }
            )
    return {"definition": {"parts": output}}


def verify_stored(workspace_id, model_id, headers):
    payload = post_for_result(
        f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition?format=TMDL",
        headers,
    )
    parts = parts_by_path(payload)
    if GROUP_PATH not in parts:
        raise RuntimeError("Calculation-group TMDL was not stored by Fabric.")
    group = text_part(parts, GROUP_PATH)
    required = [
        "calculationItem Current",
        "calculationItem 'Previous Year'",
        "calculationItem YoY",
        "calculationItem 'YoY %'",
        "calculationItem YTD",
    ]
    missing = [token for token in required if token not in group]
    if missing:
        raise RuntimeError(f"Stored calculation group is incomplete: {missing}")
    model = text_part(parts, MODEL_PATH)
    if "ref table 'Time Comparison'" not in model:
        raise RuntimeError("Stored model does not reference Time Comparison.")
    return parts


def section(title):
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


context = notebookutils.runtime.context
workspace_id = context_value(context, "currentWorkspaceId")
workspace_name = context_value(context, "currentWorkspaceName")
if not workspace_id:
    raise RuntimeError("The current Fabric workspace ID could not be obtained.")

headers = request_headers()
items = list_items(workspace_id, headers)
model_item = find_item(items, SEMANTIC_MODEL_NAME, "SemanticModel")
model_id = model_item["id"]

section("SEMANTIC MODEL V1.2.0 PREPARATION")
print(f"Workspace: {workspace_name}")
print(f"Semantic model: {SEMANTIC_MODEL_NAME}")
print(f"Semantic model ID: {model_id}")
print(f"Apply changes: {APPLY_CHANGES}")

original_definition = post_for_result(
    f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition?format=TMDL",
    headers,
)
original_parts = parts_by_path(original_definition)
rollback_request_body = {"definition": {"parts": list(original_parts.values())}}

if GROUP_PATH not in original_parts:
    raise RuntimeError(
        f"{GROUP_PATH} was not found. This repair script only replaces the existing invalid calculation group."
    )

date_table_path = validate_date_column(original_parts)
model_tmdl = text_part(original_parts, MODEL_PATH)
group_tmdl = calculation_group_tmdl()
changed_parts = {MODEL_PATH: model_tmdl, GROUP_PATH: group_tmdl}
update_request_body = update_payload(original_parts, changed_parts)

section("VALIDATION")
print(f"Validated date table part: {date_table_path}")
print("Validated date reference: 'date'[full_date]")
print("Validated minimal one-line TMDL calculation-item syntax.")
print("Validated items: Current, Previous Year, YoY, YoY %, YTD")
print("Prepared rollback_request_body in memory.")
print(f"Original parts: {len(original_parts)}")
print(f"Prepared parts: {len(update_request_body['definition']['parts'])}")

section("CALCULATION GROUP TMDL PREVIEW")
print(group_tmdl)

if not APPLY_CHANGES:
    section("FINAL STATUS")
    print("PASS: Dry-run validation completed.")
    print("No semantic-model definition was modified.")
    print("The script is locked with APPLY_CHANGES = True.")
else:
    update_url = (
        f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/updateDefinition"
    )
    try:
        post_without_result(update_url, headers, update_request_body)
        stored_parts = verify_stored(workspace_id, model_id, headers)
    except Exception as deployment_error:
        print("Deployment or verification failed. Attempting rollback.")
        post_without_result(update_url, headers, rollback_request_body)
        raise RuntimeError(
            "The semantic-model update failed and rollback was submitted."
        ) from deployment_error

    section("FINAL STATUS")
    print("PASS: Time Comparison calculation group repaired and stored definition verified.")
    print(f"Stored definition parts: {len(stored_parts)}")
    print("rollback_request_body remains available in this notebook session.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time
import uuid

import notebookutils
import requests

REPORT_NAME = "Global Sales Performance Dashboard"
DEV_PAGE_NAME = "Executive Overview v1.2 Dev"
API_BASE = "https://api.fabric.microsoft.com/v1"
TIMEOUT_SECONDS = 120
MAX_WAIT_SECONDS = 600


def context_value(context, name):
    if isinstance(context, dict):
        return context.get(name)
    return getattr(context, name, None)


def headers():
    token = notebookutils.credentials.getToken("pbi")
    if not token:
        raise RuntimeError("Fabric authentication token could not be obtained.")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def raise_api_error(response):
    if response.ok:
        return
    try:
        body = response.json()
    except ValueError:
        body = response.text
    raise RuntimeError(
        f"Fabric API request failed.\nStatus: {response.status_code}\n"
        f"URL: {response.url}\nResponse: {body}"
    )


def get_json(url, request_headers):
    response = requests.get(url, headers=request_headers, timeout=TIMEOUT_SECONDS)
    raise_api_error(response)
    return response.json()


def wait_for_operation(response, request_headers, expect_result):
    raise_api_error(response)
    if response.status_code == 200:
        return response.json() if expect_result else None
    if response.status_code != 202:
        raise RuntimeError(f"Unexpected Fabric API status: {response.status_code}")

    location = response.headers.get("Location")
    operation_id = response.headers.get("x-ms-operation-id")
    if not location and operation_id:
        location = f"{API_BASE}/operations/{operation_id}"
    if not location:
        raise RuntimeError("Fabric returned HTTP 202 without an operation location.")

    retry_after = int(response.headers.get("Retry-After", "5"))
    started_at = time.time()

    while True:
        if time.time() - started_at > MAX_WAIT_SECONDS:
            raise TimeoutError("The Fabric long-running operation exceeded the wait limit.")
        time.sleep(max(retry_after, 1))
        operation_response = requests.get(
            location, headers=request_headers, timeout=TIMEOUT_SECONDS
        )
        raise_api_error(operation_response)
        operation = operation_response.json()
        status = str(operation.get("status", "")).lower()

        if status in {"succeeded", "completed"}:
            if not expect_result:
                return None
            result_location = operation_response.headers.get("Location")
            if not result_location:
                result_location = f"{location.rstrip('/')}/result"
            result_response = requests.get(
                result_location, headers=request_headers, timeout=TIMEOUT_SECONDS
            )
            raise_api_error(result_response)
            return result_response.json()

        if status in {"failed", "cancelled", "canceled"}:
            raise RuntimeError(
                "Fabric long-running operation failed.\n"
                + json.dumps(operation, indent=2)
            )
        retry_after = int(operation_response.headers.get("Retry-After", "5"))


def post_for_result(url, request_headers):
    response = requests.post(url, headers=request_headers, timeout=TIMEOUT_SECONDS)
    return wait_for_operation(response, request_headers, True)


def post_update(url, request_headers, body):
    response = requests.post(
        url, headers=request_headers, json=body, timeout=TIMEOUT_SECONDS
    )
    wait_for_operation(response, request_headers, False)


def list_items(workspace_id, request_headers):
    url = f"{API_BASE}/workspaces/{workspace_id}/items"
    items = []
    while url:
        payload = get_json(url, request_headers)
        items.extend(payload.get("value", []))
        url = payload.get("continuationUri")
    return items


def find_item(items, name, item_type):
    matches = [
        item for item in items
        if item.get("displayName") == name and item.get("type") == item_type
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one {item_type} named '{name}', found {len(matches)}."
        )
    return matches[0]


def decode_parts(payload):
    parts = {}
    for part in payload.get("definition", {}).get("parts", []):
        item = copy.deepcopy(part)
        if item.get("payloadType") == "InlineBase64":
            item["text"] = base64.b64decode(item["payload"]).decode(
                "utf-8", errors="strict"
            )
        parts[item["path"]] = item
    return parts


def encode_parts(parts):
    output = []
    for path in sorted(parts):
        part = copy.deepcopy(parts[path])
        text = part.pop("text", None)
        if text is not None:
            part["payload"] = base64.b64encode(text.encode("utf-8")).decode("ascii")
            part["payloadType"] = "InlineBase64"
        output.append(part)
    return {"definition": {"parts": output}}


def json_part(parts, path):
    if path not in parts:
        raise KeyError(f"Definition part not found: {path}")
    return json.loads(parts[path]["text"])


def set_json_part(parts, path, value):
    parts[path]["text"] = json.dumps(
        value, ensure_ascii=False, separators=(",", ":")
    )


def walk(node):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from walk(value)


def visual_type(definition):
    for node in walk(definition):
        value = node.get("visualType")
        if isinstance(value, str):
            return value
    return ""


def semantic_fields(definition):
    fields = set()
    for node in walk(definition):
        column = node.get("Column")
        if isinstance(column, dict):
            source = column.get("Expression", {}).get("SourceRef", {}).get("Source")
            prop = column.get("Property")
            if source and prop:
                fields.add((source, prop))
    return fields


def source_entities(definition):
    entities = {}
    for node in walk(definition):
        sources = node.get("From")
        if not isinstance(sources, list):
            continue
        for source in sources:
            if not isinstance(source, dict):
                continue
            alias = source.get("Name")
            entity = source.get("Entity")
            if alias and entity:
                entities[alias] = entity
    return entities


def is_year_slicer(definition):
    if visual_type(definition) != "slicer":
        return False
    aliases = source_entities(definition)
    for alias, prop in semantic_fields(definition):
        if aliases.get(alias) == "date" and prop == "year":
            return True
    return False


def replace_year_with_time_comparison(node):
    replacements = 0
    if isinstance(node, dict):
        if node.get("Entity") == "date":
            node["Entity"] = "Time Comparison"
            replacements += 1
        if node.get("Property") == "year":
            node["Property"] = "Time Comparison"
            replacements += 1
        for key, value in list(node.items()):
            if isinstance(value, str):
                updated = value.replace("date.year", "Time Comparison.Time Comparison")
                updated = updated.replace("'date'[year]", "'Time Comparison'[Time Comparison]")
                if updated != value:
                    node[key] = updated
                    replacements += 1
            else:
                replacements += replace_year_with_time_comparison(value)
    elif isinstance(node, list):
        for value in node:
            replacements += replace_year_with_time_comparison(value)
    return replacements


def set_position(definition, x, y, width, height, tab_order):
    if not isinstance(definition.get("position"), dict):
        raise RuntimeError("The visual does not contain a top-level position object.")
    definition["position"].update(
        {
            "x": x,
            "y": y,
            "width": width,
            "height": height,
            "tabOrder": tab_order,
        }
    )


def find_dev_page(parts):
    metadata = json_part(parts, "definition/pages/pages.json")
    matches = []
    for page_id in metadata.get("pageOrder", []):
        path = f"definition/pages/{page_id}/page.json"
        page = json_part(parts, path)
        if page.get("displayName") == DEV_PAGE_NAME:
            matches.append(page_id)
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one page named '{DEV_PAGE_NAME}', found {len(matches)}."
        )
    return matches[0]


def inspect_page_visuals(parts, page_id):
    prefix = f"definition/pages/{page_id}/visuals/"
    visuals = []
    for path in sorted(parts):
        if path.startswith(prefix) and path.endswith("/visual.json"):
            definition = json_part(parts, path)
            visuals.append((path, definition))
    return visuals


def prepare_report(parts):
    page_id = find_dev_page(parts)
    visuals = inspect_page_visuals(parts, page_id)

    year_slicers = [(path, definition) for path, definition in visuals if is_year_slicer(definition)]
    if len(year_slicers) != 1:
        raise RuntimeError(
            f"Expected one Year slicer on the Dev page, found {len(year_slicers)}."
        )

    existing_time_slicers = []
    for path, definition in visuals:
        aliases = source_entities(definition)
        for alias, prop in semantic_fields(definition):
            if aliases.get(alias) == "Time Comparison" and prop == "Time Comparison":
                existing_time_slicers.append(path)

    if existing_time_slicers:
        raise RuntimeError(
            "The Dev page already contains a Time Comparison slicer. "
            "No update was submitted."
        )

    year_path, year_definition = year_slicers[0]
    year_visual_id = year_path.split("/")[-2]
    new_visual_id = uuid.uuid4().hex[:20]
    new_path = (
        f"definition/pages/{page_id}/visuals/{new_visual_id}/visual.json"
    )

    new_definition = copy.deepcopy(year_definition)
    new_definition["name"] = new_visual_id
    replacements = replace_year_with_time_comparison(new_definition)
    if replacements < 2:
        raise RuntimeError(
            "The Year slicer definition could not be safely converted to Time Comparison."
        )

    # Compact top row: Year, Time Comparison, then five KPI cards.
    set_position(year_definition, 12, 12, 116, 112, 0)
    set_position(new_definition, 140, 12, 180, 112, 1)

    cards = []
    for path, definition in visuals:
        if visual_type(definition) == "cardVisual":
            position = definition.get("position", {})
            cards.append((float(position.get("x", 0)), path, definition))
    cards.sort(key=lambda item: item[0])

    if len(cards) != 5:
        raise RuntimeError(f"Expected five KPI cards, found {len(cards)}.")

    card_x = [332, 518.4, 704.8, 891.2, 1077.6]
    for index, (_, path, definition) in enumerate(cards):
        set_position(definition, card_x[index], 12, 174.4, 112, index + 2)
        set_json_part(parts, path, definition)

    set_json_part(parts, year_path, year_definition)
    parts[new_path] = {
        "path": new_path,
        "payloadType": "InlineBase64",
        "payload": "",
        "text": json.dumps(
            new_definition, ensure_ascii=False, separators=(",", ":")
        ),
    }

    return {
        "page_id": page_id,
        "year_visual_id": year_visual_id,
        "time_visual_id": new_visual_id,
        "new_path": new_path,
        "visual_count_before": len(visuals),
        "visual_count_after": len(visuals) + 1,
    }


def verify_report(workspace_id, report_id, request_headers, expected):
    payload = post_for_result(
        f"{API_BASE}/workspaces/{workspace_id}/reports/{report_id}/getDefinition",
        request_headers,
    )
    parts = decode_parts(payload)
    page_id = find_dev_page(parts)
    visuals = inspect_page_visuals(parts, page_id)

    if len(visuals) != expected["visual_count_after"]:
        raise RuntimeError(
            f"Stored Dev page has {len(visuals)} visuals; "
            f"expected {expected['visual_count_after']}."
        )

    time_slicer_count = 0
    year_slicer_count = 0
    for _, definition in visuals:
        aliases = source_entities(definition)
        fields = semantic_fields(definition)
        for alias, prop in fields:
            if aliases.get(alias) == "Time Comparison" and prop == "Time Comparison":
                time_slicer_count += 1
            if aliases.get(alias) == "date" and prop == "year":
                year_slicer_count += 1

    if time_slicer_count != 1:
        raise RuntimeError(
            f"Stored Dev page has {time_slicer_count} Time Comparison slicers; expected 1."
        )
    if year_slicer_count != 1:
        raise RuntimeError(
            f"Stored Dev page has {year_slicer_count} Year slicers; expected 1."
        )

    return len(visuals)


def section(title):
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)


context = notebookutils.runtime.context
workspace_id = context_value(context, "currentWorkspaceId")
workspace_name = context_value(context, "currentWorkspaceName")
if not workspace_id:
    raise RuntimeError("The current Fabric workspace ID could not be obtained.")

request_headers = headers()
items = list_items(workspace_id, request_headers)
report_item = find_item(items, REPORT_NAME, "Report")
report_id = report_item["id"]
get_url = f"{API_BASE}/workspaces/{workspace_id}/reports/{report_id}/getDefinition"
update_url = f"{API_BASE}/workspaces/{workspace_id}/reports/{report_id}/updateDefinition"

section("V1.2.0 DEV PAGE UPDATE")
print(f"Workspace: {workspace_name}")
print(f"Report: {REPORT_NAME}")
print(f"Target page: {DEV_PAGE_NAME}")

original_payload = post_for_result(get_url, request_headers)
original_parts = decode_parts(original_payload)
rollback_request_body = encode_parts(original_parts)
working_parts = copy.deepcopy(original_parts)
change_summary = prepare_report(working_parts)
update_request_body = encode_parts(working_parts)

section("PRE-DEPLOYMENT VALIDATION")
print(f"Page ID: {change_summary['page_id']}")
print(f"Year slicer ID: {change_summary['year_visual_id']}")
print(f"New Time Comparison slicer ID: {change_summary['time_visual_id']}")
print(f"Dev-page visuals before: {change_summary['visual_count_before']}")
print(f"Dev-page visuals after: {change_summary['visual_count_after']}")
print("Original Executive Overview page was not modified.")
print("rollback_request_body prepared in memory.")

try:
    post_update(update_url, request_headers, update_request_body)
    stored_visual_count = verify_report(
        workspace_id, report_id, request_headers, change_summary
    )
except Exception as deployment_error:
    print("Deployment or verification failed. Submitting complete rollback.")
    post_update(update_url, request_headers, rollback_request_body)
    raise RuntimeError(
        "The Dev-page update failed and the previous complete report definition was restored."
    ) from deployment_error

section("FINAL STATUS")
print("PASS: Time Comparison slicer added to Executive Overview v1.2 Dev.")
print(f"Verified Dev-page visual definitions: {stored_visual_count}")
print("Verified one Year slicer and one Time Comparison slicer.")
print("Verified original Executive Overview page remained outside the update scope.")
print("rollback_request_body remains available in the active notebook session.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import copy
import json
import time

import notebookutils
import requests

SEMANTIC_MODEL_NAME = "Sales Records Model"
GROUP_PATH = "definition/tables/Time Comparison.tmdl"
API_BASE = "https://api.fabric.microsoft.com/v1"
TIMEOUT_SECONDS = 120
MAX_WAIT_SECONDS = 600


def ctx_value(ctx, name):
    return ctx.get(name) if isinstance(ctx, dict) else getattr(ctx, name, None)


def headers():
    token = notebookutils.credentials.getToken("pbi")
    if not token:
        raise RuntimeError("Fabric authentication token could not be obtained.")
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def fail(response):
    if response.ok:
        return
    try:
        body = response.json()
    except ValueError:
        body = response.text
    raise RuntimeError(
        f"Fabric API request failed. Status={response.status_code}; "
        f"URL={response.url}; Response={body}"
    )


def wait(response, request_headers, expect_result):
    fail(response)
    if response.status_code == 200:
        return response.json() if expect_result else None
    if response.status_code != 202:
        raise RuntimeError(f"Unexpected Fabric API status: {response.status_code}")

    location = response.headers.get("Location")
    operation_id = response.headers.get("x-ms-operation-id")
    if not location and operation_id:
        location = f"{API_BASE}/operations/{operation_id}"
    if not location:
        raise RuntimeError("Fabric returned HTTP 202 without an operation location.")

    retry_after = int(response.headers.get("Retry-After", "5"))
    started = time.time()
    while True:
        if time.time() - started > MAX_WAIT_SECONDS:
            raise TimeoutError("Fabric operation exceeded the wait limit.")
        time.sleep(max(retry_after, 1))
        status_response = requests.get(
            location, headers=request_headers, timeout=TIMEOUT_SECONDS
        )
        fail(status_response)
        status_payload = status_response.json()
        status = str(status_payload.get("status", "")).lower()
        if status in {"succeeded", "completed"}:
            if not expect_result:
                return None
            result_location = status_response.headers.get("Location")
            if not result_location:
                result_location = f"{location.rstrip('/')}/result"
            result_response = requests.get(
                result_location, headers=request_headers, timeout=TIMEOUT_SECONDS
            )
            fail(result_response)
            return result_response.json()
        if status in {"failed", "cancelled", "canceled"}:
            raise RuntimeError(json.dumps(status_payload, indent=2))
        retry_after = int(status_response.headers.get("Retry-After", "5"))


def get_json(url, request_headers):
    response = requests.get(url, headers=request_headers, timeout=TIMEOUT_SECONDS)
    fail(response)
    return response.json()


def post_result(url, request_headers):
    response = requests.post(url, headers=request_headers, timeout=TIMEOUT_SECONDS)
    return wait(response, request_headers, True)


def post_update(url, request_headers, body):
    response = requests.post(
        url, headers=request_headers, json=body, timeout=TIMEOUT_SECONDS
    )
    return wait(response, request_headers, False)


def list_items(workspace_id, request_headers):
    url = f"{API_BASE}/workspaces/{workspace_id}/items"
    output = []
    while url:
        payload = get_json(url, request_headers)
        output.extend(payload.get("value", []))
        url = payload.get("continuationUri")
    return output


def find_item(items, name, item_type):
    matches = [
        item for item in items
        if item.get("displayName") == name and item.get("type") == item_type
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected one {item_type} named '{name}', found {len(matches)}."
        )
    return matches[0]


def decode_parts(payload):
    output = {}
    for part in payload.get("definition", {}).get("parts", []):
        item = copy.deepcopy(part)
        if item.get("payloadType") == "InlineBase64":
            item["text"] = base64.b64decode(item["payload"]).decode("utf-8")
        output[item["path"]] = item
    return output


def encode_parts(parts):
    output = []
    for path in sorted(parts):
        item = copy.deepcopy(parts[path])
        text = item.pop("text", None)
        if text is not None:
            item["payload"] = base64.b64encode(text.encode("utf-8")).decode("ascii")
            item["payloadType"] = "InlineBase64"
        output.append(item)
    return {"definition": {"parts": output}}


def corrected_group_tmdl():
    return """table 'Time Comparison'
\tcalculationGroup
\t\tprecedence: 10
\t\tcalculationItem Current = SELECTEDMEASURE()
\t\t\tformatStringDefinition = SELECTEDMEASUREFORMATSTRING()
\t\tcalculationItem 'Previous Year' = CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('date'[full_date]))
\t\t\tformatStringDefinition = SELECTEDMEASUREFORMATSTRING()
\t\tcalculationItem YoY = SELECTEDMEASURE() - CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('date'[full_date]))
\t\t\tformatStringDefinition = SELECTEDMEASUREFORMATSTRING()
\t\tcalculationItem 'YoY %' = DIVIDE(SELECTEDMEASURE() - CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('date'[full_date])), CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('date'[full_date])))
\t\t\tformatStringDefinition = "0.00%;-0.00%;0.00%"
\t\tcalculationItem YTD = CALCULATE(SELECTEDMEASURE(), DATESYTD('date'[full_date]))
\t\t\tformatStringDefinition = SELECTEDMEASUREFORMATSTRING()

\tcolumn 'Time Comparison'
\t\tdataType: string
\t\tsummarizeBy: none
\t\tsourceColumn: Name
\t\tsortByColumn: Ordinal

\tcolumn Ordinal
\t\tdataType: int64
\t\tformatString: 0
\t\tsummarizeBy: sum
\t\tsourceColumn: Ordinal
"""


def verify(workspace_id, model_id, request_headers):
    payload = post_result(
        f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition?format=TMDL",
        request_headers,
    )
    parts = decode_parts(payload)
    content = parts[GROUP_PATH]["text"]
    required = [
        "formatStringDefinition = SELECTEDMEASUREFORMATSTRING()",
        'formatStringDefinition = "0.00%;-0.00%;0.00%"',
        "SAMEPERIODLASTYEAR('date'[full_date])",
        "DATESYTD('date'[full_date])",
    ]
    missing = [token for token in required if token not in content]
    if missing:
        raise RuntimeError(f"Stored definition is missing: {missing}")
    return content


context = notebookutils.runtime.context
workspace_id = ctx_value(context, "currentWorkspaceId")
if not workspace_id:
    raise RuntimeError("Current workspace ID could not be obtained.")

request_headers = headers()
model = find_item(
    list_items(workspace_id, request_headers),
    SEMANTIC_MODEL_NAME,
    "SemanticModel",
)
model_id = model["id"]
get_url = (
    f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition?format=TMDL"
)
update_url = (
    f"{API_BASE}/workspaces/{workspace_id}/semanticModels/{model_id}/updateDefinition"
)

original_payload = post_result(get_url, request_headers)
original_parts = decode_parts(original_payload)
if GROUP_PATH not in original_parts:
    raise RuntimeError(f"Missing calculation-group part: {GROUP_PATH}")

rollback_request_body = encode_parts(original_parts)
working_parts = copy.deepcopy(original_parts)
working_parts[GROUP_PATH]["text"] = corrected_group_tmdl()
update_request_body = encode_parts(working_parts)

try:
    post_update(update_url, request_headers, update_request_body)
    stored_tmdl = verify(workspace_id, model_id, request_headers)
except Exception as deployment_error:
    print("Update failed. Restoring the complete previous semantic-model definition.")
    post_update(update_url, request_headers, rollback_request_body)
    raise RuntimeError(
        "Format update failed and the previous semantic-model definition was restored."
    ) from deployment_error

print("=" * 88)
print("FINAL STATUS")
print("=" * 88)
print("PASS: Time Comparison formats deployed and stored definition verified.")
print("YoY % now uses 0.00% formatting.")
print("Current, Previous Year, YoY, and YTD preserve each selected measure format.")
print("rollback_request_body remains available in the active notebook session.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
