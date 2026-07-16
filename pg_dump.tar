from pyexpat.errors import messages

import pandas as pd
import json
import re
from sqlalchemy import text
from logs import setup_logging, get_logger
from comms import test_connections, check_schema
from comms import get_sqlalchemy_engine

log = get_logger()

DATA_PATH = "../data"

#define the expected schema for the orders data
DATA_COLUMNS = [
    "order_uuid",
    "supplier_uuid",
    "component_name",
    "system_name",
    "manufacturer_id",
    "part_no",
    "serial_no",
    "status",
    "status_date",
    "ordered_by"
]

#getting batch data from parquet file
def get_batch_data() -> pd.DataFrame:
    df = pd.read_parquet(f"{DATA_PATH}/batch_orders.parquet")

    print(df.columns.tolist())

    return df

#extracting batch data from parquet file
def extract_batch():
    return get_batch_data()

#getting streaming data from json file
def get_streaming_json() -> list:
   with open(f"{DATA_PATH}/streaming_orders.json", "r") as fh:
      messages = json.load(fh)
   return messages

#extracting streaming data from json file
def extract_stream():
    return get_streaming_json()

#clean component to lowercase_with_underscore format
def clean_component(name):
    if pd.isna(name):
        return None

    name = str(name).strip()
    if name == "" or name.lower() == "none":
        return None

    name = name.lower()
    name = name.replace("-", "_")
    name = name.replace(" ", "_")
    name = re.sub(r"_+", "_", name)

    return name

#clean user to first_name.last_name format
def clean_user(name):
    if pd.isna(name):
        return None

    #lowercase and strip whitespace
    name = str(name).strip().lower()

    name = name.replace(".", " ").replace("_", " ").replace("-", " ")

    #split by comma or space to get first and last name
    if "," in name:
        last, first = [x.strip() for x in name.split(",", 1)]
    else:
        parts = name.split()
        first = parts[0]
        last = parts[-1] if len(parts) > 1 else parts[0]

    first = first.strip()
    last = last.strip()

    if first == last:
        return first

    return f"{first}.{last}"[:32]

#clean batch data by standardizing names, aggregating orders, and enforcing schema
def clean_batch(df):
    df = df.copy()

    #standardize column names to match schema
    df = df.rename(columns={
        "part_number": "part_no",
        "serial_number": "serial_no",
        "uuid": "order_uuid"
    })

    df["component_name"] = df["component_name"].apply(clean_component)
    df["ordered_by"] = df["ordered_by"].apply(clean_user)

    df = aggregate_batch_orders(df)

    #enforce schema
    if "supplier_uuid" not in df.columns:
        df["supplier_uuid"] = df["order_uuid"]

    return df.reindex(columns=DATA_COLUMNS)

#clean stream data by aggregating orders and enforcing schema
def clean_stream(messages):
    df = aggregate_stream_orders(messages)

    #apply cleaning functions to component_name and ordered_by columns
    df["component_name"] = df["component_name"].apply(clean_component)
    df["ordered_by"] = df["ordered_by"].apply(clean_user)

    return df.reindex(columns=DATA_COLUMNS)

#validate schema by checking for missing or extra columns
def validate_schema(df, name="dataset"):
    missing = set(DATA_COLUMNS) - set(df.columns)
    extra = set(df.columns) - set(DATA_COLUMNS)

    if missing or extra:
        raise ValueError(
            f"{name} schema mismatch | missing={missing} extra={extra}"
        )

#group by order_uuid and return the latest status row based on status_date
def latest_status(group):
    return group.sort_values("status_date").iloc[-1]

#aggregate batch orders by order_uuid, keeping the latest status row and adding ordered_by and order_date columns
def aggregate_batch_orders(df):
    orders = []

    for order_uuid, group in df.groupby("order_uuid"):

        group = group.sort_values("status_date")
        latest = group.iloc[-1]
        pending = group[group["status"] == "PENDING"]
        ordered = group[group["status"] == "ORDERED"]
        ordered_by = None
        
        if not pending.empty:
            ordered_by = clean_user(pending.iloc[0]["ordered_by"])

        order_date = None
        if not ordered.empty:
            order_date = ordered.iloc[0]["status_date"]

        system = str(latest["system_name"]).upper().strip()

        orders.append({
            "supplier_uuid": str(order_uuid),
            "component_name": clean_component(latest["component_name"]),
            "system_name": system,
            "manufacturer_id": latest["manufacturer_id"],
            "part_no": latest["part_no"],
            "serial_no": latest["serial_no"],
            "ordered_by": ordered_by,
            "status": latest["status"],
            "status_date": latest["status_date"],
            "order_date": order_date,
            "comp_priority": False
        })

    return pd.DataFrame(orders)

#normalize part and serial number fields by checking for alternative column names
def normalize_part_fields(d):
    return {
        "part_no": d.get("part_no") or d.get("part_number"),
        "serial_no": d.get("serial_no") or d.get("serial_number"),
    }

#aggregate stream orders by extracting relevant fields from messages and returning a DataFrame
def aggregate_stream_orders(messages):
    rows = []

    for m in messages:
        d = m.get("details", {})

        rows.append({
            "order_uuid": m.get("order_uuid"),
            "supplier_uuid": str(m.get("order_uuid")),
            "component_name": d.get("component_name"),
            "system_name": d.get("system_name"),
            "manufacturer_id": d.get("manufacturer_id"),
            "part_no": d.get("part_no"),
            "serial_no": d.get("serial_no"),
            "status": m.get("status"),
            "status_date": m.get("datetime"),
            "ordered_by": d.get("ordered_by")
        })

    return pd.DataFrame(rows)

#load components into the database by dropping invalid keys, cleaning component names, and inserting unique values into the components table
def load_components(engine, df):
    tmp = df[["component_name", "system_name"]].drop_duplicates().copy()

    #drop rows with missing component_name or system_name
    tmp = tmp.dropna(subset=["component_name", "system_name"])

    #drop rows with empty component_name or system_name after stripping whitespace
    tmp = tmp[
        (tmp["component_name"].str.strip() != "") &
        (tmp["system_name"].str.strip() != "")
    ]

    tmp["component_name"] = tmp["component_name"].apply(clean_component)

    #insert unique component_name and system_name pairs into the components table, ignoring conflicts
    with engine.begin() as conn:
        for _, row in tmp.iterrows():
            if pd.isna(row["component_name"]) or pd.isna(row["system_name"]):
                continue
            conn.execute(
                text("""
                    INSERT INTO components (component_name, system_name)
                    VALUES (:name, :system)
                    ON CONFLICT (component_name) DO NOTHING
                """),
                {"name": str(row["component_name"]), "system": str(row["system_name"])}
            )

#load users into the database by dropping invalid keys, cleaning user names, and inserting unique values into the users table
def load_users(engine, df):
    tmp = df[["ordered_by"]].dropna().drop_duplicates().copy()

    tmp["ordered_by"] = tmp["ordered_by"].apply(clean_user)

    #insert unique ordered_by values into the users table
    with engine.begin() as conn:
        for _, row in tmp.iterrows():

            name = str(row["ordered_by"]).strip()

            #truncate user_name to 32 characters if it exceeds the limit
            if len(name) > 32:
                log.warning(f"Truncating user_name: {name}")
                name = name[:32]

            #insert user_name into the users table, ignoring conflicts
            conn.execute(
                text("""
                    INSERT INTO users (user_name)
                    VALUES (:name)
                    ON CONFLICT (user_name) DO NOTHING
                """),
                {"name": name}
            ) 

#load parts into the database by dropping invalid keys and inserting unique manufacturer_id and part_no pairs into the parts table
def load_parts(engine, df):
    tmp = df[["manufacturer_id", "part_no"]].drop_duplicates()

    with engine.begin() as conn:
        for _, row in tmp.iterrows():

            if pd.isna(row["manufacturer_id"]) or pd.isna(row["part_no"]):
                continue

            #insert manufacturer_id and part_no into the parts table, ignoring conflicts
            conn.execute(
                text("""
                    INSERT INTO parts (manufacturer_id, part_no)
                    VALUES (:mid, :pno)
                    ON CONFLICT (manufacturer_id, part_no) DO NOTHING
                """),
                {
                    "mid": int(row["manufacturer_id"]),
                    "pno": int(row["part_no"])
                }
            )

#load reference tables by calling load_components, load_parts, and load_users functions
def load_reference_tables(engine, orders):
    load_components(engine, orders)
    load_parts(engine, orders)
    load_users(engine, orders)

#insert fact table by appending final_df to the orders table in the database
def insert_fact_table(engine, final_df):
    with engine.begin() as conn:
        final_df.to_sql(
            "orders",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500
        )

#build lookup maps by querying components, parts, and users tables and creating dictionaries for component_map, part_map, and user_map
def build_lookup_maps(engine):
    with engine.connect() as conn:

        components = pd.read_sql("SELECT * FROM components", conn)
        parts = pd.read_sql("SELECT * FROM parts", conn)
        users = pd.read_sql("SELECT * FROM users", conn)

    component_map = {
        (r.component_name, r.system_name): r.component_id
        for r in components.itertuples()
    }

    part_map = {
        (r.manufacturer_id, r.part_no): r.part_id
        for r in parts.itertuples()
    }

    user_map = {
        r.user_name: r.user_id
        for r in users.itertuples()
    }

    return component_map, part_map, user_map


#map fact table by adding component_id, part_id, ordered_by, order_date, and comp_priority columns based on lookup maps and existing data
def map_fact_table(df, component_map, part_map, user_map):

    df = df.copy()

    df["component_id"] = df.apply(
        lambda r: component_map.get((r["component_name"], r["system_name"])),
        axis=1
    )

    df["part_id"] = df.apply(
        lambda r: part_map.get((r["manufacturer_id"], r["part_no"])),
        axis=1
    )

    df["ordered_by"] = df["ordered_by"].map(user_map)
    df["order_date"] = pd.to_datetime(df["status_date"])
    df["comp_priority"] = df.get("comp_priority", "NORMAL")  # or derive logic

    return df

#enrich fact table by adding order_date and comp_priority columns based on existing data
def enrich_fact_table(df):
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["status_date"])
    df["comp_priority"] = df.get("comp_priority", "NORMAL")

    return df

#ingestion function that orchestrates the ETL process by extracting, transforming, and loading data into the database
def ingest_data():
    #extract batch and stream data
    batch_raw = extract_batch()
    stream_raw = extract_stream()

    #transform batch and stream data by cleaning and validating schema
    batch_orders = clean_batch(batch_raw)
    stream_orders = clean_stream(stream_raw)

    assert batch_orders.columns.equals(stream_orders.columns), \
    f"Schema mismatch:\nBATCH: {batch_orders.columns}\nSTREAM: {stream_orders.columns}"

    validate_schema(batch_orders, "batch")
    validate_schema(stream_orders, "stream")

    #logging column names for debugging purposes
    log.info(batch_orders.columns.tolist())
    log.info(stream_orders.columns.tolist())

    #merge batch and stream orders into a single DataFrame
    orders = pd.concat([batch_orders, stream_orders], ignore_index=True)

    #logging the shape of the merged DataFrame for debugging purposes
    log.info(f"Merged shape: {orders.shape}")

    #database connection using SQLAlchemy engine
    engine = get_sqlalchemy_engine()

    #load reference tables (components, parts, users) into the database
    load_reference_tables(engine, orders)

    #lookup maps for components, parts, and users based on existing data in the database
    component_map, part_map, user_map = build_lookup_maps(engine)

    #transform the fact table by mapping component_id, part_id, ordered_by, order_date, and comp_priority columns based on lookup maps and existing data
    final_df = map_fact_table(orders, component_map, part_map, user_map)

    #transform the fact table by enriching order_date and comp_priority columns based on existing data
    final_df = enrich_fact_table(final_df)

    #clean the fact table by dropping rows with missing component_id, part_id, or ordered_by values
    final_df = final_df.dropna(subset=[
        "component_id",
        "part_id",
        "ordered_by"
    ])

    required = [
        "supplier_uuid",
        "component_id",
        "part_id",
        "serial_no",
        "comp_priority",
        "order_date",
        "ordered_by",
        "status",
        "status_date"
    ]

    #validate that all required columns are present in the final DataFrame
    missing = set(required) - set(final_df.columns)
    assert not missing, f"Missing columns: {missing}"

    #reorder the columns in the final DataFrame to match the required order
    final_df = final_df[[
        "supplier_uuid",
        "component_id",
        "part_id",
        "serial_no",
        "comp_priority",
        "order_date",
        "ordered_by",
        "status",
        "status_date"
    ]]

    #load the fact table into the database by appending the final DataFrame to the orders table
    insert_fact_table(engine, final_df)

    log.info("Ingestion complete")

if __name__ == "__main__":
    setup_logging()
    test_connections()
    check_schema()
    ingest_data()
