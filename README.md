# Truck Manufacturer Project
## Batch and Streaming Data

## Quickstart
* Develop and test your solution
   * Use `make dev-up` to bring up postgres (or recycle postgres to blank tables)
* Run `make ingest` to test full ingestion in docker compose
   * WARNING: This will reset your postgres database
* Run `make submit` to create the pg_dump file and `src/` tarball and commit the
  created artifacts.

  ### Domain Overview

This exercise builds a system to track parts orders for truck components. The company previously relied on a legacy ordering system and the only remaining data is in the `/data` folder as extracts.

A DBA has designed a new PostgreSQL schema to store the recovered legacy data and track new orders. The system processes priority orders via a message queue and regular orders through nightly batch processing.

This project cleans and ingests the legacy data, simulating both batch and stream processing.

## Data descriptions - Postgres schema

The following are the descriptions of the fields in the new Postgres schema.  Keep in mind the legacy data may or may not meet these descriptions / types, or could have nonstandard formatting (names may appear in multiple places with different capitalization, for example).  The schema can be seen in `postgres/schema.sql`

### Component

A component is a part that can be used for business purposes (install in a truck).  Fields:
* `component_id`: database assigned integer primary key
* `component_name`: `VARCHAR(64)` name of component
* `system_name`: `VARCHAR(64)` name of system (valid names are `HYDRAULIC`, `ELECTRICAL`, `TRANSMISSION`, `NAVIGATION`)

### Part

A part is something that can be ordered, like a bolt or a chassis.  Parts are generalized to a manufacturer and part number, not specified to a serial number (physical instance of a part).  The fields are:
* `part_id`: database assigned integer primary key
* `manufacturer_id`: integer id for a manufacturer
* `part_no`: Manufacturer's part number for the specific part
The `manufacturer_id` and `part_no` tuples must be unique

### Users

This is a simple lookup table to hold user information.  Fields:
* `user_id`: database assigned integer primary key
* `user_name`: `VARCHAR(32)` format should be `first_name.last_name`

### Orders

This is the main table to track orders.  It has the following fields:
* `order_id`: database assigned integer primary key
* `supplier_uuid`: The supplier's `order_uuid` `VARCHAR(36)` UNIQUE NOT NULL
* `component_id`: References components table
* `part_id`: References parts table
* `serial_no`: Integer serial number of part ordered
* `comp_priority`: Boolean, false for batch orders, true for streaming orders
* `order_date`: Datetime the date status was set to `ORDERED` (may be null if last status is `PENDING`)
* `ordered_by`: References `users.user_id` that submitted the order
* `status`: `VARCHAR(16)`, current order status. valid entries are `PENDING`, `ORDERED`, `SHIPPED`, and `RECEIVED`
* `status_date`: Datetime the `status` field was set or updated
* an `order_id` has a unique mapping to a `supplier_uuid`.

## Data descriptions - Data dumps
The batch processing dump is in the `data/batch_orders.parquet` file and the streaming dump is in the `data/streaming_orders.json`.

### Batch Order Data
The batch orders are in a parquet where each row represents a status update.  Keep in mind that not every row has all of the fields filled so you'll have to do some aggregating and cleaning. Below is a list of fields and any cleaning needed.
* `order_uuid`: UUID the shipping system uses to keep track of orders
* `component_name`: name of the component (multiple cases, `'_'` may be interchanged with `' '` characters)
* `system_name`: name of the system (no cleaning required)
* `manufacturer_id`: integer id of the manufacturer (no cleaning required)
* `part_no`: integer part number (no cleaning required)
* `serial_no`: integer serial number (no cleaning required)
* `status`: status of order (no cleaning required)
* `status_date`: datetime of update in `YYYY-MM-DD HH:MM:SS` format
* `ordered_by`: Name of user who ordered part (only shows in `PENDING` rows for batch orders or `ORDERED` messages for the streaming format, different name formats)

### Priority (streaming) Order Data
The streaming data json has the following schema and has the same general cleaning requirements as the batch data
```json
{
   "order_uuid" : "string",
   "datetime" : "string, fmt: MM-DD-YYYY HH:MM:SS",
   "status": "string",
   "details":
   {
      "component_name": "string",
      "system_name": "string",
      "manufacturer_id": "int",
      "part_number": "int",
      "serial_number": "int",
      "ordered_by": "string"
   }
}
```
The `details` field is optional and is only included on `ORDERED` status messages.  The data was recovered in chronological order by `datetime`.

### Cleaning required
* Transform the component names into `lowercase_with_underscore_spaces` format.
* Transform the user names into `first_name.last_name` format.  Other formats you may encounter are `First Last` or `Last, First`.
* The `ordered_by` field in `data/batch_orders.parquet` may have some corrupt entries, be sure to pull from the correct rows

## Development and Evaluation
Most of the setup can be done via Make targets.  Here is a list of the relevant targets:
* `make help` - shows major targets
* `make help-dev` - shows helper targets
* `make dev-up` - stands up a new postgres instance (or recycles your current postgres) with the correct table schema in `postgres/schema.sql`
* `make ingest` - builds the solution image from the `/src` folder and runs it using docker-compose. WARNING: This will recycle the database
* `make submit` - creates the tarballs for submission in the `/submission` directory

Your ingestion script's entrypoint is in the method `ingest_data()` in `src/ingest.py`.

### Testing your solution

Tests for the solution in the `/src/tests.py` file.  The default way these are implemented are using pytest, which will run any method that begins with `test`.  To run the tests, you can run `make run-tests` from the parent directory or `python -m pytest tests.py` from `src/`.

It is recommended for you to run an end-2-end test using docker compose.  This will tear down and rebuild the postgres database from scratch and run ingestion using the solution image.  You can run this test with `make ingest`, which will also save the logs from the ingestion container to `solution_logs.txt`.  There is a logging system included, so please make use of the logger to put relevant information into the `solution_logs.txt`
