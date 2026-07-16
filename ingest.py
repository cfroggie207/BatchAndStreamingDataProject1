CREATE TABLE IF NOT EXISTS components (
  component_id SERIAL PRIMARY KEY,
  component_name VARCHAR(64) UNIQUE,
  system_name VARCHAR(64) -- valid are HYDRAULIC, ELECTRICAL, TRANSMISSION, NAVIGATION
);

CREATE TABLE IF NOT EXISTS parts (
  part_id SERIAL PRIMARY KEY,
  manufacturer_id INT,
  part_no INT,
  UNIQUE (manufacturer_id, part_no)
);

CREATE TABLE IF NOT EXISTS users (
  user_id SERIAL PRIMARY KEY,
  user_name VARCHAR(32) UNIQUE -- format should be first_name.last_name
);

CREATE TABLE IF NOT EXISTS orders (
  order_id SERIAL PRIMARY KEY,
  supplier_uuid VARCHAR(36) UNIQUE NOT NULL,
  component_id INT REFERENCES components(component_id) ON DELETE SET NULL,
  part_id INT REFERENCES parts(part_id) ON DELETE SET NULL,
  serial_no INT,
  comp_priority VARCHAR(16),
  order_date TIMESTAMP,
  ordered_by INT REFERENCES users(user_id),
  status VARCHAR(16), -- valid are PENDING, ORDERED, SHIPPED, and RECEIVED
  status_date TIMESTAMP
);
