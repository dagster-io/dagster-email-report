import duckdb

# Connect to DuckDB
conn = duckdb.connect("myvacation.duckdb")

# Create dimension tables
conn.execute(
    """
CREATE TABLE host (
    id INTEGER,
    name VARCHAR,
    email VARCHAR
);
"""
)

conn.execute(
    """
CREATE TABLE guest (
    id INTEGER,
    lat FLOAT,
    lon FLOAT
);
"""
)

conn.execute(
    """
CREATE TABLE property (
    id INTEGER,
    address VARCHAR,
    lat FLOAT,
    lon FLOAT,
    host_id INTEGER,
    market_name VARCHAR,
    nightly_rate INTEGER,
    max_guests INTEGER
);
"""
)

conn.execute(
    """
CREATE TABLE reservation (
    id INTEGER,
    property_id INTEGER,
    start_date DATETIME,
    end_date DATETIME,
    guest_id INTEGER,
    total_cost FLOAT
);
"""
)

tables = conn.execute("SHOW TABLES").fetchall()
print(tables)
