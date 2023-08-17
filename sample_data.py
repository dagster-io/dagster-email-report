from datetime import datetime, timedelta
import duckdb
import random
from faker import Faker
from geopy import Point
from geopy.distance import great_circle

# Initialize Faker instance
fake = Faker()

num_properties = 250

amenities = [
    "Basketball Court",
    "Pool",
    "Trampoline",
    "Allows Dogs",
    "Allows Cats",
    "Dryer",
    "Gym",
    "Air conditioning",
    "Hot tub",
    "Movie Theater",
]

submarkets = [
    ("south-congress-austin-tx", 30.2506, -97.7494, "Austin", "TX", "78704"),
    ("the-gulch-nashville-tn", 36.1522, -86.7867, "Nashville", "TN", "37203"),
    ("capitol-hill-seattle-wa", 47.6230, -122.3194, "Seattle", "WA", "98102"),
    ("wicker-park-chicago-il", 41.9088, -87.6796, "Chicago", "IL", "60622"),
    (
        "mission-district-san-francisco-ca",
        37.7599,
        -122.4148,
        "San Francisco",
        "CA",
        "94110",
    ),
    ("williamsburg-brooklyn-ny", 40.7081, -73.9571, "Brooklyn", "NY", "11211"),
    ("arts-district-los-angeles-ca", 34.0407, -118.2365, "Los Angeles", "CA", "90013"),
    ("wynwood-miami-fl", 25.8042, -80.1989, "Miami", "FL", "33127"),
    ("rino-denver-co", 39.7684, -104.9798, "Denver", "CO", "80205"),
    ("midtown-atlanta-ga", 33.7830, -84.3827, "Atlanta", "GA", "30308"),
]


def generate_random_location(lat, lon, radius_miles):
    origin = Point(lat, lon)
    dist = radius_miles / 3960  # convert miles to degrees for geopy
    bearing = random.uniform(0, 360)  # random bearing in degrees
    destination = great_circle(miles=dist).destination(origin, bearing)
    return destination.latitude, destination.longitude


def create_properties_and_hosts(conn, num_properties, amenities, submarkets):
    conn.execute("BEGIN TRANSACTION")
    for i in range(num_properties):
        # Create a new host
        host_name = fake.name()
        host_email = fake.email()
        conn.execute(
            f"INSERT INTO host (id, name, email) VALUES ({i}, '{host_name}', '{host_email}')"
        )

        # Select a random submarket for the property
        submarket = random.choice(submarkets)
        market_name, lat, lon, city, state_code, zipcode = submarket

        # Generate a random location within 1 mile of the submarket's coordinates
        lat, lon = generate_random_location(lat, lon, 1)

        # Create a new property for the host
        street_address = fake.street_address()
        address = f"{street_address}, {city}, {state_code}, {zipcode}"
        nightly_rate = random.randint(5000, 50000)
        max_guests = random.randint(1, 10)

        # Insert into property table
        conn.execute(
            f"""
            INSERT INTO property 
            (id, address, lat, lon, host_id, market_name, nightly_rate, max_guests) 
            VALUES 
            ({i}, '{address}', {lat}, {lon}, {i}, '{market_name}', {nightly_rate}, {max_guests})
        """
        )

    conn.execute("COMMIT")


def create_guest(conn, guest_id, property, is_nearby):
    # Prepare SQL for inserting guests
    insert_guest_sql = """
        INSERT INTO guest (id, lat, lon)
        VALUES (?, ?, ?)
    """

    # Generate a random location for the guest
    max_radius = 100 if is_nearby else 1000
    radius = (
        max_radius * random.random()
    )  # Randomize the distance up to the maximum radius
    guest_lat, guest_lon = generate_random_location(
        property["lat"], property["lon"], radius
    )

    # Insert the guest into the database
    conn.execute(insert_guest_sql, (guest_id, guest_lat, guest_lon))


def create_reservations(conn):
    # Get today's date
    today = datetime.now()
    # Start four months ago
    start_date = today - timedelta(days=6 * 30)

    # Fetch all properties
    properties = conn.execute(
        "SELECT id, nightly_rate, lat, lon FROM property"
    ).fetchall()

    # Initialize reservation_id and guest_id
    reservation_id = 1
    guest_id = 1

    conn.execute("BEGIN TRANSACTION")
    for property_id, nightly_rate, property_lat, property_lon in properties:
        date = start_date
        # Calculate the property-specific occupancy rate and nearby guest rate
        property_occupancy = random.uniform(0.3, 0.95)
        nearby_guest_rate = random.uniform(0.4, 0.8)

        while date < today:
            # Decide if there will be a reservation starting on this date
            if (
                random.random() < property_occupancy
            ):  # chance of a reservation is now property specific
                # The reservation lasts 1-6 days
                length = random.randint(1, 6)
                end_date = date + timedelta(days=length)

                # Calculate total cost
                total_cost = length * nightly_rate

                # Determine if this guest is nearby
                is_nearby = random.random() < nearby_guest_rate

                # Create a new guest
                create_guest(
                    conn,
                    guest_id,
                    {"lat": property_lat, "lon": property_lon},
                    is_nearby,
                )

                # Create reservation with the new guest id
                conn.execute(
                    f"""
                    INSERT INTO reservation 
                    (id, property_id, start_date, end_date, guest_id, total_cost)
                    VALUES 
                    ({reservation_id}, {property_id}, '{date.date()}', '{end_date.date()}', {guest_id}, {total_cost})
                """
                )

                reservation_id += 1
                guest_id += 1

                # The next potential reservation can start when this one ends
                date = end_date
            else:
                # No reservation, move to the next day
                date += timedelta(days=1)
    conn.execute("COMMIT")


# Create a connection to the DuckDB
conn = duckdb.connect("myvacation.duckdb")

# Create amenities and properties
create_properties_and_hosts(conn, num_properties, amenities, submarkets)
create_reservations(conn)
