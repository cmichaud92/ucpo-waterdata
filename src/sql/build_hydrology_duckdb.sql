CREATE TABLE IF NOT EXISTS site (
    site_code VARCHAR NOT NULL PRIMARY KEY,
    site_name VARCHAR,
    agency_code VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    site_type VARCHAR,
    hydro_area_name VARCHAR,
    api_or_url VARCHAR
    );
