CREATE TABLE IF NOT EXISTS site (
    site_id INTEGER NOT NULL PRIMARY KEY,
    site_cd TEXT NOT NULL,                 -- Use source system code - used to access data
    site_nm TEXT NOT NULL,                 -- Use source system name
    site_dsc TEXT,                         -- Use source system description
    agency_cd TEXT NOT NULL,               -- Widely used agency code
    agency_nm TEXT NOT NULL,               -- Agency name
    lat_dd DECIMAL(10, 7),                 -- Latitude in decimal degrees using WGS84
    lon_dd DECIMAL(10, 7),                 -- Longitude in decimal degrees using WGS84
    elev_m INTEGER,                        -- Elevation in meters
    site_type TEXT,                        -- Hydrolofic area type - ST, LK,
    hydro_area_cd TEXT NOT NULL,           -- Code for the water the site is located on
    hydro_area_nm TEXT NOT NULL,           -- Name of the water the site is located on
    source TEXT,                           -- Data source API or URL
    create_ts TIMESTAMP,                   -- Creation timestamp
    update_ts TIMESTAMP                    -- Last update timestamp
);

CREATE TABLE IF NOT EXISTS parameter(
    parameter_id INTEGER NOT NULL PRIMARY KEY,
    parameter_cd TEXT NOT NULL,            -- Use source system code - used to access data
    parameter_nm TEXT NOT NULL,            -- Use source system name
    parameter_dsc TEXT,                    -- Use source system description
    unit_cd TEXT NOT NULL,                 -- Unit code for the parameter
    unit_nm TEXT NOT NULL,                 -- Unit name for the parameter
    -- unit_dsc TEXT,                         -- Unit description
    create_ts TIMESTAMP,                   -- Creation timestamp
    update_ts TIMESTAMP                    -- Last update timestamp
);

CREATE TABLE IF NOT EXISTS site_parameter(
    site_parameter_id INTEGER NOT NULL PRIMARY KEY,
    site_id INTEGER NOT NULL,              -- Foreign key to site table
    parameter_id INTEGER NOT NULL,         -- Foreign key to parameter table
    site_cd TEXT NOT NULL,                 -- Use source system code - used to access data
    parameter_cd TEXT NOT NULL,          -- Use source system code - used to access data
    create_ts TIMESTAMP,                   -- Creation timestamp
    update_ts TIMESTAMP,                   -- Last update timestamp
    FOREIGN KEY (site_id) REFERENCES site(site_id),
    FOREIGN KEY (parameter_id) REFERENCES parameter(parameter_id),
    UNIQUE (site_id, parameter_id)         -- Ensure unique site-parameter combinations
);

CREATE TABLE IF NOT EXISTS daily_observations (
    daily_observation_id INTEGER NOT NULL PRIMARY KEY,
    site_parameter_id INTEGER NOT NULL,     -- Foreign key to site_parameter table
    read_dt DATE NOT NULL,                -- Date of the observation
    value DECIMAL(10, 3) NOT NULL,         -- Observation value
    create_ts TIMESTAMP,                   -- Creation timestamp
    update_ts TIMESTAMP,                   -- Last update timestamp
);