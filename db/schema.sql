CREATE TABLE IF NOT EXISTS source (
    source_id INTEGER NOT NULL PRIMARY KEY,
    agency_cd TEXT NOT NULL,               -- Widely used agency code
    agency_nm TEXT NOT NULL,               -- Agency name
    source_cd TEXT NOT NULL,              -- Use source system code - used to access data
    source_nm TEXT NOT NULL,              -- Use source system name
    source_url TEXT,                      -- URL of the source system
    source_dsc TEXT,                      -- Use source system description
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Creation timestamp
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP   -- Last update timestamp
);

CREATE TABLE IF NOT EXISTS site (
    site_id INTEGER NOT NULL PRIMARY KEY,
    site_cd TEXT NOT NULL,                 -- Use source system code - used to access data
    site_nm TEXT NOT NULL,                 -- Use source system name
    site_dsc TEXT,                         -- Use source system description
    lat_dd DECIMAL(10, 7),                 -- Latitude in decimal degrees using WGS84
    lon_dd DECIMAL(10, 7),                 -- Longitude in decimal degrees using WGS84
    elev_m INTEGER,                        -- Elevation in meters
    site_type TEXT,                        -- Hydrolofic area type - ST, LK,
    hydro_area_cd TEXT NOT NULL,           -- Code for the water the site is located on
    hydro_area_nm TEXT NOT NULL,           -- Name of the water the site is located on
    source_id INTEGER NOT NULL,            -- FK to source table
    api_ingest_ind BOOLEAN NOT NULL DEFAULT TRUE, -- Indicates if the site is ingested via API
    api_ingest_notes TEXT,                 -- Notes on API ingestion
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,   -- Creation timestamp
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,   -- Last update timestamp
    FOREIGN KEY (source_id) REFERENCES source (source_id)
);

CREATE TABLE IF NOT EXISTS parameter (
    parameter_id INTEGER NOT NULL PRIMARY KEY,
    parameter_cd TEXT NOT NULL,            -- Use source system code - used to access data
    parameter_nm TEXT NOT NULL,            -- Use source system name
    parameter_interval TEXT,               -- Interval for the parameter (e.g., NWIS - (iv, dv), HDB - (IN, HR, DY, MN))
    parameter_dsc TEXT,                    -- Use source system description
    unit_cd TEXT NOT NULL,                 -- Unit code for the parameter
    unit_nm TEXT NOT NULL,                 -- Unit name for the parameter
    -- unit_dsc TEXT,                      -- Unit description
    api_ingest_ind BOOLEAN NOT NULL DEFAULT TRUE, -- Indicates if the parameter is ingested via API
    api_ingest_notes TEXT,                 -- Notes on API ingestion
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Creation timestamp
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP     -- Last update timestamp
);

CREATE TABLE IF NOT EXISTS site_parameter (
    site_parameter_id INTEGER NOT NULL PRIMARY KEY,
    site_id INTEGER NOT NULL,              -- Foreign key to site table
    parameter_id INTEGER NOT NULL,         -- Foreign key to parameter table
    -- site_cd TEXT NOT NULL,                 -- Use source system code - used to access data
    -- parameter_cd TEXT NOT NULL,          -- Use source system code - used to access data
    api_ingest_ind BOOLEAN NOT NULL DEFAULT TRUE, -- Indicates if the site is ingested via API
    api_ingest_notes TEXT,            -- Notes on API ingestion
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,                   -- Creation timestamp
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,                   -- Last update timestamp
    FOREIGN KEY (site_id) REFERENCES site (site_id),
    FOREIGN KEY (parameter_id) REFERENCES parameter (parameter_id),
    UNIQUE (site_id, parameter_id)         -- Ensure unique site-parameter combinations
);

CREATE TABLE IF NOT EXISTS usbr_site_parameter (
    usbr_site_parameter_id INTEGER NOT NULL PRIMARY KEY,
    site_parameter_id INTEGER NOT NULL,    -- Foreign key to site_parameter table
    usbr_site_parameter_cd TEXT NOT NULL,  -- Use source system code - used to access data
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,                   -- Creation timestamp
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,                   -- Last update timestamp
    FOREIGN KEY (site_parameter_id) REFERENCES site_parameter (site_parameter_id),
    UNIQUE (site_parameter_id, usbr_site_parameter_cd)  -- Ensure unique site-parameter combinations for USBR
);

CREATE TABLE IF NOT EXISTS daily_observations (
    daily_observation_id INTEGER NOT NULL PRIMARY KEY,
    site_parameter_id INTEGER NOT NULL,    -- Foreign key to site_parameter table
    read_dt DATE NOT NULL,                 -- Date of the observation
    value DECIMAL(10, 3) NOT NULL,         -- Observation value
    create_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,                   -- Creation timestamp
    update_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP                    -- Last update timestamp
);
