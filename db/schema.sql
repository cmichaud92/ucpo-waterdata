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
