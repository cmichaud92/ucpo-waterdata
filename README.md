# Upper Colorado River Program Water Data Management System

## About

This project is intended to provide processes to generate a duckdb database and Parquet based data lake.  The duckdb engine manages the duckdb tables and the data lake.  This provides a local copy of waterdata intended to support repeatable analysis.

Once the data management system is stable I intend to begin adding reporting, visualization and release tracking tools to this repository 

---

<br>

## Database :duck:

The database contains metadata around sensors, locations and source system access as well as course scale hydrological data.

### Source metadata :compass:

Information on the access methods for each source system.  Currently I am able to access USGS stream data, USBR reservoir data and NOAA forecast data.

### Site metadata :world_map:

Site names and codes mapping to the various source systems.  Includes sensor locations captured in EPSG 4269 (lat/lon NAD 83), hydrologic area names (e.g. river names) and codes which will map to STReaMS data system. 

### Parameter metadata :clock:

Parameters codes are the access points into the source systems.  This table includes units and definitions for the codes.

### Low resolution data

Data is stored in tables consistent with its temporal capture pattern.

#### Daily data, Monthly data :droplet:

Some measurements are only available in daily or monthly statics.  These course data are stored in the duckdb file.  USGS daily mean discharge is captured here for consistancy with longterm water management protocols.  

#### Forecast data

NOAA/CBRFC Forecast data is uniquely structured and will likely be stored alone. 

## Data Lake :lake:

All high granularity data is partitioned (on site and year) and stored in a data lake composed of .parquet files

### NWIS Timeseries

All 15 minute sensor readings from USGS gaging stations lands here in mostly raw form.  All available hydrologic parameters are captured and stored for each site.  

### HDB Timeseries

In development...

