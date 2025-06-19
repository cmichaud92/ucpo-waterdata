CREATE OR REPLACE VIEW vw_nwis_iv_local AS
    SELECT
        s.hydro_area_nm,
        s.site_nm,
        s.site_type,
        iv.site_cd,
        iv.parameter_cd,
        iv.approval_status,
        iv.year,
        iv.read_ts AS datetime_utc,
        iv.read_ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/Denver' AS datetime_local,
        iv.value
    --FROM read_parquet('C:/Users/miesho/Projects_git/ucpo_waterdata/data/hydrology_datalake/timeseries_iv/site=*/year=*/*.parquet') iv
    FROM read_parquet('/Volumes/T7_raw_I/ucpo_waterdata/hydrology_datalake/timeseries_iv/site=*/year=*/*.parquet') iv
    INNER JOIN site s ON iv.site_cd = s.site_cd;


CREATE OR REPLACE VIEW vw_nwis_daily_stats_local AS
    SELECT
         hydro_area_nm
         site_nm,
         site_type,
         site_cd,
         CAST(datetime_local AS DATE) AS DATE,
         parameter_cd,
         AVG(value) AS mean_value,
         MEDIAN(value) AS median_value,
         MIN(value) AS min_value,
         MAX(value) AS max_value,
         approval_status,
         year
     FROM vw_nwis_iv_local
     GROUP BY hydro_area_nm, site_nm, site_type, site_cd, date, parameter_cd, approval_status, year;


CREATE OR REPLACE VIEW vw_nwis_annual_stats_local AS
    WITH extremes AS (
        SELECT
            site_cd,
            parameter_cd,
            CAST(read_ts AS DATE) AS date_recorded,
            year,
            value,
            MIN(value) OVER (PARTITION BY site_cd, parameter_cd, year) AS min_value,
            MAX(value) OVER (PARTITION BY site_cd, parameter_cd, year) AS max_value
        -- FROM read_parquet('C:/Users/miesho/Projects_git/ucpo_waterdata/data/hydrology_datalake/timeseries_iv/site=*/year=*/*.parquet') iv
            FROM read_parquet('/Volumes/T7_raw_I/ucpo_waterdata/hydrology_datalake/timeseries_iv/site=*/year=*/*.parquet') iv
    )    
    SELECT
         s.hydro_area_nm,
         s.site_nm,
         s.site_type,
         e.site_cd,
         e.year,
         e.parameter_cd,
         FIRST(e.date_recorded) FILTER (WHERE value = e.min_value) AS min_date,
         min_value,
         FIRST(e.date_recorded) FILTER (WHERE value = e.max_value) AS max_date,
         max_value,
    FROM extremes e
    INNER JOIN site s ON e.site_cd = s.site_cd
    GROUP BY s.hydro_area_nm, s.site_nm, s.site_type, e.site_cd, e.year, e.parameter_cd, min_value, max_value
    ORDER BY s.hydro_area_nm, s.site_nm, e.year, e.parameter_cd;
