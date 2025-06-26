CREATE OR REPLACE VIEW vw_nwis_iv_local AS
SELECT
    s.hydro_area_nm,
    s.site_nm,
    s.site_type,
    iv.site_cd,
    iv.parameter_cd,
    iv.approval_status,
    iv.year,
    iv.value,
    iv.read_ts AS datetime_utc,
    iv.read_ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/Denver' AS datetime_local
-- FROM read_parquet('C:/Users/miesho/Projects_git/ucpo_waterdata/data/
-- hydrology_datalake/timeseries_iv/site=*/year=*/*.parquet') iv
FROM read_parquet('/Volumes/T7_raw_I/ucpo_waterdata/hydrology_datalake/timeseries_iv/site=*/year=*/*.parquet') AS iv
INNER JOIN site AS s ON iv.site_cd = s.site_cd;


CREATE OR REPLACE VIEW vw_nwis_daily_stats_local AS
SELECT
    hydro_area_nm,
    site_nm,
    site_type,
    site_cd,
    parameter_cd,
    approval_status,
    year,
    cast(datetime_local AS DATE) AS date_recorded,
    avg(value) AS mean_value,
    median(value) AS median_value,
    min(value) AS min_value,
    max(value) AS max_value
FROM vw_nwis_iv_local
GROUP BY hydro_area_nm, site_nm, site_type, site_cd, date_recorded, parameter_cd, approval_status, year;


CREATE OR REPLACE VIEW vw_nwis_annual_stats_local AS
WITH ranked AS (
    SELECT
        iv.site_cd,
        iv.parameter_cd,
        iv.year,
        iv.value,
        iv.approval_status,
        --p.parameter_nm,
        --p.unit_cd,
        --s.hydro_area_nm,
        --s.site_nm,
        --s.site_type,
        cast(iv.datetime_local AS DATE) AS date_recorded_local,
        row_number() OVER (
            PARTITION BY iv.site_cd, iv.parameter_cd, iv.year
            ORDER BY iv.value
        ) AS min_rank,
        row_number() OVER (
            PARTITION BY iv.site_cd, iv.parameter_cd, iv.year
            ORDER BY iv.value DESC
        ) AS max_rank
    FROM vw_nwis_iv_local AS iv
    --INNER JOIN site AS s
    --ON iv.site_cd = s.site_cd
    --INNER JOIN parameter AS p
    --ON iv.parameter_cd = p.parameter_cd
),

min_dates AS (
    SELECT
        site_cd,
        parameter_cd,
        year,
        date_recorded_local AS min_date
    FROM ranked
    WHERE min_rank = 1
),

max_dates AS (
    SELECT
        site_cd,
        parameter_cd,
        year,
        date_recorded_local AS max_date
    FROM ranked
    WHERE max_rank = 1
),

aggregated AS (
    SELECT
        site_cd,
        parameter_cd,
        year,
        approval_status,
        --parameter_nm,
        --unit_cd,
        --hydro_area_nm,
        --site_nm,
        --site_type,
        min(value) AS min_value,
        max(value) AS max_value
    FROM ranked
    GROUP BY
        site_cd, parameter_cd, year, approval_status --parameter_nm, unit_cd, hydro_area_nm, site_nm, site_type
)

SELECT
    s.hydro_area_nm,
    s.site_nm,
    s.site_type,
    a.site_cd,
    a.year,
    a.parameter_cd,
    p.parameter_nm,
    p.unit_cd,
    a.approval_status,
    a.min_value,
    a.max_value,
    md.min_date,
    mx.max_date
    -- first(e.date_recorded_local) FILTER (WHERE e.min_rank =
    -- first(e.date_recorded_local) FILTER (WHERE e.value = e.min_value) AS min_date,
    -- first(e.date_recorded_local) FILTER (WHERE e.value = e.max_value) AS max_date
FROM aggregated AS a
LEFT JOIN min_dates AS md
    ON a.site_cd = md.site_cd AND a.parameter_cd = md.parameter_cd AND a.year = md.year
LEFT JOIN max_dates AS mx
    ON a.site_cd = mx.site_cd AND a.parameter_cd = mx.parameter_cd AND a.year = mx.year
INNER JOIN site AS s
    ON a.site_cd = s.site_cd
INNER JOIN parameter AS p
    ON a.parameter_cd = p.parameter_cd
ORDER BY s.hydro_area_nm, s.site_nm, a.year, a.parameter_cd;
