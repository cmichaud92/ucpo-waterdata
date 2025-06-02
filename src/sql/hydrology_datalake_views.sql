CREATE OR REPLACE VIEW vw_nwis_iv_local AS
SELECT
    iv.site,
    iv.datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Denver' AS datetime_local,
    iv.datetime AS datetime_utc,
    iv.parameter,
    iv.value,
    iv.approval_status,
    iv.year
FROM read_parquet('C:\Users\miesho\Projects_git\ucpo_waterdata\data\hydrology_datalake\timeseries_iv\**\*.parquet') iv