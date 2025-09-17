with temps AS (
  select distinct last_reported from station_status
)

select count(*) from temps;

