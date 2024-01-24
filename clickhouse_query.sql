-- show tables

with daily_trips as (
select pickup_date, count(id) trip_count, sum(fare_amount) total_fares, 
sum(datediff('minute',pickup_datetime,dropoff_datetime)) total_trip_duration_mins
from tripdata
where year(pickup_date) between 2014 and 2016 -- capturing only dates between 1st January 2014 and 31st December 2016
and dayofweek(pickup_date) in (6,7) -- capturing only saturday and sunday trips
and dropoff_datetime > pickup_datetime -- capturing only valid trips i.e. you can not drop off a customer before you pick them up
group by pickup_date
)

select 
toStartOfMonth(pickup_date) Month,
round(avg(case when dayofweek(pickup_date) = 6 then trip_count else null end),0) as sat_mean_trip_count,

round(sum(case when dayofweek(pickup_date) = 6 then total_fares else 0 end)/
sum(case when dayofweek(pickup_date) = 6 then trip_count else 0 end),2) sat_mean_fare_trip,

round(sum(case when dayofweek(pickup_date) = 6 then total_trip_duration_mins else 0 end)/
sum(case when dayofweek(pickup_date) = 6 then trip_count else 0 end),2) sat_mean_duration_per_trip_mins,

round(avg(case when dayofweek(pickup_date) = 7 then trip_count else null end),0) as sun_mean_trip_count,

round(sum(case when dayofweek(pickup_date) = 7 then total_fares else 0 end)/
sum(case when dayofweek(pickup_date) = 7 then trip_count else 0 end),2) sun_mean_fare_trip,

round(sum(case when dayofweek(pickup_date) = 7 then total_trip_duration_mins else 0 end)/
sum(case when dayofweek(pickup_date) = 7 then trip_count else 0 end),2) sun_mean_duration_per_trip_mins
from daily_trips
group by toStartOfMonth(pickup_date)

