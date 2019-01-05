create external table parkingTkts(registation_state string, day int, month int, year int, vehicle_type string, vehicle_make string, violation_precinct string, violation_time string, violation_county string, h_no string, street_name string) row format delimited fields terminated by ',' location '/user/sk7748/project/Parking_Profile_final_output';

create external table trafficTkts(violation_code string, violation_description string, year int, month int, day_of_week string, age int, gender string, borrough string) row format delimited fields terminated by ',' location '/user/sk7748/project/Tickets_Profile_final_output';

create table trafficAgg as select year, month, borrough from trafficTkts;

create table parkingAgg as select year, month, violation_county as borrough from parkingTkts;

create table all_violations as select * from trafficAgg UNION ALL select * from parkingAgg;

create table petty_violation_agg as select month,year,borrough,count(*) as count from all_violations where borrough is not NULL and length(borrough) = 1 group by borrough,year,month;

create external table motorColl (day string, month int,year int, time string,borough string,zip string,lat double,longi double) row format delimited fields terminated by ',' location '/user/sk7748/project/Motor_Collisions_ETL_final_output' ;

alter table motorColl set tblproperties ('serialization.null.format'="");

create table motorColl1 as select month, year, borough from motorColl where (year=2017 or year= 2016) and borough is not NULL and length(borough) = 1;

create table motorAgg as select month,year,borough,count(*) as count from motorColl1 where borough is not NULL and length(borough) = 1 group by borough,year,month;

CREATE TABLE collision_violation as select motorAgg.year, motorAgg.month, motorAgg.borough, motorAgg.count as count_col, petty_violation_agg.count as count_viol from motorAgg INNER JOIN petty_violation_agg on (motorAgg.year = petty_violation_agg.year and motorAgg.month = petty_violation_agg.month and motorAgg.borough = petty_violation_agg.borrough);

SELECT (AVG(count_col*count_viol)- AVG(count_col)*AVG(count_viol))/ (1.00000000*STDDEV_POP(count_col)*STDDEV_POP(count_viol)) AS corr_coeff FROM collision_violation;