create external table MotorCollision (day string,month int,year int,time string, borough string, zipcode int,lat string,long string)
row format delimited fields terminated by ','
location '/user/opp212/project/Motor_Collisions_ETL_final_output/'; 


create table ProfileMotorCollisions as select cast(split_part(day,"\t", 2) as int) as day,month,year,cast(split_part(time,":",1) as int) as time from MotorCollision;

create external table weather (day 	int,month int,year int,prec float,snow float,dep float)
row format delimited fields terminated by ','
location '/user/opp212/project/Weather_Profile_final_output/';

create table weatheranalytic as select w.day as day,w.month as month,w.year as year,w.snow as snow,w.prec as prec,w.dep as dep, count(*) as incidences from weather as w INNER JOIN ProfileMotorCollisions as d on (w.year = d.year and w.month = d.month and w.day = d.day) group by day,month,year,snow,prec,dep;

select sum(incidences)/count(incidences) as CollisionFactor from weatheranalytic where snow > 0 or prec >= 0.2 or dep>0;

select sum(incidences)/count(incidences) as CollisionFactor from weatheranalytic where snow <= 0 and prec < 0.2 and dep<=0;






