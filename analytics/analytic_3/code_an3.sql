create external table MotorCollision (day string, month int,year int, time string,borough string,zip string,lat double,longi double)
row format delimited fields terminated by ','
location '/project/Motor_Collisions_ETL_final_output/' ;

create table CollisionHotspots as select substr(cast(lat as string),0,6) as clat, substr(cast(longi as string),0,7) as clong from MotorCollision where lat is not NULL AND longi is not NULL AND lat >= 40.49749 AND lat <= 41.32691 AND longi >= -74.3912 AND longi <= -72.92314;

select clat,clong,count(*) as counts from CollisionHotspots group by clat,clong order by counts DESC;

INSERT OVERWRITE DIRECTORY '/project/CollisionHotspots/' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' select clat,clong,count(*) as counts from CollisionHotspots group by clat,clong order by counts DESC;
