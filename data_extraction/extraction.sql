hdfs dfs -get project/Weather_Profile_final_output/part-r-00000 weatherProfileOutput

hdfs dfs -get project/Parking_ETL_final_output/part-r-00000 parkingETLOutput

hdfs dfs -get project/Parking_Profile_final_output/part-r-00000 parkingProfileOutput

hdfs dfs -copyToLocal project/Motor_Collisions_ETL_final_output/part-r-00000 MotorCollisionsETLOutput
