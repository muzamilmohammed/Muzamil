Full Load 
1. Day 1 
input(csv) in S3 bucket -->> output/bronze(parquet) in S3 bucket 
                        -->> bronze table (staging table) - 5
                        -->> silver table (transformations - dedup., cleansing, UTC format, global conversions) -5 
                        -->> Gold table (aggregations) 

action items:
14/08/2025
1. Check the input with 25GB csv file 
Observations: 
check total number of records 
Total execution time 
file should be in splitted format
2. Full load: Day 1 
- input (csv) -->> output (parquet) - 5
              -->> bronze table (staging table) - 5
              -->> silver table (transformations - dedup., cleansing, UTC format, global conversions) -5 
              -->> Gold table (aggregations) 
3. Full load: Day 2 (new inserts- 3) 
- input (csv) -8 -->> output (parquet) - 8
              -->> bronze table (staging table) - 8 
              -->> silver table (transformations - dedup., cleansing, UTC format, global conversions) -7 
4. Full load: Day 3 (new inserts- 2) 
- input (csv) -9 -->> output (parquet) - 9
              -->> bronze table (staging table) - 9 
              -->> silver table (transformations - dedup., cleansing, UTC format, global conversions) -9 
5. Full load: Day 4 (new inserts- 2, 1 update and 1 delete) 
- input (csv) -10 -->> output (parquet) - 10
              -->> bronze table (staging table) - 10 
              -->> silver table (transformations - dedup., cleansing, UTC format, global conversions) -9+2+1 

22/08/25

CLARIFICATIONS
--------------
before
s3://datascience-input-bucket/csv_files/Full_load/customers/2025/Aug/
after
s3://datascience-input-bucket/customers/Full_load/csv_files/2025/Aug/22

Name convention
Initial/ historical load
and
Incremental load

Table Names
Bronze_customers
Bronze_orders
Bronze_*

Silver_dlt_customers
Silver_dlt_*

Gold_agg_report_1
Gold_agg_report_*

Try with stored procedure
-------------------------
input -->> output --> create bronze table -->> copy output data to bronze table 
-->> dataframe to transformations - dedup., cleansing, UTC format, global conversions, control fields(load date-time)

-->> stored-procedure to transformations - dedup., cleansing, UTC format, global conversions, control fields(load date-time)

difference between dataframe and stored-procedure

stored-procedure

Job will be always in layers:
1. Lambda job that pick-up the data from input to output
2. Snowflake job to pick-up data from output to snowflake bronze table
3. Snowflake job to pick-up data from snowflake bronze table to snowflake silver table 
4. Snowflake job to pick-up data from snowflake silver table to snowflake gold table
 
PENDING
-------

1. Check the input with 25GB csv file 
Observations: 
check total number of records 
Total execution time 
file should be in splitted format
2. Full load: Day 1 
- input (csv) -->> output (parquet) - 5
              -->> bronze table (staging table) - 5
              -->> silver table (transformations - dedup., cleansing, UTC format, global conversions) -5 
              -->> Gold table (aggregations) - in progress 
3. Full load: Day 2 (new inserts- 3) 
- input (csv) -8 -->> output (parquet) - 8
              -->> bronze table (staging table) - 8 
              -->> silver table (transformations - dedup., cleansing, UTC format, global conversions) -8 
4. Full load: Day 3 (new inserts- 1) 
- input (csv) -9 -->> output (parquet) - 9
              -->> bronze table (staging table) - 9 
              -->> silver table (transformations - dedup., cleansing, UTC format, global conversions) -9 
5. Full load: Day 4 (new inserts- 2, 1 update and 1 delete) 
- input (csv) -10 -->> output (parquet) - 10
              -->> bronze table (staging table) - 10 
              -->> silver table (transformations - dedup., cleansing, UTC format, global conversions) -9+2+1 
        
17/10/2025
1. Folder structures:
    Data warehouse 
        DDL
            Table creation scripts
            Alter Script
        DML
            Input 
                File formats (WILL BE LOADED THROUGH GLUE/LAMBDA)
                    csv
                        Delta_load
                        Full_load
                    xml
                        Delta_load
                        Full_load
                    json
                        Delta_load
                        Full_load
            Bronze
                Staging_tables
                Stored_Proc (TO LOAD BRONZE TABLE)
                    Historical_stored_proc (parameterized)
                    Incremental_stored_proc (parameterized)
            Silver
                Transfrom_tables
                Stored_Proc (TO LOAD SILVER TABLE)
                    Historical_stored_proc
                    Incremental_stored_proc
            Gold
                Aggregated_tables
                Stored_Proc (TO LOAD GOLD TABLE)
                    Historical_stored_proc
                    Incremental_stored_proc
        Read.md
        