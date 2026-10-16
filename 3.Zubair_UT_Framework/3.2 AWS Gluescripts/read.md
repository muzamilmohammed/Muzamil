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