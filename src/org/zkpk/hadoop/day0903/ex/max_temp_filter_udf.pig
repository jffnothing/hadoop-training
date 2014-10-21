--max_temp_filter_udf.pig
REGISTER /home/zkpk/exercise/0903/11-1-1.jar;
DEFINE isGood org.zkpk.hadoop.day0903.IsGoodQuality2();
records = LOAD '/input/ncdc/micro-tab/sample.txt'
  AS (year:chararray,temperature:int,quality:int);
filtered_records = FILTER records BY temperature != 9999 AND isGood(quality);
grouped_records = GROUP filtered_records BY year;
max_temp = FOREACH grouped_records GENERATE group,
  MAX(filtered_records.temperature);
DUMP max_temp;
