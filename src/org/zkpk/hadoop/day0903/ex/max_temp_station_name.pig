--max_temp_station_name.pig
REGISTER /home/zkpk/exercise/0903/11-3.jar;
DEFINE isGood org.zkpk.hadoop.day0903.IsGoodQuality();
stations = LOAD '/input/ncdc/metadata/stations-fixed-width.txt'
USING org.zkpk.hadoop.day0903.CutLoadFunc('1-6,8-12,14-42')
AS (usaf:chararray,wban:chararray,name:chararray);

trimmed_stations = FOREACH stations GENERATE usaf,wban,org.zkpk.hadoop.day0903.Trim(name);

records = LOAD '/input/ncdc/all/190*'
USING org.zkpk.hadoop.day0903.CutLoadFunc('5-10,11-15,88-92,93-93')
AS (usaf:chararray,wban:chararray,temperature:int,quality:int);

filtered_records = FILTER records BY temperature != 9999 AND isGood(quality);
group_records = GROUP filtered_records BY (usaf,wban) PARALLEL 30;
max_temp = FOREACH group_records GENERATE FLATTEN(group),
MAX(filtered_records.temperature);
max_temp_named = JOIN max_temp BY (usaf,wban),trimmed_stations BY (usaf,wban) PARALLEL 30;
max_temp_result = FOREACH max_temp_named GENERATE $0,$1,$5,$2;

STORE max_temp_result INTO 'max_temp_by_station';
