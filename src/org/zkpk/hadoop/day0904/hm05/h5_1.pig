--h5_1.pig
A = LOAD '/sogou/1-5record'
AS (name:chararray,price1:float,price2:float,price3:float,price4:float,price5:float,market:chararray,province:chararray,city:chararray);
B = GROUP A BY province;
C = FOREACH B {D = DISTINCT A.name; GENERATE group, COUNT(D);};
E = ORDER C BY $1 DESC;
DUMP E;
