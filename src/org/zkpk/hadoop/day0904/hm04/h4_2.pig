--h4_2.pig
A = LOAD '/sogou/1-5record'
AS (name:chararray,price1:float,price2:float,price3:float,price4:float,price5:float,market:chararray,province:chararray,city:chararray);
B = GROUP A BY province;
C = FOREACH B {D = DISTINCT A.market; GENERATE group, COUNT(D);};
E = ORDER C BY $1 DESC;

F = LOAD '/sogou/province'
AS (prov:chararray);

G = JOIN F BY prov LEFT OUTER,E BY $0;

H = FILTER G BY $1 is null;
I = FOREACH H GENERATE $0;
DUMP I;
