--h5_2.pig
A = LOAD '/sogou/1-5record'
AS (name:chararray,price1:float,price2:float,price3:float,price4:float,price5:float,market:chararray,province:chararray,city:chararray);
B = GROUP A BY province;
C = FOREACH B {D = DISTINCT A.name; GENERATE group, COUNT(D);};
E = ORDER C BY $1 DESC;

F = LIMIT E 3;
G = JOIN F BY $0,A BY province;
H = GROUP G BY $2;
I = FOREACH H {J = DISTINCT G.province; GENERATE group, COUNT(J);};
K = FILTER I BY $1 == 3;
L = FOREACH K GENERATE $0;
DUMP L;
