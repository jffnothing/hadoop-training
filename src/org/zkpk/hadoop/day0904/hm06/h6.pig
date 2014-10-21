--h6.pig
A = LOAD '/sogou/1-5record'
AS (name:chararray,price1:float,price2:float,price3:float,price4:float,price5:float,market:chararray,province:chararray,city:chararray);
B = FILTER A BY province == 'É½Î÷';
C = GROUP B BY name;
D = FOREACH C {GENERATE group,AVG(B.price1),AVG(B.price2),AVG(B.price3),AVG(B.price4),AVG(B.price5);};
DUMP D;
