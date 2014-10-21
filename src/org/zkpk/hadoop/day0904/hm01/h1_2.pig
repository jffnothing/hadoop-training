--h1_2.pig
A = LOAD '/sogou/sogou.500w.utf8'
AS (time:chararray,uid:chararray,keyword:chararray,rank:int,order:int,url:chararray);
B = FILTER A BY keyword matches '.*ÏÉ½£ÆæÏÀ´«.*';
C = GROUP B BY uid;
D = FOREACH C {GENERATE group, COUNT(B.time);};
E = FILTER D BY $1 > 3;
F = ORDER E BY $1 DESC;
G = LIMIT F 2;
H = JOIN G BY $0,B BY $1;
DUMP H;
