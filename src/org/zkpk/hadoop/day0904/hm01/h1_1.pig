--h1_1.pig
A = LOAD '/sogou/sogou.500w.utf8'
AS (time:chararray,uid:chararray,keyword:chararray,rank:int,order:int,url:chararray);
B = FILTER A BY keyword matches '.*ÏÉ½£ÆæÏÀ´«.*';
C = GROUP B BY uid;
D = FOREACH C {GENERATE group, COUNT(B.time);};
E = FILTER D BY $1 > 3;
F = FOREACH E GENERATE $0;
DUMP F;
