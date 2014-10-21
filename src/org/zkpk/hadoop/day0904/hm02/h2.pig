--h1_2.pig
A = LOAD '/sogou/sogou.500w.utf8'
AS (time:chararray,uid:chararray,keyword:chararray,rank:int,order:int,url:chararray);
B = GROUP A ALL;
C = FOREACH B GENERATE COUNT(A); 

D = FILTER A BY rank <= 10;
E = GROUP D BY rank;
F = FOREACH E {GENERATE group, COUNT(D.time);};
G = FOREACH F GENERATE $0,(float)$1/C.$0;
DUMP G;
