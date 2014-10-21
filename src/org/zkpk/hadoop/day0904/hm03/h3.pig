--h3.pig
A = LOAD '/sogou/sogou.500w.utf8'
AS (time:chararray,uid:chararray,keyword:chararray,rank:int,order:int,url:chararray);

B = FILTER A BY url matches '.*baidu.*';
DUMP B;

C = FILTER A BY keyword matches '.*°Ù¶È.*';
DUMP C;

D = JOIN B BY $1,C BY $1;
E = GROUP D BY $1;
F = FOREACH E GENERATE $0;
DUMP F;
