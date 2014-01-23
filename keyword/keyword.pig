-- set reduce tasks --
set default_parallel 20;

-- load data and join tables --
tab1 = load 'keyword.txt' as (id1:chararray, word1:chararray);
tab2 = load 'keyword.txt' as (id2:chararray, word2:chararray);
tab3 = join tab1 by word1, tab2 by word2;

-- remove tuples with two identical IDs -- 
tab3 = filter tab3 by id1 != id2;

-- count the number of keywords in common between any two docs --
tab4 = group tab3 by (id1, id2);
tab4 = foreach tab4 generate group.id1 as id1, group.id2 as id2, COUNT(tab3) as count;

-- calculate the sum of strength of every doc --
tab5 = group tab1 by id1;
tab5 = foreach tab5 generate group as id, COUNT(tab1) as num;
tab5 = join tab4 by id1, tab5 by id;
tab5 = foreach tab5 generate id1, id2, (double)count/(double)num as strength;

-- generate records with the strength above the threshold of 0.2 --
tab5 = filter tab5 by strength > 0.2;

-- persist the result --
store tab5 into 'keyword-result';

