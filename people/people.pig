-- set reduce tasks --
set default_parallel 20;

-- load data, remove duplicate tuples and join tables --
tab1 = load 'people.txt' as (id1:chararray, author1:chararray);
tab2 = load 'people.txt' as (id2:chararray, author2:chararray);
tab3 = join tab1 by author1, tab2 by author2;

-- remove tuples with two identical IDs -- 
tab3 = filter tab3 by id1 != id2;

-- remove duplicates --
tab3 = group tab3 by (id1, id2);
tab3 = foreach tab3 generate group.id1, group.id2;

-- persist the result --
store tab3 into 'people-result';

