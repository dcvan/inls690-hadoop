-- set the number of reduce tasks --
set default_parallel 20;

-- load data and join tables by id --
tab1 = load 'date.txt' as (id:chararray, date1:int);
tab2 = load 'date.txt' as (id:chararray, date2:int);
tab3 = join tab1 by id, tab2 by id;

-- remove tuples with same starting and ending date --
tab3 = filter tab3 by date1 != date2;

-- remove tuples with starting date later than ending date --
tab3 = filter tab3 by date1 < date2;

-- replicate tab3 into tab4 and get the cartesian product of these two tables --
tab3 = foreach tab3 generate tab1::id as id1, date1, date2;
tab4 = foreach tab3 generate id1 as id2, date1 as date12, date2 as date22;
tab5 = cross tab3, tab4;

-- remove tuples with same id --
tab5 = filter tab5 by id1 != id2;

-- keep tuples with a time period falling in other's time period and remove others -- 
tab5 = filter tab5 by (date12 >= date1) and (date22 <= date2);
tab5 = foreach tab5 generate id1, id2;

-- persist the result --
store tab5 into 'date-result';
